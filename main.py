import os, json, subprocess, time, re, zipfile, shlex
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timezone

import requests
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.auth.exceptions import RefreshError
from googleapiclient.errors import HttpError

from scenedetect import VideoManager, SceneManager
from scenedetect.detectors import ContentDetector
import webvtt

# ------------------------- Config via env -------------------------
SOURCE_CHANNEL_ID = os.getenv("SOURCE_CHANNEL_ID", "").strip()

# Spaced scheduling: each workflow uploads at most 1 per run.
DAILY_UPLOADS = int(os.getenv("DAILY_UPLOADS", "1"))

# Daily policy (dynamic per video)
DAILY_BASE = int(os.getenv("DAILY_BASE", "3"))                 # default daily uploads
DAILY_BOOST = int(os.getenv("DAILY_BOOST", "5"))               # boosted daily uploads
DAILY_BOOST_THRESHOLD = int(os.getenv("DAILY_BOOST_THRESHOLD", "21"))  # if clips > this, use DAILY_BOOST

# Clip lengths + scene detection
MIN_SHORT_SEC = int(os.getenv("MIN_SHORT_SEC", "20"))
MAX_SHORT_SEC = int(os.getenv("MAX_SHORT_SEC", "58"))
TARGET_SHORT_SEC = int(os.getenv("TARGET_SHORT_SEC", "45"))
SCENE_THRESHOLD = float(os.getenv("SCENE_THRESHOLD", "27.0"))

# Cleanup toggles
CLEANUP_RELEASE_ON_COMPLETE = os.getenv("CLEANUP_RELEASE_ON_COMPLETE", "true").lower() in ("1","true","yes","y")
CLEANUP_DELETE_TAG = os.getenv("CLEANUP_DELETE_TAG", "true").lower() in ("1","true","yes","y")

# GitHub (for Releases storage)
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")
GITHUB_REPOSITORY = os.getenv("GITHUB_REPOSITORY", "")  # owner/repo

# YouTube OAuth
YT_CLIENT_ID = os.getenv("YT_CLIENT_ID")
YT_CLIENT_SECRET = os.getenv("YT_CLIENT_SECRET")
YT_REFRESH_TOKEN = os.getenv("YT_REFRESH_TOKEN")

# yt-dlp tuning (cookies + mobile client to avoid "sign in" bot checks)
YTDLP_CLIENT = (os.getenv("YTDLP_CLIENT") or "android").strip().lower()  # android|ios|tv|web
COOKIES_PATH = os.getenv("YTDLP_COOKIES", "").strip()

STATE_PATH = Path("data/state.json")
WORK_DIR = Path("work")
WORK_DIR.mkdir(parents=True, exist_ok=True)
STATE_PATH.parent.mkdir(parents=True, exist_ok=True)

STOPWORDS = set("""
a about above after again against all am an and any are as at be because been before being below between both but by
could did do does doing down during each few for from further had has have having he he'd he'll he's her here here's
hers herself him himself his how how's i i'd i'll i'm i've if in into is it it's its itself let's me more most my
myself nor of on once only or other our ours ourselves out over own same she she'd she'll she's should so some such
than that that's the their theirs them themselves then there there's these they they'd they'll they're they've this those
through to too under until up very was we we'd we'll we're we've were what what's when when's where where's which while
who who's whom why why's with you you'd you'll you're you've your yours yourself yourselves
""".split())

# ------------------------- Utilities -------------------------
def load_state() -> Dict[str, Any]:
    if STATE_PATH.exists():
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    return {"processed_video_ids": [], "current": None, "last_run": None, "daily": None}

def save_state(state: Dict[str, Any]):
    STATE_PATH.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")

def run_cmd(cmd: List[str]):
    print("$", " ".join(shlex.quote(c) for c in cmd))
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    # Always print tool output for debugging (yt-dlp/ffmpeg)
    if p.stdout:
        print(p.stdout)
    if p.returncode != 0:
        raise subprocess.CalledProcessError(p.returncode, cmd, p.stdout)

def today_str():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def get_youtube_client():
    if not (YT_CLIENT_ID and YT_CLIENT_SECRET and YT_REFRESH_TOKEN):
        raise RuntimeError("Missing YT_CLIENT_ID / YT_CLIENT_SECRET / YT_REFRESH_TOKEN.")
    creds = Credentials(
        token=None,
        refresh_token=YT_REFRESH_TOKEN,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=YT_CLIENT_ID,
        client_secret=YT_CLIENT_SECRET,
        scopes=[
            "https://www.googleapis.com/auth/youtube.upload",
            "https://www.googleapis.com/auth/youtube.readonly",
        ],
    )
    return build("youtube", "v3", credentials=creds)

def who_am_i(youtube):
    try:
        me = youtube.channels().list(part="snippet,contentDetails", mine=True).execute()
        items = me.get("items", [])
        if items:
            it = items[0]
            print(f"Uploading to channel: {it['snippet']['title']} ({it['id']})")
        else:
            print("Warning: Token has no channel associated (did you authorize the Brand Account's channel?).")
    except Exception as e:
        print(f"Channel check failed: {e}")

# ------------------------- YouTube API helpers -------------------------
def get_uploads_playlist_id(youtube, channel_id: str) -> str:
    resp = youtube.channels().list(part="contentDetails", id=channel_id).execute()
    items = resp.get("items", [])
    if not items:
        raise RuntimeError("Channel not found or no contentDetails for SOURCE_CHANNEL_ID.")
    return items[0]["contentDetails"]["relatedPlaylists"]["uploads"]

def list_channel_video_ids(youtube, channel_id: str, max_items=200) -> List[str]:
    uploads_pl = get_uploads_playlist_id(youtube, channel_id)
    video_ids = []
    page_token = None
    while True:
        resp = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=uploads_pl,
            maxResults=50,
            pageToken=page_token
        ).execute()
        for it in resp.get("items", []):
            vid = it["contentDetails"]["videoId"]
            video_ids.append(vid)
            if len(video_ids) >= max_items:
                return video_ids
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return video_ids

def get_video_meta(youtube, video_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    out = {}
    for i in range(0, len(video_ids), 50):
        chunk = video_ids[i:i+50]
        resp = youtube.videos().list(part="snippet,contentDetails,status", id=",".join(chunk)).execute()
        for it in resp.get("items", []):
            out[it["id"]] = it
    return out

def seconds_from_iso8601_duration(dur: str) -> float:
    m = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", dur)
    if not m: return 0.0
    h = int(m.group(1) or 0); mm = int(m.group(2) or 0); s = int(m.group(3) or 0)
    return h*3600 + mm*60 + s

# ------------------------- yt-dlp helpers -------------------------
def have_cookies() -> bool:
    return bool(COOKIES_PATH) and Path(COOKIES_PATH).exists() and Path(COOKIES_PATH).stat().st_size > 0

def ytdlp_common_args() -> List[str]:
    args: List[str] = []
    if have_cookies():
        args += ["--cookies", COOKIES_PATH]
    # Be resilient/polite; values are printed as masked in logs, but actual values are used.
    args += ["--sleep-requests", "1:3", "--retries", "8", "--retry-sleep", "1:3"]
    return args

def ytdlp_try(url: str, extra: List[str], client: str) -> bool:
    cmd = ["yt-dlp", *ytdlp_common_args(),
           "--extractor-args", f"youtube:player_client={client}",
           *extra, url]
    try:
        run_cmd(cmd)
        return True
    except subprocess.CalledProcessError as e:
        print(f"[yt-dlp] Client '{client}' failed with code {e.returncode}.")
        return False

def ytdlp_with_client_fallback(url: str, extra: List[str]) -> None:
    order = [c for c in [YTDLP_CLIENT, "ios", "tv", "web"] if c]
    seen = set()
    clients = [c for c in order if not (c in seen or seen.add(c))]
    last_ok = False
    for c in clients:
        if ytdlp_try(url, extra, c):
            last_ok = True
            break
    if not last_ok:
        raise RuntimeError("yt-dlp failed for all client profiles (android/ios/tv/web). See logs above for details.")

# ------------------------- Scene detection & planning -------------------------
def download_video_lowres(video_id: str, out_dir: Path) -> Path:
    out_tmpl = str(out_dir / f"{video_id}.%(ext)s")
    url = f"https://www.youtube.com/watch?v={video_id}"
    extra = [
        "-f", "bv*[height<=480][ext=mp4]+ba/b[ext=mp4][height<=480]/b",
        "--merge-output-format", "mp4",
        "-o", out_tmpl
    ]
    ytdlp_with_client_fallback(url, extra)
    for ext in (".mp4", ".mkv", ".webm", ".mov"):
        p = out_dir / f"{video_id}{ext}"
        if p.exists(): return p
    raise RuntimeError("Low-res download failed.")

def detect_scenes(video_path: Path, threshold: float) -> List[Tuple[float, float]]:
    vm = VideoManager([str(video_path)])
    sm = SceneManager()
    sm.add_detector(ContentDetector(threshold=threshold))
    vm.set_downscale_factor()
    vm.start()
    sm.detect_scenes(frame_source=vm)
    scene_list = sm.get_scene_list()
    vm.release()
    return [(s.get_seconds(), e.get_seconds()) for s, e in scene_list]

def build_boundaries_from_scenes(scenes: List[Tuple[float, float]], total_sec: float) -> List[float]:
    if not scenes:
        return [0.0, total_sec]
    cuts = [0.0]
    for i, (s, e) in enumerate(scenes):
        if i > 0:
            cuts.append(s)
    cuts.append(total_sec)
    out = [cuts[0]]
    for c in cuts[1:]:
        if c > out[-1] + 0.05:
            out.append(round(c,3))
    if out[-1] < total_sec:
        out[-1] = total_sec
    return out

def plan_segments_from_boundaries(bounds: List[float]) -> List[Dict[str, Any]]:
    segs: List[Dict[str, Any]] = []
    i = 0
    total = bounds[-1]
    target = max(MIN_SHORT_SEC, min(TARGET_SHORT_SEC, MAX_SHORT_SEC))
    while i < len(bounds)-1:
        start = bounds[i]
        best_j = None
        best_diff = 1e9
        for j in range(i+1, len(bounds)):
            L = bounds[j] - start
            if L > MAX_SHORT_SEC + 0.001:
                break
            if MIN_SHORT_SEC <= L <= MAX_SHORT_SEC:
                d = abs(L - target)
                if d < best_diff:
                    best_diff = d
                    best_j = j
        if best_j is not None:
            end = bounds[best_j]
            segs.append({"start": round(start,3), "end": round(end,3), "status": "pending"})
            i = best_j
        else:
            end = min(start + target, start + MAX_SHORT_SEC, total)
            if end - start < MIN_SHORT_SEC and i+1 < len(bounds):
                end = min(bounds[i+1], start + MAX_SHORT_SEC, total)
            if end - start >= MIN_SHORT_SEC:
                segs.append({"start": round(start,3), "end": round(end,3), "status": "pending"})
            j = i+1
            while j < len(bounds) and bounds[j] <= end + 0.001:
                j += 1
            i = j-1 if j-1 > i else j
    return [s for s in segs if s["end"] - s["start"] >= MIN_SHORT_SEC - 0.001]

def try_download_auto_captions(video_id: str, lang: str = "en") -> Optional[Path]:
    url = f"https://www.youtube.com/watch?v={video_id}"
    out_tmpl = str(WORK_DIR / f"{video_id}.%(ext)s")
    extra = [
        "--skip-download",
        "--write-auto-sub", "--sub-lang", lang, "--convert-subs", "vtt",
        "-o", out_tmpl
    ]
    try:
        ytdlp_with_client_fallback(url, extra)
    except Exception:
        return None
    cand = WORK_DIR / f"{video_id}.{lang}.vtt"
    return cand if cand.exists() else None

# ------------------------- Download & Render -------------------------
def download_full_video_hd(video_id: str, out_dir: Path) -> Path:
    out_path = out_dir / f"{video_id}.mp4"
    url = f"https://www.youtube.com/watch?v={video_id}"
    extra = [
        "-f", "bv*[height<=1080][ext=mp4]+ba/b[ext=mp4][height<=1080]/b",
        "--merge-output-format", "mp4",
        "-o", str(out_path)
    ]
    ytdlp_with_client_fallback(url, extra)
    if out_path.exists():
        return out_path
    for ext in (".mkv", ".webm", ".mov"):
        p = out_dir / f"{video_id}{ext}"
        if p.exists():
            return p
    raise RuntimeError("Full video download failed.")

def render_vertical_segment(input_path: Path, start: float, end: float, out_path: Path):
    vf = ("crop=w='if(gte(iw/ih,9/16),ih*9/16,iw)':"
          "h='if(gte(iw/ih,9/16),ih,iw*16/9)':"
          "x='(iw - w)/2':y='(ih - h)/2',"
          "scale=1080:1920,fps=30")
    dur = max(0.1, end - start)
    cmd = [
        "ffmpeg","-y","-ss",str(start),"-t",str(dur),"-i",str(input_path),
        "-vf",vf,"-c:v","libx264","-preset","veryfast","-crf","22",
        "-c:a","aac","-b:a","128k","-movflags","+faststart",str(out_path)
    ]
    run_cmd(cmd)

# ------------------------- GitHub Releases helpers -------------------------
def gh_headers(json=True):
    h = {"Authorization": f"token {GITHUB_TOKEN}"} if GITHUB_TOKEN else {}
    if json:
        h["Accept"] = "application/vnd.github+json"
    return h

def gh_owner_repo() -> Tuple[str, str]:
    if not GITHUB_REPOSITORY or "/" not in GITHUB_REPOSITORY:
        raise RuntimeError("GITHUB_REPOSITORY not set (owner/repo).")
    owner, repo = GITHUB_REPOSITORY.split("/", 1)
    return owner, repo

def gh_get_release_by_tag(tag: str) -> Optional[Dict[str, Any]]:
    owner, repo = gh_owner_repo()
    url = f"https://api.github.com/repos/{owner}/{repo}/releases/tags/{tag}"
    r = requests.get(url, headers=gh_headers())
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json()

def gh_create_release(tag: str, name: str, body: str = "") -> Dict[str, Any]:
    owner, repo = gh_owner_repo()
    url = f"https://api.github.com/repos/{owner}/{repo}/releases"
    payload = {"tag_name": tag, "name": name, "body": body, "prerelease": False, "draft": False}
    r = requests.post(url, headers=gh_headers(), json=payload)
    r.raise_for_status()
    return r.json()

def gh_upload_asset(upload_url_template: str, file_path: Path, label: str = "") -> Dict[str, Any]:
    upload_url = upload_url_template.split("{",1)[0] + f"?name={file_path.name}"
    if label:
        upload_url += f"&label={requests.utils.quote(label)}"
    with open(file_path, "rb") as f:
        r = requests.post(upload_url, headers={**gh_headers(json=False), "Content-Type": "application/zip"}, data=f)
    r.raise_for_status()
    return r.json()

def gh_get_asset_download_url(tag: str, asset_name: str) -> str:
    rel = gh_get_release_by_tag(tag)
    if not rel:
        raise RuntimeError(f"Release {tag} not found.")
    for a in rel.get("assets", []):
        if a["name"] == asset_name:
            return a["browser_download_url"]
    raise RuntimeError(f"Asset {asset_name} not found in release {tag}.")

def gh_download_to(url: str, dest_path: Path):
    headers = gh_headers(json=False)
    with requests.get(url, headers=headers, stream=True) as r:
        r.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024*1024):
                if chunk:
                    f.write(chunk)

def gh_delete_release_asset(asset_id: int):
    owner, repo = gh_owner_repo()
    url = f"https://api.github.com/repos/{owner}/{repo}/releases/assets/{asset_id}"
    r = requests.delete(url, headers=gh_headers())
    if r.status_code not in (204, 404):
        r.raise_for_status()

def gh_delete_release(release_id: int):
    owner, repo = gh_owner_repo()
    url = f"https://api.github.com/repos/{owner}/{repo}/releases/{release_id}"
    r = requests.delete(url, headers=gh_headers())
    if r.status_code not in (204, 404):
        r.raise_for_status()

def gh_delete_git_tag(tag: str):
    owner, repo = gh_owner_repo()
    url = f"https://api.github.com/repos/{owner}/{repo}/git/refs/tags/{tag}"
    r = requests.delete(url, headers=gh_headers())
    if r.status_code not in (204, 404):
        r.raise_for_status()

def cleanup_release_storage(storage: Dict[str, Any]):
    if not storage: 
        return
    if not GITHUB_REPOSITORY:
        print("Cleanup skipped: GITHUB_REPOSITORY not set.")
        return
    tag = storage.get("tag")
    if not tag: 
        return
    rel = gh_get_release_by_tag(tag)
    if not rel:
        print(f"No release found for tag {tag}, nothing to clean.")
        return
    # Delete all assets in the release
    for a in rel.get("assets", []):
        try:
            print(f"Deleting asset {a['name']} (id={a['id']})...")
            gh_delete_release_asset(a["id"])
        except Exception as e:
            print(f"Asset delete failed: {e}")
    # Delete the release itself
    try:
        print(f"Deleting release id={rel['id']} for tag {tag}...")
        gh_delete_release(rel["id"])
    except Exception as e:
        print(f"Release delete failed: {e}")
    # Optionally delete the tag ref
    if CLEANUP_DELETE_TAG:
        try:
            print(f"Deleting git tag ref {tag}...")
            gh_delete_git_tag(tag)
        except Exception as e:
            print(f"Tag delete failed: {e}")

# ------------------------- Upload & Titles -------------------------
def upload_short(youtube, file_path: Path, title: str, description: str, tags: List[str]) -> str:
    body = {
        "snippet": {"title": title[:100], "description": description[:5000], "tags": tags[:500], "categoryId": "22"},
        "status": {"privacyStatus": "public", "selfDeclaredMadeForKids": False}
    }
    media = MediaFileUpload(str(file_path), chunksize=-1, 
