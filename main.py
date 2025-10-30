import os, json, subprocess, time, re, zipfile
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
YTDLP_CLIENT = os.getenv("YTDLP_CLIENT", "android")  # android|ios|tv|web
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
    print("$", " ".join(cmd))
    subprocess.run(cmd, check=True)

def run_cmd_capture(cmd: List[str]) -> str:
    print("$", " ".join(cmd))
    out = subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    return out.stdout

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
def ytdlp_common_args() -> List[str]:
    args: List[str] = []
    args += ["--ignore-config", "--force-ipv4"]
    if COOKIES_PATH and Path(COOKIES_PATH).exists():
        args += ["--cookies", COOKIES_PATH]
    if YTDLP_CLIENT:
        args += ["--extractor-args", f"youtube:player_client={YTDLP_CLIENT}"]
    # Polite retries (min:max syntax)
    args += ["--sleep-requests", "1:3", "--retries", "10", "--retry-sleep", "1:3"]
    return args

def ytdlp_try(cmd_base: List[str], try_clients: List[str]) -> None:
    tried = []
    for client in try_clients:
        tried.append(client)
        cmd = cmd_base[:]
        # replace/append player_client per attempt
        if "--extractor-args" in cmd:
            idx = cmd.index("--extractor-args")
            cmd[idx+1] = f"youtube:player_client={client}"
        else:
            cmd += ["--extractor-args", f"youtube:player_client={client}"]
        print(f"Trying yt-dlp with player_client={client} ...")
        try:
            run_cmd(cmd)
            return
        except subprocess.CalledProcessError as e:
            print(f"yt-dlp failed with client={client}, code={e.returncode}")
            time.sleep(2)
    raise RuntimeError(f"yt-dlp failed with clients {tried}")

# ------------------------- Scene detection & planning -------------------------
def download_video_lowres(video_id: str, out_dir: Path) -> Path:
    out_tmpl = str(out_dir / f"{video_id}.%(ext)s")
    url = f"https://www.youtube.com/watch?v={video_id}"
    cmd_base = [
        "yt-dlp",
        *ytdlp_common_args(),
        "-f", "bv*[height<=480][ext=mp4]+ba/b[ext=mp4][height<=480]/b",
        "--merge-output-format", "mp4",
        "-o", out_tmpl, url
    ]
    # try sequence: env client, then ios, then web, then android
    prefs = [c for c in [YTDLP_CLIENT, "ios", "web", "android"] if c]
    ytdlp_try(cmd_base, prefs)

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
    cmd_base = [
        "yt-dlp",
        *ytdlp_common_args(),
        "--skip-download",
        "--write-auto-sub", "--sub-lang", lang, "--convert-subs", "vtt",
        "-o", out_tmpl, url
    ]
    try:
        prefs = [c for c in [YTDLP_CLIENT, "ios", "web", "android"] if c]
        ytdlp_try(cmd_base, prefs)
    except Exception:
        return None
    cand = WORK_DIR / f"{video_id}.{lang}.vtt"
    return cand if cand.exists() else None

# ------------------------- Download & Render -------------------------
def download_full_video_hd(video_id: str, out_dir: Path) -> Path:
    out_path = out_dir / f"{video_id}.mp4"
    url = f"https://www.youtube.com/watch?v={video_id}"
    cmd_base = [
        "yt-dlp",
        *ytdlp_common_args(),
        "-f", "bv*[height<=1080][ext=mp4]+ba/b[ext=mp4][height<=1080]/b",
        "--merge-output-format", "mp4",
        "-o", str(out_path),
        url
    ]
    prefs = [c for c in [YTDLP_CLIENT, "ios", "web", "android"] if c]
    ytdlp_try(cmd_base, prefs)

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
    media = MediaFileUpload(str(file_path), chunksize=-1, resumable=True, mimetype="video/mp4")
    request = youtube.videos().insert(part="snippet,status", body=body, media_body=media)
    response = None
    attempt = 0
    while response is None:
        attempt += 1
        try:
            status, response = request.next_chunk()
            if response and "id" in response:
                print(f"Uploaded: {response['id']}")
                return response["id"]
        except Exception as e:
            print(f"Upload chunk error (attempt {attempt}): {e}")
            time.sleep(5 * attempt)
    raise RuntimeError("Upload failed unexpectedly.")

def hhmmss(sec: float) -> str:
    m = int(sec // 60); s = int(sec % 60)
    return f"{m}:{s:02d}"

def smart_title(base_title: str, clip_idx: int, total_clips: int, keywords: List[str], start: float, end: float) -> str:
    base = re.sub(r"\s+", " ", base_title).strip()
    time_hint = f"{hhmmss(start)}-{hhmmss(end)}"
    if keywords:
        key_phrase = " ".join([k.title() for k in keywords[:3]])
        title = f"{base} | {key_phrase} ({time_hint}) #Shorts"
    else:
        title = f"{base} | Clip {clip_idx}/{total_clips} ({time_hint}) #Shorts"
    if len(title) > 100:
        over = len(title) - 100
        base_cut = max(0, len(base) - over - 3)
        title = f"{base[:base_cut]}... | {title.split('|',1)[1]}"
    return title

# ------------------------- Pre-render pipeline -------------------------
def prerender_and_publish_to_release(video_id: str, segments: List[Dict[str, Any]], base_title: str) -> Dict[str, Any]:
    full_path = download_full_video_hd(video_id, WORK_DIR)
    folder = WORK_DIR / f"clips-{video_id}"
    folder.mkdir(parents=True, exist_ok=True)

    for idx, seg in enumerate(segments):
        clip_path = folder / f"{video_id}_clip_{idx:03d}.mp4"
        render_vertical_segment(full_path, seg["start"], seg["end"], clip_path)

    manifest = {
        "video_id": video_id,
        "title": base_title,
        "generated": datetime.now(timezone.utc).isoformat(),
        "clips": [
            {
                "index": idx,
                "file": f"{video_id}_clip_{idx:03d}.mp4",
                "start": seg["start"],
                "end": seg["end"],
                "keywords": seg.get("keywords", []),
            } for idx, seg in enumerate(segments)
        ]
    }
    (folder / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    zip_path = WORK_DIR / f"{video_id}_clips.zip"
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for p in folder.iterdir():
            z.write(p, arcname=f"clips-{video_id}/{p.name}")

    tag = f"clips-{video_id}"
    name = f"Pre-rendered clips for {video_id}"
    rel = gh_get_release_by_tag(tag)
    if not rel:
        rel = gh_create_release(tag, name, f"All pre-rendered clips for video {video_id}.")
    asset_names = {a["name"] for a in rel.get("assets", [])}
    if zip_path.name not in asset_names:
        gh_upload_asset(rel["upload_url"], zip_path, label=f"{video_id} clips")

    try:
        full_path.unlink(missing_ok=True)
        for p in folder.iterdir():
            p.unlink()
        folder.rmdir()
        zip_path.unlink(missing_ok=True)
    except Exception:
        pass

    return {"tag": tag, "asset": f"{video_id}_clips.zip"}

def ensure_zip_downloaded(tag: str, asset_name: str, dest: Path):
    if dest.exists():
        return
    url = gh_get_asset_download_url(tag, asset_name)
    gh_download_to(url, dest)

def extract_clip(zip_path: Path, video_id: str, clip_index: int, out_path: Path):
    arc = f"clips-{video_id}/{video_id}_clip_{clip_index:03d}.mp4"
    with zipfile.ZipFile(zip_path, "r") as z:
        with z.open(arc) as src, open(out_path, "wb") as dst:
            dst.write(src.read())

# ------------------------- Orchestrator -------------------------
def main():
    if not SOURCE_CHANNEL_ID:
        raise RuntimeError("Please set SOURCE_CHANNEL_ID secret.")

    try:
        youtube = get_youtube_client()
        who_am_i(youtube)  # sanity: prints which channel this token uploads to
    except RefreshError as e:
        raise RuntimeError("OAuth token refresh failed (invalid_scope/invalid_grant). "
                           "Re-generate the refresh token with BOTH scopes: youtube.upload and youtube.readonly, "
                           "and select the Brand Account channel during consent.") from e
    except HttpError as e:
        raise RuntimeError(f"YouTube API error on init: {e}") from e

    state = load_state()

    # Reset daily counter if date changed
    if not state.get("daily") or state["daily"].get("date") != today_str():
        state["daily"] = {"date": today_str(), "uploaded": 0}
        save_state(state)

    # Continue with current video if pending segments exist
    if state.get("current") and any(seg["status"] == "pending" for seg in state["current"]["segments"]):
        current = state["current"]
    else:
        # Pick next unprocessed long-form video
        all_ids = list_channel_video_ids(youtube, SOURCE_CHANNEL_ID, max_items=300)
        meta = get_video_meta(youtube, all_ids)
        candidates = []
        for vid in all_ids:
            if vid in state["processed_video_ids"]:
                continue
            item = meta.get(vid)
            if not item:
                continue
            dur_sec = seconds_from_iso8601_duration(item["contentDetails"]["duration"])
            if dur_sec >= 120:
                candidates.append((vid, dur_sec, item))
        if not candidates:
            print("No new long-form videos found.")
            return
        candidates.sort(key=lambda t: t[2]["snippet"]["publishedAt"])
        vid, dur_sec, item = candidates[0]
        title = item["snippet"]["title"]
        description = item["snippet"].get("description","")
        tags = item["snippet"].get("tags", []) or []

        # Scene detection on low-res
        tmp_path = download_video_lowres(vid, WORK_DIR)
        try:
            scenes = detect_scenes(tmp_path, SCENE_THRESHOLD)
        except Exception as e:
            print("Scene detection failed, falling back to sliding windows:", e)
            scenes = []

        # Plan segments
        if scenes:
            bounds = build_boundaries_from_scenes(scenes, dur_sec)
            segments = plan_segments_from_boundaries(bounds)
        else:
            segments = []
            start = 0.0
            while start + MIN_SHORT_SEC < dur_sec:
                end = min(start + TARGET_SHORT_SEC, dur_sec, start + MAX_SHORT_SEC)
                if dur_sec - end < MIN_SHORT_SEC and end < dur_sec:
                    end = dur_sec
                if end - start >= MIN_SHORT_SEC:
                    segments.append({"start": round(start,3), "end": round(end,3), "status": "pending"})
                start = end

        # Captions -> keywords
        vtt_path = try_download_auto_captions(vid, "en")
        for s in segments:
            s["keywords"] = extract_keywords_for_segment(vtt_path, s["start"], s["end"]) if vtt_path else []

        # Pre-render ALL clips and publish as release asset
        storage = {}
        if GITHUB_REPOSITORY:
            storage = prerender_and_publish_to_release(vid, segments, title)
        else:
            print("Skipping pre-render to release (not running in GitHub Actions).")

        state["current"] = {
            "video_id": vid,
            "video_title": title,
            "video_description": description,
            "video_tags": tags,
            "segments": segments,
            "storage": storage,
        }
        current = state["current"]
        save_state(state)

        try:
            tmp_path.unlink(missing_ok=True)
            if vtt_path: vtt_path.unlink(missing_ok=True)
        except Exception:
            pass

    # ------------------------- DAILY GATING -------------------------
    total_clips = len(current["segments"])
    allowed_today = DAILY_BOOST if total_clips > DAILY_BOOST_THRESHOLD else DAILY_BASE
    uploaded_today = state.get("daily", {}).get("uploaded", 0)
    if uploaded_today >= allowed_today:
        print(f"Daily limit reached ({uploaded_today}/{allowed_today}). Skipping this slot.")
        return
    # ---------------------------------------------------------------

    # Only 1 per run (spaced schedules).
    to_upload = [seg for seg in current["segments"] if seg["status"] == "pending"][:DAILY_UPLOADS]
    if not to_upload:
        if CLEANUP_RELEASE_ON_COMPLETE:
            try:
                cleanup_release_storage(current.get("storage", {}))
            except Exception as e:
                print(f"[cleanup] Warning: {e}")
        state["processed_video_ids"].append(current["video_id"])
        state["current"] = None
        save_state(state)
        print("Finished all segments for current video. Cleanup complete. Will pick a new one next run.")
        return

    src_id = current["video_id"]
    seg_total = len(current["segments"])
    zip_meta = current.get("storage", {})
    zip_tag = zip_meta.get("tag")
    zip_name = zip_meta.get("asset")
    zip_path = WORK_DIR / (zip_name if zip_name else f"{src_id}_clips.zip")

    if zip_tag and zip_name:
        ensure_zip_downloaded(zip_tag, zip_name, zip_path)
    else:
        raise RuntimeError("Missing pre-render storage info; expected to have release ZIP.")

    uploaded_count = 0
    for seg in to_upload:
        idx = current["segments"].index(seg)
        start, end = seg["start"], seg["end"]
        tmp_clip = WORK_DIR / f"{src_id}_clip_{idx:03d}.mp4"

        # Extract from pre-rendered ZIP
        extract_clip(zip_path, src_id, idx, tmp_clip)

        # Metadata
        keywords = seg.get("keywords", [])
        title = smart_title(current["video_title"], idx+1, seg_total, keywords, start, end)
        base_tags = current.get("video_tags", []) or []
        tags = list(dict.fromkeys((["Shorts","highlight"] + base_tags + [k for k in keywords])))[:15]
        description = f"Highlight from: https://youtu.be/{src_id}\n#Shorts"

        yt_video_id = upload_short(youtube, tmp_clip, title, description, tags)

        # Mark uploaded + cleanup temp
        seg["status"] = "uploaded"
        seg["upload_id"] = yt_video_id
        uploaded_count += 1
        state["daily"]["uploaded"] = state.get("daily", {}).get("uploaded", 0) + 1
        save_state(state)
        try:
            tmp_clip.unlink(missing_ok=True)
        except Exception:
            pass

        print(f"Uploaded {uploaded_count}/{DAILY_UPLOADS} this run. Today: {state['daily']['uploaded']}/{allowed_today}")

    if not any(s["status"] == "pending" for s in current["segments"]):
        if CLEANUP_RELEASE_ON_COMPLETE:
            try:
                cleanup_release_storage(current.get("storage", {}))
            except Exception as e:
                print(f"[cleanup] Warning: {e}")
        state["processed_video_ids"].append(current["video_id"])
        state["current"] = None

    state["last_run"] = datetime.now(timezone.utc).isoformat()
    save_state(state)
    print("Run complete.")

if __name__ == "__main__":
    # Legal note: Only process content you own or have permission to reuse.
    main()
