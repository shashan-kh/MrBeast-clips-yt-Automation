# YouTube Shorts Automation (Pre-rendered + Scene-Detected + Spaced Uploads + Auto-Cleanup)

Pipeline:
- Picks the next long-form video from the chosen channel (not processed before).
- Detects scenes (PySceneDetect) and plans Shorts 20–58s (target 45s).
- Pre-renders ALL clips to vertical 1080x1920 and zips them.
- Stores the ZIP as a GitHub Release asset for persistent storage.
- Uploads 3/day if the video has <=21 clips, else 5/day (spread over 5 times).
- Auto-cleans the Release assets (and tag) after all clips for a video are uploaded.
- Moves on to the next long-form video automatically.

Legal: Only use with content you own or are licensed to reuse.

## Required GitHub Secrets
- YT_CLIENT_ID
- YT_CLIENT_SECRET
- YT_REFRESH_TOKEN
- SOURCE_CHANNEL_ID

## Optional GitHub Secrets (defaults shown)
- MIN_SHORT_SEC (20)
- MAX_SHORT_SEC (58)
- TARGET_SHORT_SEC (45)
- SCENE_THRESHOLD (27.0)
- DAILY_BASE (3)
- DAILY_BOOST (5)
- DAILY_BOOST_THRESHOLD (21)
- CLEANUP_RELEASE_ON_COMPLETE (true)
- CLEANUP_DELETE_TAG (true)

## Generate a refresh token
Enable YouTube Data API v3 in Google Cloud, create OAuth Client (Desktop App), download JSON.
Run locally:
```bash
pip install google-auth-oauthlib
python get_refresh_token.py /path/to/client_secret.json
