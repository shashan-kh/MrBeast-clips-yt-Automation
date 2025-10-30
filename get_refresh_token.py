import sys
from google_auth_oauthlib.flow import InstalledAppFlow

# Request BOTH scopes so the token can read metadata & upload.
SCOPES = [
    "https://www.googleapis.com/auth/youtube.upload",
    "https://www.googleapis.com/auth/youtube.readonly",
]

def main():
    if len(sys.argv) < 2:
        print("Usage: python get_refresh_token.py /path/to/client_secret.json", file=sys.stderr)
        sys.exit(1)
    # Use a Desktop App OAuth client JSON (top-level key "installed").
    flow = InstalledAppFlow.from_client_secrets_file(sys.argv[1], SCOPES)
    creds = flow.run_local_server(port=0, access_type="offline", prompt="consent")
    print(creds.refresh_token or "")

if __name__ == "__main__":
    main()
