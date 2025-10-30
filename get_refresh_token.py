import sys
from google_auth_oauthlib.flow import InstalledAppFlow
SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]
def main():
    if len(sys.argv) < 2:
        print("Usage: python get_refresh_token.py /path/to/client_secret.json", file=sys.stderr)
        sys.exit(1)
    flow = InstalledAppFlow.from_client_secrets_file(sys.argv[1], SCOPES)
    creds = flow.run_local_server(port=0, access_type="offline", prompt="consent", include_granted_scopes="true")
    print(creds.refresh_token or "")
if __name__ == "__main__":
    main()
