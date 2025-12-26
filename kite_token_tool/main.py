import os
import json
import threading
import webbrowser
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

from kiteconnect import KiteConnect
from dotenv import load_dotenv

# ================== LOAD ENV ==================

# .env should be in the same folder as this script
# KITE_API_KEY=xxxxxxxx
# KITE_API_SECRET=yyyyyyyy
# KITE_REDIRECT_URL=http://127.0.0.1:8000/
load_dotenv()

API_KEY = os.getenv("KITE_API_KEY")
API_SECRET = os.getenv("KITE_API_SECRET")
REDIRECT_URL = os.getenv("KITE_REDIRECT_URL", "http://127.0.0.1:8000/")

if not API_KEY or not API_SECRET:
    raise RuntimeError("Set KITE_API_KEY and KITE_API_SECRET in .env")

kite = KiteConnect(api_key=API_KEY)

# This will hold the request_token once we get it from the redirect
request_token_holder = {"token": None}


# ================== HTTP HANDLER ==================

class KiteLoginHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        qs = parse_qs(parsed.query)

        if "request_token" in qs:
            request_token_holder["token"] = qs["request_token"][0]

            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(
                b"<h1>Login successful</h1>"
                b"<p>You can close this window and go back to the terminal.</p>"
            )
        else:
            self.send_response(400)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(
                b"<h1>No request_token found</h1>"
                b"<p>Something went wrong. Check the terminal.</p>"
            )

    # Silence default logging in the console
    def log_message(self, format, *args):
        return


def run_server(server: HTTPServer):
    server.serve_forever()


# ================== MAIN FLOW ==================

def main():
    # Start local HTTP server in background thread
    server = HTTPServer(("127.0.0.1", 8000), KiteLoginHandler)
    t = threading.Thread(target=run_server, args=(server,), daemon=True)
    t.start()

    # Generate login URL
    login_url = kite.login_url()

    print("Login URL (also opening in your default browser):")
    print(login_url)
    print("\n1) Log in with your Zerodha credentials.")
    print("2) Complete TOTP.")
    print("3) Wait until the page says 'Login successful'.")
    print("   Then come back here.\n")

    # Try to open browser automatically
    try:
        webbrowser.open(login_url)
    except Exception as e:
        print(f"Couldn't open browser automatically: {e}")
        print("Copy-paste the URL above into your browser manually.")

    # Wait for the request_token to be set by the HTTP handler
    print("Waiting for redirect and request_token...")
    timeout_seconds = 120
    start = time.time()

    while request_token_holder["token"] is None:
        if time.time() - start > timeout_seconds:
            server.shutdown()
            raise TimeoutError("Timed out waiting for request_token. Try again quickly after login.")
        time.sleep(0.2)

    # Stop the server now that we have the token
    server.shutdown()

    request_token = request_token_holder["token"]
    print(f"\nGot request_token: {request_token}")

    # Exchange request_token for access_token
    try:
        data = kite.generate_session(request_token, api_secret=API_SECRET)
    except Exception as e:
        print("\nError generating access token:")
        print(e)
        return

    access_token = data["access_token"]
    kite.set_access_token(access_token)

    print("\n=== ACCESS TOKEN FOR TODAY ===")
    print(access_token)

    # Build credentials payload
    creds = {
        "api_key": API_KEY,
        "api_secret": API_SECRET,
        "access_token": access_token,
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    }

    # Save to JSON file in *this* folder
    out_path = os.path.join(os.path.dirname(__file__), "kite_credentials.json")
    with open(out_path, "w") as f:
        json.dump(creds, f, indent=2)

    print(f"\nSaved credentials to: {out_path}")
    print("You can now import and reuse them in your other scripts.")


if __name__ == "__main__":
    main()
