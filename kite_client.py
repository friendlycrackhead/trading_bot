import json
from pathlib import Path
from kiteconnect import KiteConnect


def get_kite_client():
    """
    Returns an authenticated KiteConnect instance loaded from:
    algo/kite_token_tool/kite_credentials.json
    """

    # Root folder: .../algo
    root = Path(__file__).resolve().parent

    creds_path = root / "kite_token_tool" / "kite_credentials.json"

    if not creds_path.exists():
        raise FileNotFoundError(
            f"Missing credentials file: {creds_path}\n"
            f"Run kite_token_tool/main.py to generate access token."
        )

    with creds_path.open() as f:
        creds = json.load(f)

    api_key = creds.get("api_key")
    access_token = creds.get("access_token")

    if not api_key or not access_token:
        raise RuntimeError("Invalid credentials file: api_key/access_token missing.")

    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)

    return kite
