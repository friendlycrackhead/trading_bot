import json
import time
from pathlib import Path
from kiteconnect import KiteConnect
from kiteconnect.exceptions import NetworkException, KiteException
import requests


def get_kite_client():
    """
    Returns an authenticated KiteConnect instance loaded from:
    algo/kite_token_tool/kite_credentials.json

    Sets default timeout of 10 seconds for all API calls.
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

    # Set global timeout for all API calls (10 seconds)
    kite.timeout = 10

    return kite


def kite_retry(func, *args, max_retries=2, **kwargs):
    """
    Retry wrapper for Kite API calls with exponential backoff.

    Retries on:
    - Timeouts
    - Network errors (ConnectionError, etc.)
    - 5xx server errors

    Does NOT retry on:
    - Business logic errors (order rejected, invalid params)
    - 4xx client errors
    - Successful responses with empty/unexpected data

    Args:
        func: Kite API method to call
        *args: Positional arguments for func
        max_retries: Maximum retry attempts (default: 2, total 3 attempts)
        **kwargs: Keyword arguments for func

    Returns:
        API response

    Raises:
        Original exception if all retries fail
    """
    delays = [0.5, 1.0]  # Fixed delays between retries

    for attempt in range(max_retries + 1):
        try:
            result = func(*args, **kwargs)
            return result

        except (requests.exceptions.Timeout,
                requests.exceptions.ConnectionError,
                NetworkException) as e:
            # Network-level errors - safe to retry
            if attempt < max_retries:
                delay = delays[attempt]
                print(f"[RETRY] Attempt {attempt + 1}/{max_retries + 1} failed: {e}")
                print(f"[RETRY] Waiting {delay}s before retry...")
                time.sleep(delay)
            else:
                print(f"[RETRY] All {max_retries + 1} attempts failed: {e}")
                raise

        except KiteException as e:
            # Kite business logic errors - check if retryable
            # 5xx errors (500-599) are server errors - retry
            # 4xx errors (400-499) are client errors - don't retry
            if hasattr(e, 'code') and 500 <= e.code < 600:
                if attempt < max_retries:
                    delay = delays[attempt]
                    print(f"[RETRY] Server error {e.code}: {e.message}")
                    print(f"[RETRY] Waiting {delay}s before retry...")
                    time.sleep(delay)
                else:
                    print(f"[RETRY] All {max_retries + 1} attempts failed: {e.message}")
                    raise
            else:
                # Client error or business logic error - don't retry
                raise

        except Exception as e:
            # Unknown errors - don't retry (could be logic errors)
            raise
