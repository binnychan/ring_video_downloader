import getpass
import asyncio
import json
import logging
from pathlib import Path

from ring_doorbell import Auth, AuthenticationError, Requires2FAError, Ring

# Config
user_agent = "RingFetcher-1.0"
cache_file = Path("/app/ring_token.json")
cred_file = Path("/app/ring_credential.json")

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

def token_updated(token):
    """Callback when Ring refreshes or updates the token."""
    cache_file.write_text(json.dumps(token))
    logging.info("Token refreshed and saved.")

def otp_callback():
    return input("2FA code: ")

def save_credentials(username, password):
    consent = input("Do you want to save credentials for reuse? (y/n): ").strip().lower()
    if consent == "y":
        cred_file.write_text(json.dumps({"username": username, "password": password}))
        logging.info("Credentials saved to /app/ring_credential.json")
    else:
        logging.info("Credentials not saved.")

def load_credentials():
    if cred_file.is_file():
        creds = json.loads(cred_file.read_text())
        return creds.get("username"), creds.get("password")
    return None, None

async def do_auth():
    """Authenticate with Ring, handling 2FA with max 3 retries."""
    username, password = load_credentials()
    if not username or not password:
        username = input("Username: ")
        password = getpass.getpass("Password: ")
        save_credentials(username, password)

    auth = Auth(user_agent, None, token_updated)

    try:
        # First attempt without 2FA
        await auth.async_fetch_token(username, password)
    except Requires2FAError:
        max_attempts = 3
        attempts = 0
        while attempts < max_attempts:
            code = otp_callback()
            try:
                await auth.async_fetch_token(username, password, code)
                logging.info("2FA successful.")
                break
            except Requires2FAError:
                attempts += 1
                logging.warning("Invalid 2FA code, attempt %d of %d.", attempts, max_attempts)
                if attempts >= max_attempts:
                    logging.error("Maximum 2FA attempts reached. Exiting.")
                    raise
            except AuthenticationError:
                logging.error("Authentication failed. Check username/password.")
                raise
    return auth

async def main():
    auth = None
    try:
        if cache_file.is_file():
            cached_token = json.loads(cache_file.read_text())
            auth = Auth(user_agent, cached_token, token_updated)
            ring = Ring(auth)
            try:
                await ring.async_update_data()
                logging.info("Using cached token successfully.")
            except AuthenticationError:
                logging.warning("Cached token expired, re-authenticating...")
                await auth.async_close()
                auth = await do_auth()
                ring = Ring(auth)
                await ring.async_update_data()
        else:
            auth = await do_auth()
            ring = Ring(auth)
            await ring.async_update_data()

        devices = ring.devices()
        logging.info("Devices linked to your account:")
        logging.info("Doorbots (owned): %s", [d.name for d in devices.doorbots])
        logging.info("Authorized doorbots (shared): %s", [d.name for d in devices.authorized_doorbots])
        logging.info("Stickup cams: %s", [d.name for d in devices.stickup_cams])
        logging.info("Chimes: %s", [d.name for d in devices.chimes])

    finally:
        if auth:
            await auth.async_close()
            logging.info("Session closed cleanly.")

if __name__ == "__main__":
    asyncio.run(main())