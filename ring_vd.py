import os
import json
import argparse
import asyncio
import datetime
import aiohttp
import zoneinfo
import hashlib
import time
import logging
from pathlib import Path
from ring_doorbell import Auth, Ring, AuthenticationError

USER_AGENT = "RingVD-1.0"
TOKEN_FILE = Path("/app/ring_token.json")
LOCAL_TZ = zoneinfo.ZoneInfo("Asia/Hong_Kong")  # UTC+8

# Telegram Bot Configuration
TELEGRAM_TG_CONFIG_FILE = Path("/app/tg.json")

# Sleep Configuration
SLEEP_SECONDS = 24 * 3600  # 24 hours

# Configure monthly error log
log_dir = Path("/tmp/ring_videos")
log_dir.mkdir(parents=True, exist_ok=True)
log_file = log_dir / ("error-%s.log" % datetime.datetime.now().strftime("%Y-%m"))
logging.basicConfig(
    filename=log_file,
    level=logging.ERROR,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def token_updated(token):
    TOKEN_FILE.write_text(json.dumps(token))

def md5sum(data):
    return hashlib.md5(data).hexdigest()

async def send_telegram_message(message):
    bot_token = None
    chat_id = None

    if TELEGRAM_TG_CONFIG_FILE.is_file():
        try:
            cfg = json.loads(TELEGRAM_TG_CONFIG_FILE.read_text())
            bot_token = cfg.get("bot_token")
            chat_id = cfg.get("chat_id")
        except Exception as e:
            print(f"❌ Failed to load Telegram config from {TELEGRAM_TG_CONFIG_FILE}: {e}")

    if not bot_token or not chat_id:
        print("Telegram bot token or chat ID not configured in /app/tg.json, skipping notification")
        return

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown"
    }

    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(url, json=payload) as resp:
                if resp.status == 200:
                    print("✅ Telegram message sent")
                else:
                    print(f"❌ Failed to send Telegram message: {resp.status}")
        except Exception as e:
            print(f"❌ Error sending Telegram message: {e}")

def generate_summary(stats):
    duration = str(datetime.timedelta(seconds=stats["duration_seconds"]))
    summary = f"""📹 **Ring Video Fetch Summary**

🕒 **Run Time:** {stats["start_time"]}
⏱️ **Duration:** {duration}
🏠 **Device:** {stats["device_name"]}

📊 **Statistics:**
- Total events in history: {stats["total_events"]}
- Events processed: {stats["events_processed"]}
- Videos downloaded: {stats["videos_downloaded"]}
- Videos skipped (already exist): {stats["videos_skipped"]}
- Errors encountered: {stats["errors"]}

✅ **Run completed successfully!**"""
    return summary

async def get_ring_old():
    if not TOKEN_FILE.is_file():
        # Infinite loop if token is missing
        while True:
            ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{ts}] Please run reauth.py to generate the token")
            time.sleep(60)  # wait 1 minute
    else:
        auth = Auth(USER_AGENT, json.loads(TOKEN_FILE.read_text()), token_updated)
        ring = Ring(auth)
        try:
            await ring.async_create_session()  # auth token still valid
        except AuthenticationError:  # auth token has expired
            auth = await do_auth()

    await ring.async_update_data()
    return ring, auth

async def get_ring():
    ring = None
    auth = None

    while True:
        if not TOKEN_FILE.is_file():
            ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{ts}] Token file missing. Please run reauth.py to generate the token.")
        else:
            try:
                auth = Auth(USER_AGENT, json.loads(TOKEN_FILE.read_text()), token_updated)
                ring = Ring(auth)
                await ring.async_create_session()  # validate token
                await ring.async_update_data()
                return ring, auth  # success → exit loop
            except AuthenticationError:
                ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print(f"[{ts}] Authentication failed. Token expired or invalid. Please run reauth.py.")

        # wait 1 minute before retrying
        await asyncio.sleep(60)

async def fetch_all_history(device, max_events=None, batch_size=100):
    all_events = []
    older_than = None
    while True:
        history = await device.async_history(limit=batch_size, older_than=older_than)
        if not history:
            break
        all_events.extend(history)
        older_than = history[-1]["id"]
        if max_events and len(all_events) >= max_events:
            break
    return all_events if not max_events else all_events[:max_events]

async def download_from_share_url(url, filename):
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.read()
                    new_md5 = md5sum(data)

                    if filename.exists():
                        existing_md5 = md5sum(filename.read_bytes())
                        if existing_md5 == new_md5:
                            print("✅ Skipped %s (MD5 match)" % filename)
                            return
                        else:
                            print("⚠️ MD5 mismatch, replacing %s" % filename)

                    with open(filename, "wb") as f:
                        f.write(data)
                    print("✅ Saved %s (%d bytes, MD5=%s)" % (filename, len(data), new_md5))
                else:
                    msg = "❌ Failed to download %s, status=%d" % (filename, resp.status)
                    print(msg)
                    logging.error(msg)
        except Exception as e:
            msg = "❌ Exception downloading %s: %s" % (filename, e)
            print(msg)
            logging.error(msg)

async def fetch_shared_videos(debug=False, all_videos=False):
    start_time = time.time()
    ring, auth = await get_ring()
    devices = ring.devices()

    if not devices.authorized_doorbots:
        print("No shared doorbots found.")
        await auth.async_close()
        return {"error": "No shared doorbots found"}

    device = devices.authorized_doorbots[0]
    print("Shared device: %s, subscription=%s" % (device.name, device.has_subscription))

    if all_videos:
        history = await fetch_all_history(device, max_events=None)
    else:
        history = await fetch_all_history(device, max_events=2000)

    cutoff = None
    if not all_videos:
        cutoff = datetime.datetime.now(LOCAL_TZ) - datetime.timedelta(hours=48)

    stats = {
        "device_name": device.name,
        "total_events": len(history),
        "events_processed": 0,
        "videos_downloaded": 0,
        "videos_skipped": 0,
        "errors": 0,
        "start_time": datetime.datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S"),
        "duration_seconds": 0
    }

    for event in history:
        created_at = event["created_at"].astimezone(LOCAL_TZ)
        if cutoff and created_at < cutoff:
            continue

        stats["events_processed"] += 1

        print("\nEvent %s at %s, status=%s" % (
            event["id"], created_at, event.get("recording", {}).get("status")
        ))

        try:
            url = await device.async_recording_url(event["id"])
            if debug:
                print("Share/play URL: %s" % url)
            if url:
                month_folder = Path("/tmp/ring_videos") / created_at.strftime("%Y-%m")
                month_folder.mkdir(parents=True, exist_ok=True)
                filename = month_folder / ("%s_%s.mp4" % (
                    created_at.strftime("%Y-%m-%d_%H-%M-%S"), event["id"]
                ))
                
                # Download and track stats
                async with aiohttp.ClientSession() as session:
                    try:
                        async with session.get(url) as resp:
                            if resp.status == 200:
                                data = await resp.read()
                                new_md5 = md5sum(data)

                                if filename.exists():
                                    existing_md5 = md5sum(filename.read_bytes())
                                    if existing_md5 == new_md5:
                                        print("✅ Skipped %s (MD5 match)" % filename)
                                        stats["videos_skipped"] += 1
                                        continue
                                    else:
                                        print("⚠️ MD5 mismatch, replacing %s" % filename)

                                with open(filename, "wb") as f:
                                    f.write(data)
                                print("✅ Saved %s (%d bytes, MD5=%s)" % (filename, len(data), new_md5))
                                stats["videos_downloaded"] += 1
                            else:
                                msg = "❌ Failed to download %s, status=%d" % (filename, resp.status)
                                print(msg)
                                logging.error(msg)
                                stats["errors"] += 1
                    except Exception as e:
                        msg = "❌ Exception downloading %s: %s" % (filename, e)
                        print(msg)
                        logging.error(msg)
                        stats["errors"] += 1
            else:
                msg = "⚠️ No share/play URL for event %s" % event["id"]
                print(msg)
                logging.error(msg)
                stats["errors"] += 1
        except Exception as e:
            msg = "❌ Error fetching URL for event %s: %s" % (event["id"], e)
            print(msg)
            logging.error(msg)
            stats["errors"] += 1

    await auth.async_close()
    
    end_time = time.time()
    stats["duration_seconds"] = int(end_time - start_time)
    
    if stats["errors"] > 0:
        print("⚠️ %d errors logged, see %s" % (stats["errors"], log_file))
    
    return stats

async def download_by_id(event_id, debug=False):
    ring, auth = await get_ring()
    devices = ring.devices()

    if not devices.authorized_doorbots:
        print("No shared doorbots found.")
        return

    device = devices.authorized_doorbots[0]
    print("Downloading event ID %s from device %s" % (event_id, device.name))

    try:
        url = await device.async_recording_url(event_id)
        if debug:
            print("Share/play URL: %s" % url)
        if url:
            created_at = datetime.datetime.now(LOCAL_TZ)
            month_folder = Path("/tmp/ring_videos") / created_at.strftime("%Y-%m")
            month_folder.mkdir(parents=True, exist_ok=True)
            filename = month_folder / ("%s_%s.mp4" % (
                created_at.strftime("%Y-%m-%d_%H-%M-%S"), event_id
            ))
            await download_from_share_url(url, filename)
        else:
            msg = "⚠️ No share/play URL for event %s" % event_id
            print(msg)
            logging.error(msg)
    except Exception as e:
        msg = "❌ Error fetching URL for event %s: %s" % (event_id, e)
        print(msg)
        logging.error(msg)

    await auth.async_close()

async def list_devices():
    ring, auth = await get_ring()
    devices = ring.devices()

    print("Devices linked to your account:")
    print("Doorbots (owned):", [d.name for d in devices.doorbots])
    print("Authorized doorbots (shared):", [d.name for d in devices.authorized_doorbots])
    print("Stickup cams:", [d.name for d in devices.stickup_cams])
    print("Chimes:", [d.name for d in devices.chimes])

    await auth.async_close()

async def list_videos(limit=10, debug=False):
    ring, auth = await get_ring()
    devices = ring.devices()

    if not devices.authorized_doorbots:
        print("No shared doorbots found.")
        return

    device = devices.authorized_doorbots[0]
    print("Listing videos for shared device: %s" % device.name)

    history = await fetch_all_history(device, max_events=limit)
    for idx, event in enumerate(history, start=1):
        created_at = event["created_at"].astimezone(LOCAL_TZ)
        print("%d. Event %s at %s, status=%s" % (
            idx, event["id"], created_at, event.get("recording", {}).get("status")
        ))
        if debug:
            try:
                url = await device.async_recording_url(event["id"])
                print("   Share/play URL: %s" % url)
            except Exception as e:
                print("   Error getting share/play URL: %s" % e)

    print("\nTotal events listed: %d" % len(history))
    await auth.async_close()

def main_loop(args):

    if args.downloadbyid:
        asyncio.run(download_by_id(args.downloadbyid, debug=args.debug))
        return

    # Flags → run once and exit
    if args.list_devices:
        asyncio.run(list_devices())
    elif args.list_videos:
        asyncio.run(list_videos(limit=args.limit, debug=args.debug))
    elif args.all:
        stats = asyncio.run(fetch_shared_videos(debug=args.debug, all_videos=True))
        if "error" not in stats:
            summary = generate_summary(stats)
            print(summary)
            asyncio.run(send_telegram_message(summary))
        print("✅ All videos downloaded, exiting.")
    else:
        # No flags → repeat daily
        while True:
            stats = asyncio.run(fetch_shared_videos(debug=args.debug, all_videos=False))
            if "error" not in stats:
                summary = generate_summary(stats)
                print(summary)
                asyncio.run(send_telegram_message(summary))
            print("✅ Run complete, sleeping %d hours (%d seconds)..." % (SLEEP_SECONDS // 3600, SLEEP_SECONDS))
            time.sleep(SLEEP_SECONDS)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ring Fetcher (share/play mode with pagination, daily loop)")
    parser.add_argument("--list-devices", action="store_true", help="List devices linked to account")
    parser.add_argument("--list-videos", action="store_true", help="List recent videos for shared doorbot")
    parser.add_argument("--limit", type=int, default=10, help="Number of events to list (for --list-videos)")
    parser.add_argument("--debug", action="store_true", help="Show share/play URLs")
    parser.add_argument("--all", action="store_true", help="Download all available videos once (ignore 48h cutoff)")
    parser.add_argument("--downloadbyid", type=str, help="Download a specific video by event ID")
    args = parser.parse_args()

    main_loop(args)