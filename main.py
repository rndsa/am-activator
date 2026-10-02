#!/usr/bin/env python3
"""
AM Auto-Activator — Unlimited / Turbo Mode with Dynamic Config Hot-Reload & Circuit Breaker
Pipeline: rzmail (create) -> AM Gateway (send-link) -> rzmail (poll) -> AM Gateway (verify-link)
Optimized for high-throughput unattended daemon mode in Pterodactyl container.
"""

import sys
import os
import time
import re
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from threading import Lock

# Defaults
DEFAULT_AM_PRIMARY = "https://v.axjet.xyz"
DEFAULT_KEY_PRIMARY = "am-sk-29bf295c45cddff5cbb0c31e143b4feb"
DEFAULT_RZMAIL_BASE = "https://rzmail.my.id"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(SCRIPT_DIR, "am_accounts.txt")
CONFIG_FILE = os.path.join(SCRIPT_DIR, "config.json")

# High-performance Session Factory with large connection pool
def create_session():
    session = requests.Session()
    retries = Retry(
        total=2,
        backoff_factor=0.2,
        status_forcelist=[502, 503, 504],
        raise_on_status=False
    )
    adapter = HTTPAdapter(
        max_retries=retries,
        pool_connections=50,
        pool_maxsize=100
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

session = create_session()

# Thread-safe stats & circuit breaker
data_lock = Lock()
circuit_breaker_lock = Lock()
circuit_breaker_until = 0.0

stats = {
    "success": 0,
    "failed": 0,
    "start_time": time.time(),
    "accounts": []
}

def log_event(text):
    now = datetime.now().strftime("%H:%M:%S")
    line = f"[{now}] {text}"
    print(line, flush=True)

def load_live_config():
    """Read config.json dynamically with graceful fallback."""
    defaults = {
        "concurrency": 8,
        "delay_seconds": 0.2,
        "poll_interval": 0.6,
        "target_count": 0,
        "am_gateway": DEFAULT_AM_PRIMARY,
        "am_api_key": DEFAULT_KEY_PRIMARY,
        "rzmail_api_url": DEFAULT_RZMAIL_BASE
    }
    if not os.path.exists(CONFIG_FILE):
        return defaults
    try:
        with open(CONFIG_FILE, "r") as f:
            data = json.load(f)
            return {
                "concurrency": max(1, min(30, int(data.get("concurrency", 8)))),
                "delay_seconds": max(0.0, float(data.get("delay_seconds", 0.2))),
                "poll_interval": max(0.3, float(data.get("poll_interval", 0.6))),
                "target_count": int(data.get("target_count", 0)),
                "am_gateway": str(data.get("am_gateway", DEFAULT_AM_PRIMARY)).strip() or DEFAULT_AM_PRIMARY,
                "am_api_key": str(data.get("am_api_key", DEFAULT_KEY_PRIMARY)).strip() or DEFAULT_KEY_PRIMARY,
                "rzmail_api_url": str(data.get("rzmail_api_url", DEFAULT_RZMAIL_BASE)).strip() or DEFAULT_RZMAIL_BASE
            }
    except Exception:
        return defaults

def activate_one_account(worker_idx, gateway, api_key, rzmail_url, poll_interval):
    """Execute single flow: tempmail -> send link -> fetch magic link -> verify pro."""
    global circuit_breaker_until
    tag = f"[#{worker_idx:05d}]"
    start_time = time.time()

    # Pre-check circuit breaker
    with circuit_breaker_lock:
        if time.time() < circuit_breaker_until:
            return False

    # 1. Create temporary mailbox
    try:
        r = session.get(f"{rzmail_url}/api/create", timeout=12)
        inbox = r.json()
        email = inbox.get("address")
        if not email:
            log_event(f"{tag} ❌ Gagal create inbox ({rzmail_url})")
            with data_lock:
                stats["failed"] += 1
            return False
    except Exception as e:
        log_event(f"{tag} ❌ Mail create timeout/error ({rzmail_url}): {e}")
        with data_lock:
            stats["failed"] += 1
        return False

    log_event(f"{tag} 📧 Inbox: {email} | Kirim link...")

    # 2. Send magic link via AM gateway
    active_gw = gateway.rstrip("/")
    active_key = api_key

    send_ok = False
    candidates = [(active_gw, active_key)]
    err_msg = ""

    for gw, k in candidates:
        try:
            r = session.post(
                f"{gw}/api/send-link",
                json={"email": email},
                headers={"Content-Type": "application/json", "x-api-key": k},
                timeout=15
            )
            res = r.json()
            if r.status_code == 200 and res.get("success"):
                send_ok = True
                active_gw = gw
                active_key = k
                break
            else:
                err_msg = res.get("message") or res.get("code") or r.text
        except Exception as e:
            err_msg = str(e)
            continue

    if not send_ok:
        err_lower = err_msg.lower()
        is_quota = "quota" in err_lower or "exceeded" in err_lower
        is_ratelimit = "too_many_attempts" in err_lower or "terlalu banyak" in err_lower

        if is_quota:
            log_event(f"{tag} ❌ Gateway reject ({active_gw}): QUOTA EXCEEDED (Limit harian Firebase upstream habis)")
            with circuit_breaker_lock:
                if time.time() >= circuit_breaker_until:
                    circuit_breaker_until = time.time() + 60
                    log_event("⚠️  [CIRCUIT BREAKER] Kuota harian Firebase habis (25.000 email/hari). Cooling down 60s...")
        elif is_ratelimit:
            log_event(f"{tag} ❌ Gateway reject ({active_gw}): Rate limited upstream. Cooldown 15s...")
            with circuit_breaker_lock:
                if time.time() >= circuit_breaker_until:
                    circuit_breaker_until = time.time() + 15
        else:
            clean_err = err_msg[:75] if err_msg else "Unknown rejection"
            log_event(f"{tag} ❌ Gagal send-link: {clean_err}")

        with data_lock:
            stats["failed"] += 1
        return False

    # 3. Poll incoming email for magic link (max 30s)
    found_link = None
    poll_sleep = max(0.3, float(poll_interval))
    max_attempts = int(30 / poll_sleep)
    for _ in range(max_attempts):
        time.sleep(poll_sleep)
        try:
            r = session.get(f"{rzmail_url}/api/messages/{email}", timeout=8)
            data = r.json()
            if data.get("count", 0) > 0:
                msg_id = data["messages"][0]["id"]
                r_det = session.get(f"{rzmail_url}/api/inboxes/{email}/messages/{msg_id}", timeout=8)
                msg_det = r_det.json()
                body = msg_det.get("message", {}).get("body_html") or msg_det.get("message", {}).get("body_text") or ""
                
                links = re.findall(r"https://alightcreative\.com/[^\s\"'<>\\]+", body)
                if not links:
                    links = re.findall(r"https://[^\s\"'<>\\]+oobCode=[^\s\"'<>\\]+", body)
                if links:
                    found_link = links[0]
                    break
        except Exception:
            pass

    if not found_link:
        log_event(f"{tag} ⏱️ Timeout tunggu email ({email})")
        with data_lock:
            stats["failed"] += 1
        return False

    # 4. Verify link & provision VIP membership
    try:
        r = session.post(
            f"{active_gw}/api/verify-link",
            json={"email": email, "link": found_link},
            headers={"Content-Type": "application/json", "x-api-key": active_key},
            timeout=25
        )
        res = r.json()
        dur = round(time.time() - start_time, 1)

        if r.status_code == 200 and res.get("success"):
            dt = res.get("data", {})
            uid = dt.get("uid", "-")
            membership = dt.get("membership", "ACTIVE")
            valid_until = dt.get("validUntil", "-")

            with data_lock:
                stats["success"] += 1
                stats["accounts"].append(email)

            # Append to file
            line = f"{email}|UID:{uid}|Status:{membership}|ValidUntil:{valid_until}\n"
            with open(OUTPUT_FILE, "a") as f:
                f.write(line)
                f.flush()

            elapsed = max(round(time.time() - stats["start_time"], 1), 0.1)
            rate = round(stats["success"] / (elapsed / 60), 1)
            log_event(f"{tag} ✅ SUCCESS ({dur}s) | {email} | UID: {uid[:10]}.. | Exp: {valid_until}")
            log_event(f"    📊 Total Sukses: {stats['success']} | Gagal: {stats['failed']} | Speed: {rate} acc/menit")
            return True
        else:
            log_event(f"{tag} ❌ Verify ditolak: {res.get('message')}")
            with data_lock:
                stats["failed"] += 1
            return False
    except Exception as e:
        log_event(f"{tag} ❌ Error verify: {e}")
        with data_lock:
            stats["failed"] += 1
        return False

def main():
    cfg = load_live_config()
    concurrency = int(cfg.get("concurrency", 8))
    delay = float(cfg.get("delay_seconds", 0.2))
    poll_interval = float(cfg.get("poll_interval", 0.6))
    target_count = int(cfg.get("target_count", 0))
    am_gw = str(cfg.get("am_gateway", DEFAULT_AM_PRIMARY)).strip() or DEFAULT_AM_PRIMARY
    am_key = str(cfg.get("am_api_key", DEFAULT_KEY_PRIMARY)).strip() or DEFAULT_KEY_PRIMARY
    rz_url = str(cfg.get("rzmail_api_url", DEFAULT_RZMAIL_BASE)).strip() or DEFAULT_RZMAIL_BASE

    unlimited = (target_count <= 0)
    target_str = "∞ (TANPA BATAS / UNLIMITED)" if unlimited else f"{target_count} akun"
    
    log_event("=" * 60)
    log_event("🚀 AM AUTO-ACTIVATOR (HOT-RELOAD ENGINE)")
    log_event(f"🎯 Target         : {target_str}")
    log_event(f"⚙️  Worker Paralel : {concurrency} worker")
    log_event(f"⏱️  Jeda Dispatch  : {delay}s")
    log_event(f"🔍 Cek Email Tiap : {poll_interval}s")
    log_event(f"🌐 AM Gateway     : {am_gw}")
    log_event(f"🔑 AM Key         : {am_key[:8]}...{am_key[-4:] if len(am_key) > 12 else ''}")
    log_event(f"📬 RZMail URL     : {rz_url}")
    log_event(f"📁 Config File    : {CONFIG_FILE}")
    log_event(f"💾 Output File    : {OUTPUT_FILE}")
    log_event("=" * 60)

    worker_counter = 0
    last_cfg_check = time.time()
    last_known_cfg = (concurrency, delay, poll_interval, target_count, am_gw, am_key, rz_url)

    MAX_EXECUTOR_POOL = 30
    executor = ThreadPoolExecutor(max_workers=MAX_EXECUTOR_POOL)
    active_futures = set()

    try:
        while True:
            # Check for config file updates every 2 seconds
            now = time.time()
            if now - last_cfg_check >= 2.0:
                last_cfg_check = now
                new_cfg = load_live_config()
                new_conc = max(1, min(MAX_EXECUTOR_POOL, int(new_cfg.get("concurrency", concurrency))))
                new_delay = max(0.0, float(new_cfg.get("delay_seconds", delay)))
                new_poll = max(0.3, float(new_cfg.get("poll_interval", poll_interval)))
                new_target = int(new_cfg.get("target_count", target_count))
                new_gw = str(new_cfg.get("am_gateway", am_gw)).strip() or DEFAULT_AM_PRIMARY
                new_key = str(new_cfg.get("am_api_key", am_key)).strip() or DEFAULT_KEY_PRIMARY
                new_rz = str(new_cfg.get("rzmail_api_url", rz_url)).strip() or DEFAULT_RZMAIL_BASE

                current_tuple = (new_conc, new_delay, new_poll, new_target, new_gw, new_key, new_rz)
                if current_tuple != last_known_cfg:
                    concurrency, delay, poll_interval, target_count, am_gw, am_key, rz_url = current_tuple
                    last_known_cfg = current_tuple
                    log_event("⚙️  [HOT-RELOAD] Config diperbarui realtime:")
                    log_event(f"    • Worker: {concurrency} | Delay: {delay}s | Poll: {poll_interval}s | Target: {target_count}")
                    log_event(f"    • AM GW: {am_gw} | RZMail: {rz_url}")

            # Check if target reached (if not unlimited)
            if target_count > 0 and stats["success"] >= target_count:
                log_event(f"🎯 Target {target_count} akun telah tercapai!")
                break

            # Handle Circuit Breaker pause
            if time.time() < circuit_breaker_until:
                time.sleep(1.0)
                continue

            # Fill up workers up to concurrency
            while len(active_futures) < concurrency:
                if target_count > 0 and (worker_counter >= target_count or stats["success"] >= target_count):
                    break
                if time.time() < circuit_breaker_until:
                    break
                worker_counter += 1
                fut = executor.submit(activate_one_account, worker_counter, am_gw, am_key, rz_url, poll_interval)
                active_futures.add(fut)
                if delay > 0:
                    time.sleep(delay)

            # Wait for at least one worker to finish
            if active_futures:
                done = set()
                for fut in as_completed(active_futures):
                    done.add(fut)
                    break
                active_futures.difference_update(done)
            else:
                if target_count > 0 and stats["success"] >= target_count:
                    break
                time.sleep(0.5)

    except KeyboardInterrupt:
        log_event("\n[!] Dihentikan user (SIGINT/Stop).")
    finally:
        executor.shutdown(wait=False)

    elapsed = round(time.time() - stats["start_time"], 1)
    log_event("=" * 60)
    log_event(f"✨ STATUS AKHIR: Sukses: {stats['success']} | Gagal: {stats['failed']} | Waktu: {elapsed}s")
    log_event(f"📁 Hasil tersimpan di: {OUTPUT_FILE}")
    log_event("=" * 60)

if __name__ == "__main__":
    main()
