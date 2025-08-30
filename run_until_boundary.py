#!/usr/bin/env python3
# run_until_boundary.py (v20.1)
# Self-healing controller: run sec_only.py repeatedly (with backoff) until
# we see hit_boundary==true and entries_seen>0 in outputs/sec_debug_stats.json.
# New: time-aware exit with RUNNER_MAX_WALL_SECS so the job never overruns the Actions timeout.

import json, os, sys, time, subprocess

ROOT = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(ROOT, "outputs")
STATS = os.path.join(OUT, "sec_debug_stats.json")

MAX_ATTEMPTS = int(os.environ.get("RUNNER_MAX_ATTEMPTS", "8"))
BACKOFFS = os.environ.get("RUNNER_BACKOFFS", "0,45,90,180,300,420,600,900").split(",")
BACKOFFS = [int(x) for x in BACKOFFS if str(x).strip().isdigit()]
MAX_WALL = int(os.environ.get("RUNNER_MAX_WALL_SECS", "6600"))  # default ~110 minutes

def read_stats():
    try:
        with open(STATS, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def ok(stats):
    hb = stats.get("hit_boundary") is True
    eseen = int(stats.get("entries_seen", 0))
    return hb and eseen > 0

def now():
    return int(time.time())

def main():
    t0 = now()
    for attempt in range(1, MAX_ATTEMPTS+1):
        elapsed = now() - t0
        if elapsed >= MAX_WALL:
            print(f"[runner] ⏱ Reached wall clock limit ({elapsed}s >= {MAX_WALL}s). Exiting gracefully for next run to resume.")
            sys.exit(2)

        print(f"[runner] Attempt {attempt}/{MAX_ATTEMPTS} (elapsed {elapsed}s) - launching sec_only.py")
        code = subprocess.call([sys.executable, os.path.join(ROOT, "sec_only.py")])
        print(f"[runner] sec_only.py exit code: {code}")

        stats = read_stats()
        print(f"[runner] stats snapshot: hit_boundary={stats.get('hit_boundary')} entries_seen={stats.get('entries_seen')} last_oldest_et_scanned={stats.get('last_oldest_et_scanned')}")

        if ok(stats):
            print("[runner] ✅ Boundary reached and entries present. Done.")
            sys.exit(0)

        back = BACKOFFS[min(attempt-1, len(BACKOFFS)-1)] if BACKOFFS else 60
        remaining = MAX_WALL - (now() - t0)
        if back > remaining:
            back = max(0, remaining - 5)
        print(f"[runner] Not complete yet. Sleeping {back}s then retrying...")
        time.sleep(back)

    stats = read_stats()
    print(f"[runner] ❌ Exhausted attempts. hit_boundary={stats.get('hit_boundary')} entries_seen={stats.get('entries_seen')}")
    sys.exit(1)

if __name__ == "__main__":
    main()
