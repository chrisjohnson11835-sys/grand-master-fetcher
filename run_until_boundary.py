#!/usr/bin/env python3
# run_until_boundary.py (v20)
# Self-healing controller: run sec_only.py repeatedly (with backoff) until
# we see hit_boundary:true and entries_seen>0 in outputs/sec_debug_stats.json.
import json, os, sys, time, subprocess

ROOT = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(ROOT, "outputs")
STATS = os.path.join(OUT, "sec_debug_stats.json")
CKPT = os.path.join(OUT, "sec_checkpoint.json")

MAX_ATTEMPTS = int(os.environ.get("RUNNER_MAX_ATTEMPTS", "6"))
BACKOFFS = os.environ.get("RUNNER_BACKOFFS", "0,30,60,120,180,300").split(",")
BACKOFFS = [int(x) for x in BACKOFFS if str(x).strip().isdigit()]

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

def main():
    for attempt in range(1, MAX_ATTEMPTS+1):
        print(f"[runner] Attempt {attempt}/{MAX_ATTEMPTS} - launching sec_only.py")
        # Run the worker
        code = subprocess.call([sys.executable, os.path.join(ROOT, "sec_only.py")])
        print(f"[runner] sec_only.py exit code: {code}")

        stats = read_stats()
        print(f"[runner] stats snapshot: hit_boundary={stats.get('hit_boundary')} entries_seen={stats.get('entries_seen')} last_oldest_et_scanned={stats.get('last_oldest_et_scanned')}")

        if ok(stats):
            print("[runner] ✅ Boundary reached and entries present. Done.")
            sys.exit(0)

        # If not OK, back off and try again (resume mode will continue where we left off)
        back = BACKOFFS[min(attempt-1, len(BACKOFFS)-1)] if BACKOFFS else 60
        print(f"[runner] Not complete yet. Sleeping {back}s then retrying...")
        time.sleep(back)

    # Final read & exit with non-zero so CI notifies us
    stats = read_stats()
    print(f"[runner] ❌ Exhausted attempts. hit_boundary={stats.get('hit_boundary')} entries_seen={stats.get('entries_seen')}")
    sys.exit(1)

if __name__ == "__main__":
    main()
