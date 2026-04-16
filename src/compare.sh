#!/usr/bin/env bash
# compare.sh - CFS vs Round Robin Scheduler Comparison
#
# Runs 3 cpu_hog containers under each scheduler and measures:
#   - Total wall-clock time for all containers to complete
#   - Throughput (containers completed per second)
#   - RR overhead % vs CFS
#
# Must be run as root: sudo ./compare.sh

set -euo pipefail
cd "$(dirname "$0")"

# ─── Configuration ────────────────────────────────────────────
ENGINE="./engine"
DURATION=10          # seconds each cpu_hog runs
QUANTUM=500          # RR time quantum in milliseconds
N=3                  # number of containers

ROOTFS_DIRS=("./rootfs-alpha" "./rootfs-beta" "./rootfs-gamma")
IDS=("alpha" "beta" "gamma")

# ─── Colours ──────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'
BLUE='\033[0;34m'; YELLOW='\033[1;33m'; NC='\033[0m'

# ─── Helpers ──────────────────────────────────────────────────
die() { echo -e "${RED}FATAL: $1${NC}" >&2; exit 1; }

check_prereqs() {
    [[ $(id -u) -eq 0 ]] || die "Run as root: sudo ./compare.sh"
    [[ -x "$ENGINE" ]]    || die "engine binary not found — run: make"
    command -v bc >/dev/null 2>&1 || apt-get install -y bc >/dev/null 2>&1

    for d in "${ROOTFS_DIRS[@]}"; do
        [[ -d "$d" ]]        || die "Missing $d — see README rootfs setup"
        [[ -x "$d/bin/sh" ]] || die "$d has no /bin/sh"
    done
    [[ -x "./cpu_hog" ]] || die "cpu_hog binary not found — run: make"
}

setup_rootfs() {
    echo "[info] Copying static cpu_hog binary into each rootfs..." >&2
    for d in "${ROOTFS_DIRS[@]}"; do
        cp -f ./cpu_hog "$d/cpu_hog"
        chmod +x "$d/cpu_hog"
    done
}

cleanup() {
    pkill -f "engine supervisor" 2>/dev/null || true
    rm -f /tmp/mini_runtime.sock
    sleep 0.3
}

# Wait until all N containers have exited (no running or paused state).
# FIX: use $(( waited + 1 )) instead of (( waited++ )) to avoid
# bash arithmetic exit-code 1 killing the script under set -e.
wait_all_done() {
    local max_wait=$(( DURATION * 6 ))
    local waited=0
    while [[ $waited -lt $max_wait ]]; do
        sleep 1
        waited=$(( waited + 1 ))   # FIX: was (( waited++ )) — returned exit 1 when waited=0

        local running
        running=$("$ENGINE" ps 2>/dev/null | grep -cE 'running|paused' || echo "0")
        [[ "$running" -eq 0 ]] && return 0
    done

    # FIX: explicitly return failure so caller knows we timed out
    echo -e "${RED}WARNING: containers did not exit within ${max_wait}s${NC}" >&2
    return 1
}

# ─── Single benchmark run ─────────────────────────────────────
# All status output goes to stderr.
# Only the elapsed milliseconds are printed to stdout,
# so CFS_MS=$(run_benchmark ...) captures just the number.
run_benchmark() {
    local mode="$1"
    local label="$2"
    local extra=""
    [[ "$mode" == "rr" ]] && extra="--quantum $QUANTUM"

    echo -e "\n${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}" >&2
    echo -e "${BLUE} Experiment: $label${NC}" >&2
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}" >&2

    cleanup
    rm -rf logs/

    # Start supervisor in background; redirect its output to a log file
    # shellcheck disable=SC2086
    "$ENGINE" supervisor "${ROOTFS_DIRS[0]}" --scheduler "$mode" $extra \
        >"/tmp/supervisor_${mode}.log" 2>&1 &
    local sup_pid=$!
    sleep 1   # give supervisor time to bind the socket

    # Record wall-clock start time in milliseconds
    local t_start
    t_start=$(date +%s%3N)

    # Launch all containers
    local i
    for i in $(seq 0 $(( N - 1 ))); do
        "$ENGINE" start "${IDS[$i]}" "${ROOTFS_DIRS[$i]}" "/cpu_hog $DURATION" \
            >/dev/null 2>&1
        echo "  [+] started container '${IDS[$i]}'" >&2
    done

    echo "  Waiting for all containers to exit..." >&2
    wait_all_done || true   # warn but continue even if timeout

    local t_end
    t_end=$(date +%s%3N)
    local elapsed=$(( t_end - t_start ))

    # Print final container states
    echo "" >&2
    echo "  Final container states:" >&2
    "$ENGINE" ps 2>/dev/null | sed 's/^/    /' >&2

    # Print last log line per container (shows cpu_hog completion)
    echo "  Per-container output (last line):" >&2
    local id
    for id in "${IDS[@]}"; do
        if [[ -f "logs/${id}.log" ]]; then
            tail -1 "logs/${id}.log" | sed "s/^/    [${id}] /" >&2
        fi
    done

    echo -e "${GREEN}  ✓ $label finished in ${elapsed}ms${NC}" >&2

    # Cleanly stop supervisor
    kill "$sup_pid" 2>/dev/null || true
    wait "$sup_pid" 2>/dev/null || true
    cleanup

    # This is the only thing printed to stdout — captured by the caller
    echo "$elapsed"
}

# ─── Main ─────────────────────────────────────────────────────
check_prereqs
setup_rootfs

echo -e "${YELLOW}"
cat <<'BANNER'
╔═══════════════════════════════════════════════════╗
║   CFS  vs  Round Robin  –  Scheduler Comparison   ║
╚═══════════════════════════════════════════════════╝
BANNER
echo -e "${NC}"
echo "  Workload  : $N × cpu_hog (${DURATION}s CPU-bound each)"
echo "  RR Quantum: ${QUANTUM} ms"
echo "  Rootfs    : Alpine Linux (static cpu_hog binary)"
echo ""

CFS_MS=$(run_benchmark "cfs" "CFS (Linux native scheduler)")
RR_MS=$( run_benchmark "rr"  "Round Robin (user-space, quantum=${QUANTUM}ms)")

# ─── Compute metrics ──────────────────────────────────────────
CFS_S=$(  echo "scale=3; $CFS_MS / 1000" | bc)
RR_S=$(   echo "scale=3; $RR_MS  / 1000" | bc)

# Throughput = containers / wall_time_seconds
CFS_TP=$( echo "scale=4; $N * 1000 / $CFS_MS" | bc)
RR_TP=$(  echo "scale=4; $N * 1000 / $RR_MS"  | bc)

# Overhead = extra time RR adds over CFS (%)
if [[ $CFS_MS -gt 0 ]]; then
    OVERHEAD=$(echo "scale=1; ($RR_MS - $CFS_MS) * 100 / $CFS_MS" | bc)
else
    OVERHEAD="N/A"
fi

# ─── Results table ────────────────────────────────────────────
echo ""
echo -e "${YELLOW}╔═══════════════════════════════════════════════════════════╗"
echo "║                       RESULTS                            ║"
echo -e "╚═══════════════════════════════════════════════════════════╝${NC}"
echo ""
printf "  %-38s %12s %12s\n" "Metric"                     "CFS"         "Round Robin"
printf "  %-38s %12s %12s\n" "------"                     "---"         "-----------"
printf "  %-38s %11ss %11ss\n" "Total wall time"           "$CFS_S"      "$RR_S"
printf "  %-38s %12s %12s\n"  "Throughput (containers/s)"  "$CFS_TP"     "$RR_TP"
printf "  %-38s %12s %11s%%\n" "RR overhead vs CFS"       "baseline"    "$OVERHEAD"
echo ""

echo -e "${YELLOW}Analysis:${NC}"
cat <<EOF
  CFS (Completely Fair Scheduler):
    * Linux kernel schedules containers natively using virtual runtime
    * Each process accumulates vruntime proportional to CPU usage
    * CPU-bound containers get equal shares automatically
    * I/O-bound processes get a wakeup bonus, improving response time

  Round Robin (user-space, quantum=${QUANTUM}ms):
    * Supervisor sends SIGSTOP/SIGCONT every ${QUANTUM}ms
    * Only ONE container runs at a time (others are frozen)
    * Fair for CPU-bound tasks, but adds signal overhead
    * Does NOT give any boost to I/O-bound tasks (unlike CFS)
    * Predictable, explicit time slices — easier to reason about

  Expected outcome for CPU-bound workloads (cpu_hog):
    * Both show similar total wall time (both are fair)
    * RR is slightly slower due to SIGSTOP/SIGCONT overhead
    * All containers finish at roughly the same time (good fairness)

  To see the CFS advantage more clearly, try I/O-bound workload:
    Edit this script: replace "/cpu_hog $DURATION" with "/io_pulse 30 200"
    CFS will complete io_pulse faster; RR treats it the same as CPU tasks.
EOF

echo ""
echo "Log files saved in:    ./logs/"
echo "Supervisor logs:       /tmp/supervisor_cfs.log  /tmp/supervisor_rr.log"