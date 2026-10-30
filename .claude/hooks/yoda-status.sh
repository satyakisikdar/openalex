#!/bin/sh
# SessionStart: report Yoda job status so a killed run is flagged next session (data.md rule: a log with START but
# no END marker is a killed run unless the job is still running). Read-only; gives up quickly when Yoda is offline.
out=$(timeout 12 ssh -o BatchMode=yes -o ConnectTimeout=5 yoda 'sh -s' 2>/dev/null <<'EOF'
echo "Running jobs:"
ps -eo pid,etime,args | grep -E '[p]ython.*(optimize_parquet|flatten_openalex_files|filter_dataset|audit_)' \
  | grep -v 'uv run' | cut -c1-140 | sed 's/^/  /' || true
echo "Logs from the last 7 days with START but no END (running or killed):"
find /home/ssikdar/scratch -maxdepth 2 -name '*.log' -mtime -7 2>/dev/null | sort | while read -r f; do
  if grep -q 'START' "$f" && ! grep -q 'END' "$f"; then
    echo "  $f (last: $(tail -n 1 "$f" | cut -c1-100))"
  fi
done
echo "Disk: $(df -h /data | tail -n 1 | awk '{print $4 " free, " $5 " used"}')"
EOF
)
if [ -n "$out" ]; then
  printf 'Yoda status (from .claude/hooks/yoda-status.sh):\n%s\n' "$out"
else
  echo "Yoda status: unreachable (offline or ssh failed); job state unknown."
fi
exit 0
