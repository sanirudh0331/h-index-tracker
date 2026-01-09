#!/bin/bash
cd /Users/anirudhsudarshan/h_index_tracker
source venv/bin/activate
export PATH="$HOME/bin:$PATH"

echo "Starting remaining batches at $(date)" >> compute.log

while true; do
    remaining=$(sqlite3 data/hindex.db "SELECT COUNT(*) FROM researchers WHERE history_computed = 0;")

    if [ "$remaining" -eq 0 ]; then
        echo "All researchers processed!" >> compute.log
        break
    fi

    echo "=== Starting batch, $remaining remaining ===" >> compute.log
    caffeinate -i python3 scripts/compute_history.py --limit 10000 >> compute.log 2>&1

    # Push to GitHub after each batch
    git add data/hindex.db
    git commit -m "Update H-index history batch - $remaining remaining

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
    git push origin main
done

echo "Done! $(date)" >> compute.log
