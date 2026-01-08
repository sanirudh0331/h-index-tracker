#!/bin/bash
cd /Users/anirudhsudarshan/h_index_tracker
export PATH="$HOME/bin:$PATH"

echo "Starting batch processing at $(date)" >> compute.log

# Run batches until no more candidates
while true; do
    remaining=$(sqlite3 data/hindex.db "SELECT COUNT(*) FROM researchers WHERE h_index BETWEEN 2 AND 30 AND history_computed = 0;")
    
    if [ "$remaining" -eq 0 ]; then
        echo "All candidates processed!" >> compute.log
        break
    fi
    
    echo "=== Starting batch, $remaining remaining ===" >> compute.log
    python3 scripts/compute_history.py --limit 5000 >> compute.log 2>&1
done

# Push to GitHub
echo "Pushing to GitHub at $(date)" >> compute.log
git add data/hindex.db
git commit -m "Complete H-index history for all candidates

Computed historical H-index (2015-2025) for all researchers with H-index 2-30

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
git push origin main

echo "Done! $(date)" >> compute.log
