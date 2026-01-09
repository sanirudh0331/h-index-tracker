#!/bin/bash
cd /Users/anirudhsudarshan/h_index_tracker
export PATH="$HOME/bin:$PATH"
echo "Starting batch at $(date)" >> compute.log
caffeinate -i python3 scripts/compute_history.py --limit 50000 >> compute.log 2>&1
echo "Batch complete at $(date)" >> compute.log

# Push to GitHub when done
git add data/hindex.db
git commit -m "Complete H-index history for all researchers

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
git push origin main
