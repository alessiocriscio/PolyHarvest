#!/bin/bash
# Runs as root from polyharvest-update.timer.
# Brings the checkout to origin/main and restarts the bot when main changed.
# If the new code does not compile or the bot does not stay up, the previous
# commit is restored and the bad one is remembered so it is not retried.
set -euo pipefail

REPO=/home/azureuser/bot_pm
STATE=/var/lib/polyharvest
BAD="$STATE/bad_commit"
git_repo() { runuser -u azureuser -- git -C "$REPO" "$@"; }

mkdir -p "$STATE"
git_repo fetch --quiet origin main
OLD=$(git_repo rev-parse HEAD)
NEW=$(git_repo rev-parse origin/main)

[ "$OLD" = "$NEW" ] && exit 0
if [ -f "$BAD" ] && [ "$(cat "$BAD")" = "$NEW" ]; then
    exit 0
fi

rollback() {
    echo "update to $NEW failed ($1), restoring $OLD"
    echo "$NEW" > "$BAD"
    git_repo reset --quiet --hard "$OLD"
    systemctl restart polyharvest.service
    exit 1
}

echo "updating $OLD -> $NEW"
git_repo reset --quiet --hard "$NEW"

runuser -u azureuser -- "$REPO/pm_env/bin/python" -m py_compile \
    "$REPO/bots/bot.py" "$REPO/bots/obi_engine.py" "$REPO/bots/pm_ws.py" || rollback "syntax error"

systemctl restart polyharvest.service
sleep 5
PID=$(systemctl show -p MainPID --value polyharvest.service)
sleep 20
if [ "$(systemctl is-active polyharvest.service)" != "active" ] \
   || [ "$(systemctl show -p MainPID --value polyharvest.service)" != "$PID" ]; then
    rollback "bot did not stay up"
fi
echo "update ok, bot running at $NEW"
