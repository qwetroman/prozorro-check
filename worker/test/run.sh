#!/usr/bin/env bash
# Локальний наскрізний тест Worker: імітація Telegram + wrangler dev з чистою локальною базою D1.
set -euo pipefail
cd "$(dirname "$0")/.."
WRANGLER=${WRANGLER:-npx wrangler}
[ -f .dev.vars ] || printf 'TG_BOT_TOKEN=test-token\nTG_ADMIN_IDS=111\nTG_REVIEW_CHAT_ID=111\nTG_CHANNEL_ID=@test_channel\nTG_API=http://127.0.0.1:9999\n' > .dev.vars
rm -rf .wrangler/state
$WRANGLER d1 migrations apply DB --local >/dev/null
node test/mock-telegram.mjs & MOCK=$!
XVARS=""
[ -n "${X:-}" ] && XVARS="--var X_API_KEY:k --var X_API_SECRET:s --var X_ACCESS_TOKEN:t --var X_ACCESS_SECRET:a --var X_API:http://127.0.0.1:9999"
$WRANGLER dev --local --port 8787 $XVARS >/tmp/wrangler-dev.log 2>&1 & DEV=$!
trap 'kill $MOCK $DEV 2>/dev/null || true' EXIT
for i in $(seq 1 60); do curl -s -o /dev/null http://127.0.0.1:8787/ && break; sleep 1; done
node test/x-oauth.mjs
if [ -n "${X:-}" ]; then node test/e2e-x.mjs; else node test/e2e.mjs; fi
