# Prozorro anomaly bot (GitHub Actions)

Every 30 minutes GitHub runs one pass:

1. Fetch new Prozorro tenders and flag anomalies.
2. Send strong new cases to your Telegram for review (✅ Опублікувати / ✏️ Редагувати / ❌ Відхилити).
3. Publish what you approved to X.

Nothing is posted without your tap. Taps are picked up on the next pass, so approvals take effect within 30–60 minutes.

**Where state lives**

- The tender database is in the Actions cache, saved each run and kept as long as the bot runs at least weekly.
- Your decisions and the fetch position are in `state/ledger.json`, committed to the repo by the bot. If the cache is ever lost, nothing is re-offered and nothing is posted twice.

## Setup (done by you)

1. **Create a repository.**
   - Public: Actions minutes are free and unlimited.
   - Private: the free plan includes 2,000 minutes a month. A pass takes about 1.5–3 minutes, so change the cron line in `.github/workflows/prozorro.yml` to hourly (`"7 * * * *"`).
2. **Upload these files** (web UI: *Add file → Upload files*, drag the whole folder so `.github/workflows/` is kept).
3. **Add secrets:** *Settings → Secrets and variables → Actions → New repository secret*. Add one for each line in `.env.example`:

   | Secret | Required? |
   | --- | --- |
   | `TG_BOT_TOKEN`, `TG_ADMIN_IDS`, `TG_REVIEW_CHAT_ID` | required |
   | `X_API_KEY`, `X_API_SECRET`, `X_ACCESS_TOKEN`, `X_ACCESS_SECRET` | required for X |
   | `TG_CHANNEL_ID` | optional |

4. **Allow the bot to commit:** *Settings → Actions → General → Workflow permissions → Read and write*.
5. **First fill:** *Actions → prozorro-bot → Run workflow*, with `since` = a date about 10 days ago and `limit` = 40000. Repeat with an empty `since` until the log stops saying "hit max_tenders_per_run". Each run takes ≤ ~15 min, and every run continues where the last stopped.
6. Send `/start` to your Telegram bot. Drafts start arriving after the next scheduled pass.

## Day to day

- Review drafts in Telegram.
- `/pause` and `/resume` stop and restart publishing. `/queue` shows what's waiting.
- Tune volume and thresholds in `publisher.json`, then commit.
- Test without posting by running a pass locally with `--dry-run`, e.g. `python prozorro_publisher.py --dry-run ci`.

## Notes

- GitHub can start scheduled runs 5–20 minutes late.
- In public repositories, schedules pause after 60 days without commits. The bot's ledger commits normally keep it active.
- Military purchases are anonymised ("Військовий замовник", no unit numbers) and delayed 14 days.
