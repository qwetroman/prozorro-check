// X через API: схвалення -> допис у X на найближчому проході -> посилання в повідомленні. Запуск: X=1 test/run.sh
import crypto from "node:crypto";
import assert from "node:assert/strict";
const W = "http://127.0.0.1:8787", TG = "http://127.0.0.1:9999", ADMIN = 111;
const KEY = crypto.createHmac("sha256", "test-token").update("prozorro-api").digest("hex");
const HOOK = crypto.createHmac("sha256", "test-token").update("prozorro-webhook").digest("hex");
const api = async (path, body) => (await fetch(`${W}/api/${path}`, { method: body ? "POST" : "GET",
  headers: { Authorization: `Bearer ${KEY}`, "content-type": "application/json" }, body: body ? JSON.stringify(body) : undefined })).json();
const calls = async () => (await fetch(`${TG}/__calls`)).json();
const posts = async () => Object.fromEntries((await api("export?limit=500")).posts.map((p) => [p.id, p]));
let passed = 0;
const ok = (c, m) => { assert.ok(c, m); passed++; console.log("  ✓", m); };
const quiet = (() => { const h = +new Intl.DateTimeFormat("en-GB", { timeZone: "Europe/Kyiv", hour: "2-digit", hourCycle: "h23" }).format(new Date()); return h >= 23 || h < 8; })();

const id = "UA-2026-11-14-000777-a";
await api("drafts", { drafts: [{ id, text_x: "Допис для X через API.\nСигнал ризику, не доказ порушення.", context: "к", priority: 9 }] });
await fetch(`${W}/tg/hook`, { method: "POST", headers: { "content-type": "application/json", "X-Telegram-Bot-Api-Secret-Token": HOOK },
  body: JSON.stringify({ update_id: 0, message: { message_id: 1, from: { id: ADMIN }, chat: { id: ADMIN }, text: "/next" } }) });
let P = await posts();
await fetch(`${W}/tg/hook`, { method: "POST", headers: { "content-type": "application/json", "X-Telegram-Bot-Api-Secret-Token": HOOK },
  body: JSON.stringify({ update_id: 1, callback_query: { id: "c1", from: { id: ADMIN }, data: `ok:${id}`, message: { message_id: P[id].review_msg_id } } }) });
P = await posts();
ok(P[id].status === "approved" && !P[id].x_sent_at, "✅ з ключами X: чекає на публікацію в X, посилання вручну не надсилається");
let cs = await calls();
ok(!cs.some((c) => c.method === "editMessageText" && JSON.stringify(c.params.reply_markup).includes("x.com/intent")), "немає кнопки «Відкрити в X»");
await fetch(`${TG}/__reset`);
await api("tick", {});
cs = await calls();
P = await posts();
if (quiet) {
  ok(!cs.some((c) => c.method === "x:tweet"), "тихі години: у X нічого");
} else {
  const tw = cs.find((c) => c.method === "x:tweet");
  ok(tw && tw.params.text === P[id].text_x && /^OAuth .*oauth_signature="/.test(tw.auth), "допис у X з підписом OAuth");
  ok(P[id].x_id && P[id].x_sent_at, `x_id збережено: ${P[id].x_id}`);
  ok(cs.some((c) => c.method === "editMessageText" && c.params.text.includes(`x.com/i/web/status/${P[id].x_id}`)), "у повідомленні посилання на допис");
  await fetch(`${TG}/__reset`);
  await api("tick", {});
  ok(!(await calls()).some((c) => c.method === "x:tweet"), "вдруге той самий допис не публікується");
}
console.log(`\nX: усі перевірки пройдено: ${passed}`);
