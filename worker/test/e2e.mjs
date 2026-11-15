// Наскрізний тест Worker локально: wrangler dev + імітація Telegram (test/mock-telegram.mjs).
// Запуск: див. test/run.sh
import crypto from "node:crypto";
import assert from "node:assert/strict";

const W = "http://127.0.0.1:8787", TG = "http://127.0.0.1:9999";
const TOKEN = "test-token", ADMIN = 111;
const h = (label) => crypto.createHmac("sha256", TOKEN).update(label).digest("hex");
const KEY = h("prozorro-api"), HOOK = h("prozorro-webhook");

const api = async (path, body) => (await fetch(`${W}/api/${path}`, {
  method: body ? "POST" : "GET", headers: { Authorization: `Bearer ${KEY}`, "content-type": "application/json" },
  body: body ? JSON.stringify(body) : undefined })).json();
const calls = async () => (await fetch(`${TG}/__calls`)).json();
const reset = () => fetch(`${TG}/__reset`);
let upd = 1000;
const hook = (u, secret = HOOK) => fetch(`${W}/tg/hook`, { method: "POST",
  headers: { "content-type": "application/json", "X-Telegram-Bot-Api-Secret-Token": secret }, body: JSON.stringify(u) });
const click = (data, mid, from = ADMIN, update_id = ++upd) => hook({ update_id,
  callback_query: { id: `cb${update_id}`, from: { id: from }, data, message: { message_id: mid, chat: { id: ADMIN } } } });
const message = (text, reply_to) => hook({ update_id: ++upd,
  message: { message_id: 90000 + upd, from: { id: ADMIN }, chat: { id: ADMIN }, text,
    ...(reply_to ? { reply_to_message: { message_id: reply_to } } : {}) } });
const posts = async () => Object.fromEntries((await api("export")).posts.map((p) => [p.id, p]));
const wait = (ms) => new Promise((r) => setTimeout(r, ms));
const of = (cs, m) => cs.filter((c) => c.method === m);

let passed = 0;
const ok = (cond, msg) => { assert.ok(cond, msg); passed++; console.log("  ✓", msg); };

// 1. 12 чернеток -> у чаті 8, решта в черзі (найважливіші першими)
await reset();
const drafts = Array.from({ length: 12 }, (_, i) => ({ id: `UA-2026-11-${10 + i}-${String(i).padStart(6, "0")}-a`, kind: "tender",
  text_x: `Замовник ${i}: предмет, 1,0 млн грн.\nСигнал ризику, не доказ порушення.`, text_tg: null,
  context: `контекст ${i}`, priority: i, features: { i } }));
let r = await api("drafts", { drafts });
ok(r.added === 12 && r.sent === 8, `чернетки: додано ${r.added}, надіслано ${r.sent}`);
let P = await posts();
ok(Object.values(P).filter((p) => p.status === "in_review").length === 8, "8 на перевірці");
ok(P["UA-2026-11-21-000011-a"].status === "in_review" && P["UA-2026-11-10-000000-a"].status === "queued", "спершу вищий пріоритет");
r = await api("drafts", { drafts: drafts.slice(0, 2) });
ok(r.added === 0, "повтор тих самих id ігнорується");

// 2. вебхук без секрету відхиляється
ok((await hook({ update_id: 1 }, "wrong")).status === 403, "вебхук без правильного секрету: 403");

// 3. ✅ -> схвалено, відповідь на натискання, посилання X на тому самому повідомленні, вікно дозаповнюється
const A = "UA-2026-11-21-000011-a";
await reset();
await click(`ok:${A}`, P[A].review_msg_id);
await wait(300);
let cs = await calls();
P = await posts();
ok(P[A].status === "approved" && P[A].decided_by === String(ADMIN), "✅: статус approved");
ok(of(cs, "answerCallbackQuery").some((c) => /Схвалено/.test(c.params.text)), "✅: Telegram отримав відповідь на натискання");
const ed = of(cs, "editMessageText").find((c) => c.params.message_id === P[A].review_msg_id);
ok(ed && ed.params.reply_markup.inline_keyboard[0][0].url.startsWith("https://x.com/intent/post?text="), "✅: кнопка «Відкрити в X» на тому ж повідомленні");
ok(of(cs, "sendMessage").length === 1, "✅: надіслано наступну чернетку з черги");

// 4. повтор того самого оновлення не обробляється вдруге
await reset();
const dupId = ++upd;
await click(`no:UA-2026-11-20-000010-a`, P["UA-2026-11-20-000010-a"].review_msg_id, ADMIN, dupId);
await click(`no:UA-2026-11-20-000010-a`, P["UA-2026-11-20-000010-a"].review_msg_id, ADMIN, dupId);
await wait(300);
cs = await calls();
ok(of(cs, "answerCallbackQuery").length === 1, "повторне оновлення Telegram ігнорується");
P = await posts();
ok(P["UA-2026-11-20-000010-a"].status === "rejected" && P["UA-2026-11-20-000010-a"].review_msg_id === null, "❌: відхилено");
ok(of(cs, "deleteMessage").length === 1, "❌: повідомлення видалено");

// 5. ⏭ -> назад у чергу, не повертається одразу, поки є інші
await reset();
const S = "UA-2026-11-19-000009-a";
await click(`sk:${S}`, P[S].review_msg_id);
await wait(300);
P = await posts();
ok(P[S].status === "queued" && P[S].skips === 1, "⏭: назад у черзі");
cs = await calls();
ok(of(cs, "sendMessage").every((c) => !c.params.text.includes("контекст 9")), "⏭: замість неї прийшла інша");

// 6. ✏️ -> прохання відповісти; відповідь замінює текст
await reset();
const E = "UA-2026-11-18-000008-a";
await click(`ed:${E}`, P[E].review_msg_id);
cs = await calls();
const prompt = of(cs, "sendMessage")[0];
ok(prompt.params.reply_markup.force_reply, "✏️: бот просить новий текст");
P = await posts();
ok(P[E].status === "awaiting_edit", "✏️: статус awaiting_edit");
await message("Новий текст допису +link", P[E].edit_prompt_id);
P = await posts();
ok(P[E].status === "in_review" && P[E].edited === 1 && P[E].text_x.endsWith(`https://prozorro.gov.ua/tender/${E}`), "✏️: текст замінено, посилання додано");
await message("x".repeat(300), null);
P = await posts();
ok(P[E].text_x.startsWith("Новий текст"), "звичайне повідомлення без reply текст не змінює");

// 7. не-адміністратор
await reset();
await click(`ok:${E}`, P[E].review_msg_id, 999);
cs = await calls();
P = await posts();
ok(of(cs, "answerCallbackQuery")[0].params.text === "Немає прав" && P[E].status === "in_review", "чуже натискання ігнорується");

// 8. натискання на старій копії (ще до вебхука) для чернетки, що чекає в черзі: рішення приймається
await reset();
const Q = Object.values(P).find((p) => p.status === "queued" && p.id !== S).id;
await click(`ok:${Q}`, 42);   // id < 100: «старе» повідомлення
await wait(300);
P = await posts();
ok(P[Q].status === "approved" && P[Q].review_msg_id === 42, "стара копія: рішення зараховано");

// 9. натискання на вже обробленій
await reset();
await click(`ok:UA-2026-11-20-000010-a`, 43);
cs = await calls();
ok(/Вже оброблено/.test(of(cs, "answerCallbackQuery")[0].params.text), "оброблена чернетка: «Вже оброблено»");
ok(of(cs, "editMessageText").length === 1, "стару копію не видалити - позначено й кнопки прибрано");

// 10. розклад: публікація в канал (поза тихими годинами), закріплений «Розклад»
await reset();
await api("tick", {});
cs = await calls();
P = await posts();
const quiet = (() => { const h = +new Intl.DateTimeFormat("en-GB", { timeZone: "Europe/Kyiv", hour: "2-digit", hourCycle: "h23" }).format(new Date()); return h >= 23 || h < 8; })();
if (quiet) {
  ok(!of(cs, "sendMessage").some((c) => c.params.chat_id === "@test_channel"), "тихі години: у канал нічого");
} else {
  ok(of(cs, "sendMessage").some((c) => c.params.chat_id === "@test_channel"), "канал: опубліковано перший схвалений");
  ok(P[A].status === "published" && P[A].channel_msg_id > 0, "канал: статус published");
  await reset();
  await api("tick", {});
  ok(!of(await calls(), "sendMessage").some((c) => c.params.chat_id === "@test_channel"), "канал: не частіше ніж раз на годину");
}
ok(of(cs, "pinChatMessage").length === 1, "«Розклад» надіслано й закріплено");
await reset();
await api("tick", {});
ok(of(await calls(), "pinChatMessage").length === 0, "«Розклад» не змінився - нічого не надсилається");

// 11. команди
await reset();
await message("/queue");
cs = await calls();
ok(/У чаті на перевірці/.test(of(cs, "sendMessage")[0].params.text), "/queue відповідає станом черги");

// 12. приховування й оновлення текстів з Actions
await reset();
r = await api("suppress", { items: [{ id: E, reason: "військові закупівлі вимкнено" }] });
P = await posts();
ok(r.suppressed === 1 && P[E].status === "suppressed", "suppress: прибрано з перевірки");
r = await api("texts", { items: [{ id: Q, text_tg: "повний текст" }] });
ok(r.updated === 1, "texts: повний текст оновлено для схваленого");

// 13. перенесення зі старого журналу й журнал подій
r = await api("import", { posts: [{ id: "UA-2026-09-01-000001-a", kind: "tender", status: "rejected", text_x: "старий", created_at: "2026-09-01T00:00:00Z" }],
  meta: { schedule_msg_id: "1029" } });
ok(r.added === 1, "import: старий запис перенесено");
const ev = (await api("events")).events;
ok(ev.filter((e) => e.kind === "click").length === 7, `журнал подій: натискань ${ev.filter((e) => e.kind === "click").length}`);
ok(!ev.some((e) => e.kind === "error"), "без помилок у журналі");

console.log(`\nУсі перевірки пройдено: ${passed}`);
