// «Прозорро Монітор»: Cloudflare Worker - єдине сховище чернеток і рішень, обробка натискань Telegram
// у момент натискання (вебхук), публікація в канал за розкладом.
//
// Маршрути:
//   POST /tg/hook          - вебхук Telegram (перевіряється заголовок X-Telegram-Bot-Api-Secret-Token)
//   /api/*                 - для GitHub Actions (заголовок Authorization: Bearer <ключ>)
// Ключ API і секрет вебхука виводяться з TG_BOT_TOKEN (HMAC), тож окремих секретів не потрібно.
// Розклад (cron): прострочення, вікно перевірки, публікація в канал, закріплений «Розклад», тижневий підсумок.

import { hasXKeys, postToX } from "./x.js";

const PORTAL = "https://prozorro.gov.ua/tender/";
const ACTIVE = ["in_review", "awaiting_edit"];
const LABEL = {
  queued: "у черзі на перевірку", in_review: "на перевірці", awaiting_edit: "редагується", approved: "схвалено",
  published: "опубліковано", rejected: "відхилено", suppressed: "приховано", expired: "прострочено", failed: "помилка",
};

// Безкоштовний тариф Cloudflare: не більше 50 зовнішніх запитів (fetch) на один виклик Worker.
// Кожен виклик отримує свій лічильник; коли він вичерпаний, необов'язкова робота переноситься на наступний раз.
const BUDGET = 45;
const withBudget = (env) => ({ ...env, budget: { left: BUDGET } });

export default {
  async fetch(request, rawEnv, ctx) {
    const env = withBudget(rawEnv);
    const url = new URL(request.url);
    try {
      if (url.pathname === "/tg/hook" && request.method === "POST") return await webhook(request, env, ctx);
      if (url.pathname.startsWith("/api/")) return await api(request, env, url);
      if (url.pathname === "/") return new Response("Прозорро Монітор працює");
      return new Response("not found", { status: 404 });
    } catch (e) {
      await logEvent(env, "error", { data: `${url.pathname}: ${e.stack || e}` });
      return json({ ok: false, error: String(e) }, 500);
    }
  },
  async scheduled(event, env, ctx) {
    ctx.waitUntil(tick(withBudget(env)));
  },
};

// ------------------------------------------------------------------ налаштування
const cfg = (env) => ({
  window: int(env.REVIEW_WINDOW, 8),             // скільки чернеток одночасно в чаті
  expireDays: int(env.EXPIRE_DAYS, 30),          // чернетка без рішення стільки днів - прострочена
  channelMinutes: int(env.CHANNEL_MINUTES, 60),
  quiet: [int(env.QUIET_FROM, 23), int(env.QUIET_TO, 8)],
  postToX: env.POST_TO_X !== "false",
  xApi: env.POST_TO_X !== "false" && hasXKeys(env),   // є ключі X: публікуємо самі; інакше - посилання вам
  xMinutes: int(env.X_MINUTES, 0),                // X через API: не частіше ніж раз на стільки хвилин (0 = без обмеження)
  xMaxPerDay: int(env.X_MAX_PER_DAY, 0),          // X через API: не більше стільки дописів за 24 год (0 = без обмеження)
  digestHour: int(env.DIGEST_HOUR, 18),
  hashtags: env.HASHTAGS || "#Prozorro",
  registry: env.REGISTRY_URL || "https://opendatabot.ua/c/",
  tgApi: env.TG_API || "https://api.telegram.org",
});
const int = (v, d) => (v === undefined || v === "" ? d : parseInt(v, 10));
const nowIso = () => new Date().toISOString();
const json = (o, status = 200) => new Response(JSON.stringify(o), { status, headers: { "content-type": "application/json" } });
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function hmacHex(key, msg) {
  const k = await crypto.subtle.importKey("raw", new TextEncoder().encode(key), { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  const sig = await crypto.subtle.sign("HMAC", k, new TextEncoder().encode(msg));
  return [...new Uint8Array(sig)].map((b) => b.toString(16).padStart(2, "0")).join("");
}
const apiKey = (env) => hmacHex(env.TG_BOT_TOKEN, "prozorro-api");
const hookSecret = (env) => hmacHex(env.TG_BOT_TOKEN, "prozorro-webhook");

// ------------------------------------------------------------------ час (Київ)
function kyiv(d = new Date()) {
  const p = Object.fromEntries(new Intl.DateTimeFormat("en-GB", {
    timeZone: "Europe/Kyiv", year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit",
    weekday: "short", hourCycle: "h23",
  }).formatToParts(d).map((x) => [x.type, x.value]));
  return { y: +p.year, mo: +p.month, d: +p.day, h: +p.hour, mi: +p.minute, wd: p.weekday, date: `${p.day}.${p.month}`, hm: `${p.hour}:${p.minute}` };
}
function isQuiet(env, d = new Date()) {
  const [a, b] = cfg(env).quiet, h = kyiv(d).h;
  return a > b ? h >= a || h < b : a <= h && h < b;
}
function nextOk(env, d) {
  let t = new Date(d);
  for (let i = 0; i < 300 && isQuiet(env, t); i++) t = new Date(t.getTime() + 10 * 60000);
  return t;
}
function when(d) {
  const k = kyiv(d), today = kyiv();
  return k.date === today.date ? k.hm : `${k.date} ${k.hm}`;
}
function isoWeek(d = new Date()) {
  const k = kyiv(d), t = new Date(Date.UTC(k.y, k.mo - 1, k.d));
  const day = t.getUTCDay() || 7;
  t.setUTCDate(t.getUTCDate() + 4 - day);
  const y0 = new Date(Date.UTC(t.getUTCFullYear(), 0, 1));
  return `${t.getUTCFullYear()}-W${String(Math.ceil(((t - y0) / 86400000 + 1) / 7)).padStart(2, "0")}`;
}

// ------------------------------------------------------------------ сховище
const one = (env, sql, ...a) => env.DB.prepare(sql).bind(...a).first();
const all = async (env, sql, ...a) => (await env.DB.prepare(sql).bind(...a).all()).results;
const run = (env, sql, ...a) => env.DB.prepare(sql).bind(...a).run();
const getPost = (env, id) => one(env, "SELECT * FROM posts WHERE id=?", id);
async function meta(env, k, v) {
  if (v === undefined) return (await one(env, "SELECT v FROM meta WHERE k=?", k))?.v ?? null;
  await run(env, "INSERT OR REPLACE INTO meta (k, v) VALUES (?, ?)", k, String(v));
}
async function logEvent(env, kind, { update_id = null, post_id = null, user_id = null, data = null } = {}) {
  try {
    const r = await run(env, "INSERT OR IGNORE INTO events (at, update_id, kind, post_id, user_id, data) VALUES (?,?,?,?,?,?)",
      nowIso(), update_id, kind, post_id, user_id, typeof data === "string" ? data.slice(0, 4000) : JSON.stringify(data));
    return r.meta.changes > 0;
  } catch (e) {
    console.log("logEvent", e);
    return true;
  }
}

// ------------------------------------------------------------------ Telegram
class BudgetError extends Error {}

async function tg(env, method, params) {
  for (let attempt = 0; attempt < 2; attempt++) {
    if (env.budget && env.budget.left-- <= 0) throw new BudgetError("бюджет запитів вичерпано");
    const r = await fetch(`${cfg(env).tgApi}/bot${env.TG_BOT_TOKEN}/${method}`, {
      method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(params),
    });
    const j = await r.json();
    if (j.ok) return j.result;
    const wait = j.parameters?.retry_after;
    if (j.error_code === 429 && wait && wait <= 5 && attempt === 0) { await sleep(wait * 1000 + 300); continue; }
    const err = new Error(`telegram ${method}: ${j.error_code} ${j.description}`);
    err.code = j.error_code;
    err.description = j.description || "";
    throw err;
  }
}
const quietly = (p) => p.catch(() => null);
const admins = (env) => new Set(String(env.TG_ADMIN_IDS || "").split(",").map((s) => s.trim()).filter(Boolean));
const reviewChat = (env) => env.TG_REVIEW_CHAT_ID;

const esc = (s) => String(s ?? "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
function tgHtml(env, text) {
  // текст допису для Telegram: коди ЄДРПОУ стають посиланнями на реєстр
  return esc(text).split("\n").map((line) => !line.startsWith("ЄДРПОУ:") ? line
    : line.replace(/(замовник|переможець) (\d{8})\b/g, (_, w, c) => `${w} <a href="${cfg(env).registry}${c}">${c}</a>`)).join("\n");
}
function weightedLen(text) {
  // X рахує кожне посилання як 23 символи; кирилиця/латиниця - 1, емодзі й CJK - 2
  const t = String(text).replace(/https?:\/\/\S+/g, "x".repeat(23));
  let n = 0;
  for (const ch of t) {
    const c = ch.codePointAt(0);
    n += c <= 0x10ff || (c >= 0x2000 && c <= 0x200d) || (c >= 0x2010 && c <= 0x201f) || (c >= 0x2032 && c <= 0x2037) ? 1 : 2;
  }
  return n;
}
const xIntent = (text) => "https://x.com/intent/post?text=" + encodeURIComponent(text);
const buttons = (id) => ({ inline_keyboard: [[
  { text: "✅ Опублікувати", callback_data: `ok:${id}` },
  { text: "✏️ Редагувати", callback_data: `ed:${id}` },
  { text: "❌ Відхилити", callback_data: `no:${id}` },
], [{ text: "⏭ Пізніше", callback_data: `sk:${id}` }]] });

function reviewHtml(env, p, status) {
  const c = cfg(env);
  let text = p.text_x || "", full = p.text_tg;
  if (!c.postToX && full) { text = full; full = null; }
  const mine = new Set(text.split("\n"));
  const extra = (full || "").split("\n").filter((x) => x && !mine.has(x));
  const title = c.postToX ? `Чернетка для X</b> (${weightedLen(text)}/280)` : "Чернетка для Telegram-каналу</b>";
  const chance = p.model_p === null || p.model_p === undefined ? "" : `🤖 Шанс схвалення (модель): ${Math.round(p.model_p * 100)}%\n`;
  const head = `<b>${p.kind === "explore" ? "🧪 " : ""}${title}\n${chance}<blockquote>${tgHtml(env, text)}</blockquote>\n`
    + (extra.length ? `<b>Лише в Telegram-каналі:</b>\n${extra.map((x) => tgHtml(env, x)).join("\n")}\n` : "");
  const tail = (p.id.startsWith("PAT-") ? "" : `\n${PORTAL}${p.id}`) + (status ? `\n\n<b>${esc(status)}</b>` : "");
  const room = 4000 - head.length - tail.length - 30;
  let ctx = p.context || "";
  if (ctx.length > room) ctx = ctx.slice(0, Math.max(0, room)).replace(/\s+\S*$/, "") + " (скорочено)";
  return `${head}<b>Для перевірки:</b>\n${esc(ctx)}${tail}`;
}

async function channelEta(env, id) {
  // орієнтовний час публікації в канал для схваленого допису
  if (!env.TG_CHANNEL_ID) return null;
  const q = await all(env, "SELECT id FROM posts WHERE status='approved' AND channel_msg_id IS NULL ORDER BY decided_at, id");
  const i = q.findIndex((r) => r.id === id);
  if (i < 0) return null;
  return channelTimesFrom(env, await meta(env, "last_channel_post"), i + 1)[i];
}
function channelTimesFrom(env, last, n) {
  const c = cfg(env);
  let t = new Date();
  if (last) t = new Date(Math.max(t, new Date(last).getTime() + (c.channelMinutes - 5) * 60000));
  const out = [];
  for (let i = 0; i < n; i++) { t = nextOk(env, t); out.push(t); t = new Date(t.getTime() + c.channelMinutes * 60000); }
  return out;
}

async function approvedHtml(env, p, extraStatus) {
  const c = cfg(env);
  const eta = await channelEta(env, p.id);
  const lines = [];
  if (c.xApi) {
    lines.push("<b>✅ Схвалено</b>", `<blockquote>${tgHtml(env, p.text_x)}</blockquote>`,
      p.x_id ? `🐦 Опубліковано в X: https://x.com/i/web/status/${p.x_id}` : "🐦 У X - під час найближчого проходу (до 10 хв, поза тихими годинами)");
  } else if (c.postToX) {
    lines.push("<b>✅ Схвалено. Допис для X</b> #ready_to_x",
      "Натисніть «Відкрити в X», перевірте текст і натисніть Post, а потім «✔️ Опубліковано в X» - повідомлення зникне.",
      `<pre>${esc(p.text_x)}</pre>`);
  } else {
    lines.push("<b>✅ Схвалено</b>", `<blockquote>${tgHtml(env, p.text_tg || p.text_x)}</blockquote>`);
  }
  if (p.channel_msg_id > 0) lines.push("📣 Опубліковано в Telegram-каналі");
  else if (eta) lines.push(`📣 У Telegram-канал ≈ ${when(eta)}`);
  if (!p.id.startsWith("PAT-")) lines.push(PORTAL + p.id);
  if (extraStatus) lines.push(`<b>${esc(extraStatus)}</b>`);
  return lines.join("\n");
}
async function showApproved(env, p, extraStatus) {
  if (!p.review_msg_id) return;
  const c = cfg(env);
  const text = await approvedHtml(env, p, extraStatus);
  const manualX = c.postToX && !c.xApi;
  const done = [{ text: "✔️ Опубліковано в X", callback_data: `px:${p.id}` }];   // прибрати повідомлення, коли запощено
  const kb = manualX ? { inline_keyboard: [[{ text: "Відкрити в X", url: xIntent(p.text_x) }], done] } : { inline_keyboard: [] };
  try {
    await tg(env, "editMessageText", { chat_id: reviewChat(env), message_id: p.review_msg_id, text, parse_mode: "HTML",
      disable_web_page_preview: true, reply_markup: kb });
  } catch (e) {
    if (/not modified/.test(e.description)) return;
    if (manualX) {   // напр. задовге посилання для кнопки: даємо його текстом
      await quietly(tg(env, "editMessageText", { chat_id: reviewChat(env), message_id: p.review_msg_id, parse_mode: "HTML",
        disable_web_page_preview: true, text: text + `\n<a href="${esc(xIntent(p.text_x))}">Відкрити в X</a>`, reply_markup: { inline_keyboard: [done] } }));
    }
  }
}

async function dropMessage(env, mid, fallbackHtml) {
  // видаляємо повідомлення; якщо Telegram не дає (старше 48 год) - прибираємо кнопки й позначаємо
  if (!mid) return;
  try {
    await tg(env, "deleteMessage", { chat_id: reviewChat(env), message_id: mid });
  } catch (e) {
    if (fallbackHtml) {
      await quietly(tg(env, "editMessageText", { chat_id: reviewChat(env), message_id: mid, text: fallbackHtml, parse_mode: "HTML",
        disable_web_page_preview: true, reply_markup: { inline_keyboard: [] } }));
    } else {
      await quietly(tg(env, "editMessageReplyMarkup", { chat_id: reviewChat(env), message_id: mid, reply_markup: { inline_keyboard: [] } }));
    }
  }
}

// ------------------------------------------------------------------ вебхук
async function webhook(request, env, ctx) {
  if (request.headers.get("X-Telegram-Bot-Api-Secret-Token") !== await hookSecret(env)) return new Response("forbidden", { status: 403 });
  const u = await request.json();
  const fresh = await logEvent(env, u.callback_query ? "click" : "message", {
    update_id: u.update_id,
    user_id: String((u.callback_query || u.message || {}).from?.id ?? ""),
    post_id: u.callback_query?.data?.split(":").slice(1).join(":") || null,
    data: u.callback_query ? { data: u.callback_query.data, msg: u.callback_query.message?.message_id }
      : { text: u.message?.text?.slice(0, 500), reply_to: u.message?.reply_to_message?.message_id },
  });
  if (!fresh) return json({ ok: true, duplicate: true });     // Telegram повторив те саме оновлення
  try {
    if (u.callback_query) await onClick(env, u.callback_query);
    else if (u.message) await onMessage(env, u.message);
  } catch (e) {
    await logEvent(env, "error", { data: `update ${u.update_id}: ${e.stack || e}` });
  }
  ctx.waitUntil(fillWindow(env).catch((e) => logEvent(env, "error", { data: `fillWindow: ${e.stack || e}` })));
  return json({ ok: true });
}

async function answer(env, q, text) {
  await quietly(tg(env, "answerCallbackQuery", { callback_query_id: q.id, text }));
}

async function onClick(env, q) {
  const uid = String(q.from.id);
  if (!admins(env).has(uid)) return answer(env, q, "Немає прав");
  const data = q.data || "";
  const i = data.indexOf(":");
  const action = data.slice(0, i), id = data.slice(i + 1);
  const mid = q.message?.message_id;
  let p = await getPost(env, id);
  if (!p) {
    await answer(env, q, "Цієї чернетки немає в базі");
    return dropMessage(env, mid);
  }
  if (action === "px") {
    // «Опубліковано в X»: допис уже в X, повідомлення #ready_to_x більше не потрібне
    await run(env, "UPDATE posts SET x_posted_at=COALESCE(x_posted_at, ?), review_msg_id=NULL WHERE id=?", nowIso(), id);
    await answer(env, q, "✔️ Позначено як опубліковане в X");
    return dropMessage(env, mid, "<b>✔️ Опубліковано в X</b>");
  }
  if (!["queued", ...ACTIVE].includes(p.status)) {
    await answer(env, q, `Вже оброблено: ${LABEL[p.status] || p.status}`);
    if (mid && mid !== p.review_msg_id) await dropMessage(env, mid, `<b>Вже оброблено: ${esc(LABEL[p.status] || p.status)}</b>`);
    return;
  }
  // натиснули на старій копії (напр. ще з часів до переходу на вебхук): працюємо з нею, поточну копію прибираємо
  const other = p.review_msg_id && mid && p.review_msg_id !== mid ? p.review_msg_id : null;
  const t = nowIso();
  if (action === "ok") {
    // з каналом або з X через API: чекає своєї черги; інакше посилання для X - це і є публікація
    const c = cfg(env), manualX = c.postToX && !c.xApi;
    await run(env, "UPDATE posts SET status=?, decided_at=?, decided_by=?, review_msg_id=?, x_sent_at=? WHERE id=?",
      env.TG_CHANNEL_ID || c.xApi ? "approved" : "published", t, uid, mid ?? p.review_msg_id, manualX ? t : null, id);
    await answer(env, q, "✅ Схвалено");
    await showApproved(env, await getPost(env, id));
  } else if (action === "no") {
    await run(env, "UPDATE posts SET status='rejected', decided_at=?, decided_by=?, review_msg_id=NULL WHERE id=?", t, uid, id);
    await answer(env, q, "❌ Відхилено");
    await dropMessage(env, mid, "<b>❌ Відхилено</b>");
  } else if (action === "sk") {
    await run(env, "UPDATE posts SET status='queued', skips=skips+1, review_msg_id=NULL, review_sent_at=NULL WHERE id=?", id);
    await answer(env, q, "⏭ Відкладено, покажу пізніше");
    await dropMessage(env, mid, "<b>⏭ Відкладено</b>");
  } else if (action === "ed") {
    const res = await tg(env, "sendMessage", { chat_id: reviewChat(env), reply_markup: { force_reply: true },
      text: `Надішліть новий текст для ${id} відповіддю (reply) на це повідомлення`
        + (cfg(env).postToX ? " (до 280 символів)." : ".") + " Посилання на тендер додайте самі або напишіть +link наприкінці." });
    await run(env, "UPDATE posts SET status='awaiting_edit', edit_prompt_id=?, review_msg_id=? WHERE id=?",
      res.message_id, mid ?? p.review_msg_id, id);
    await answer(env, q, "✏️ Чекаю новий текст");
    return other && dropMessage(env, other);
  } else {
    return answer(env, q, "Невідома дія");
  }
  if (other) await dropMessage(env, other);
}

async function onMessage(env, msg) {
  const uid = String(msg.from?.id ?? "");
  if (!admins(env).has(uid)) return;
  let text = (msg.text || "").trim();
  const replyTo = msg.reply_to_message?.message_id;
  if (replyTo) {
    const p = await one(env, "SELECT * FROM posts WHERE status='awaiting_edit' AND edit_prompt_id=?", replyTo);
    if (!p) return;
    if (text.endsWith("+link")) text = text.slice(0, -5).trimEnd() + (p.id.startsWith("PAT-") ? "" : `\n${PORTAL}${p.id}`);
    if (cfg(env).postToX && weightedLen(text) > 280) {
      return tg(env, "sendMessage", { chat_id: reviewChat(env), reply_to_message_id: replyTo,
        text: `Задовго: ${weightedLen(text)}/280. Надішліть коротший текст відповіддю на те саме прохання.` });
    }
    await run(env, "UPDATE posts SET text_x=?, text_tg=NULL, edited=1, status='in_review', edit_prompt_id=NULL WHERE id=?", text, p.id);
    const q = await getPost(env, p.id);
    const html = reviewHtml(env, q, "✏️ Текст замінено");
    try {
      await tg(env, "editMessageText", { chat_id: reviewChat(env), message_id: q.review_msg_id, text: html, parse_mode: "HTML",
        disable_web_page_preview: true, reply_markup: buttons(q.id) });
    } catch (e) {
      const res = await tg(env, "sendMessage", { chat_id: reviewChat(env), text: html, parse_mode: "HTML",
        disable_web_page_preview: true, reply_markup: buttons(q.id) });
      await quietly(tg(env, "deleteMessage", { chat_id: reviewChat(env), message_id: q.review_msg_id }));
      await run(env, "UPDATE posts SET review_msg_id=?, review_sent_at=? WHERE id=?", res.message_id, nowIso(), q.id);
    }
    await quietly(tg(env, "deleteMessage", { chat_id: reviewChat(env), message_id: replyTo }));
    await quietly(tg(env, "deleteMessage", { chat_id: reviewChat(env), message_id: msg.message_id }));
    return;
  }
  const cmd = text.split(/[\s@]/)[0];
  if (cmd === "/pause" || cmd === "/resume") {
    await meta(env, "paused", cmd === "/pause" ? "1" : "0");
    return say(env, cmd === "/pause" ? "⏸ Публікацію в канал призупинено (/resume - відновити)" : "▶️ Публікацію відновлено");
  }
  if (cmd === "/queue" || cmd === "/stats") {
    await verifyShown(env);
    await fillWindow(env);
    return say(env, await statsText(env));
  }
  if (cmd === "/next") {
    await verifyShown(env);
    // наступна порція одразу, навіть якщо в чаті вже повне вікно
    const sent = await fillWindow(env, cfg(env).window);
    const left = (await one(env, "SELECT COUNT(*) AS n FROM posts WHERE status='queued'")).n;
    return say(env, sent ? `Надіслано ще ${sent}. У черзі лишилось ${left}.` : `Черга порожня: нових чернеток немає.`);
  }
  if (cmd === "/model") return say(env, (await meta(env, "model_report")) || "Звіту моделі ще немає.");
  if (cmd === "/start" || cmd === "/help") {
    return say(env, "Під кожною чернеткою: ✅ опублікувати, ✏️ редагувати, ❌ відхилити, ⏭ пізніше.\n"
      + `У чаті одночасно до ${cfg(env).window} чернеток; щойно ви вирішите одну, приходить наступна.\n`
      + "/queue - стан черги, /next - ще порція чернеток, /model - як справи в моделі, /pause і /resume - публікація в канал.");
  }
}
const say = (env, text) => tg(env, "sendMessage", { chat_id: reviewChat(env), text, disable_web_page_preview: true });

async function statsText(env) {
  const cnt = Object.fromEntries((await all(env, "SELECT status, COUNT(*) AS n FROM posts GROUP BY status")).map((r) => [r.status, r.n]));
  const chan = (await one(env, "SELECT COUNT(*) AS n FROM posts WHERE status='approved' AND channel_msg_id IS NULL")).n;
  const lines = [
    `У чаті на перевірці: ${(cnt.in_review || 0) + (cnt.awaiting_edit || 0)}; чекають у черзі: ${cnt.queued || 0}`,
    `Схвалено, чекають на канал: ${chan}`,
    `Опубліковано: ${cnt.published || 0}; відхилено: ${cnt.rejected || 0}; приховано: ${cnt.suppressed || 0}; прострочено: ${cnt.expired || 0}`,
  ];
  if ((await meta(env, "paused")) === "1") lines.push("⏸ Публікацію в канал призупинено.");
  if (isQuiet(env)) lines.push("Зараз тихі години: канал - після їх завершення.");
  return lines.join("\n");
}

// ------------------------------------------------------------------ вікно перевірки
async function verifyShown(env) {
  // Telegram не повідомляє ботові, коли ви самі видаляєте повідомлення (напр. «Очистити історію»).
  // Перевіряємо, чи чернетки «в чаті» справді там: повторно ставимо ті самі кнопки - на видаленому
  // повідомленні Telegram відповідає «not found». Зниклі повертаються в чергу й надсилаються знову.
  const rows = await all(env, "SELECT id, review_msg_id FROM posts WHERE status='in_review' AND review_msg_id IS NOT NULL");
  let gone = 0;
  for (const p of rows) {
    if (env.budget.left < 12) break;
    try {
      await tg(env, "editMessageReplyMarkup", { chat_id: reviewChat(env), message_id: p.review_msg_id, reply_markup: buttons(p.id) });
    } catch (e) {
      if (e instanceof BudgetError) break;
      if (/not found|MESSAGE_ID_INVALID|can't be edited/i.test(e.description || "")) {
        await run(env, "UPDATE posts SET status='queued', review_msg_id=NULL, review_sent_at=NULL WHERE id=? AND status='in_review'", p.id);
        gone++;
      }
    }
  }
  await meta(env, "last_verify", nowIso());
  if (gone) await logEvent(env, "info", { data: `зниклих із чату чернеток повернуто в чергу: ${gone}` });
  return gone;
}

async function verifyIfDue(env) {
  const last = await meta(env, "last_verify");
  if (!last || new Date(last).getTime() < Date.now() - 30 * 60000) await verifyShown(env);
}

async function fillWindow(env, extra = 0) {
  // extra: стільки чернеток понад вікно (команда /next)
  const c = cfg(env);
  const shown = (await one(env, `SELECT COUNT(*) AS n FROM posts WHERE status IN ('in_review','awaiting_edit')`)).n;
  let need = Math.max(c.window - shown, 0) + extra;
  if (need <= 0) return 0;
  const rows = await all(env, `SELECT * FROM posts WHERE status='queued' ORDER BY skips, priority DESC, created_at DESC, id LIMIT ${need | 0}`);
  let sent = 0;
  for (const p of rows) {
    if (env.budget && env.budget.left < 4) break;
    // «бронюємо» чернетку, щоб два одночасні виклики не надіслали її двічі
    const claim = await run(env, "UPDATE posts SET status='in_review', review_sent_at=? WHERE id=? AND status='queued'", nowIso(), p.id);
    if (!claim.meta.changes) continue;
    try {
      const res = await tg(env, "sendMessage", { chat_id: reviewChat(env), text: reviewHtml(env, p), parse_mode: "HTML",
        disable_web_page_preview: true, reply_markup: buttons(p.id), disable_notification: shown + sent > 0 });
      await run(env, "UPDATE posts SET review_msg_id=? WHERE id=?", res.message_id, p.id);
      if (p.review_msg_id) await dropMessage(env, p.review_msg_id);   // стара копія (напр. перенесена зі старого бота)
      sent++;
    } catch (e) {
      await run(env, "UPDATE posts SET status='queued', review_sent_at=NULL WHERE id=?", p.id);
      if (!(e instanceof BudgetError)) await logEvent(env, "error", { post_id: p.id, data: `надсилання чернетки: ${e.message}` });
      break;
    }
  }
  return sent;
}

// ------------------------------------------------------------------ розклад (cron)
async function tick(env) {
  const steps = [publishX, publishChannel, verifyIfDue, fillWindow, updateSchedule, weeklyDigest, refreshOld, expire, addPostedButtons, pruneEvents];
  for (const f of steps) {
    try { await f(env); } catch (e) {
      if (!(e instanceof BudgetError)) await logEvent(env, "error", { data: `${f.name}: ${e.stack || e}` });
    }
  }
  await meta(env, "last_tick", nowIso());
}

async function addPostedButtons(env) {
  // одноразово: кнопка «✔️ Опубліковано в X» на повідомленнях #ready_to_x, схвалених до її появи (курсор - за id)
  const c = cfg(env);
  if (!c.postToX || c.xApi || (await meta(env, "px_buttons")) === "done") return;
  const after = (await meta(env, "px_buttons")) || "";
  const rows = await all(env, `SELECT * FROM posts WHERE status IN ('approved','published') AND review_msg_id IS NOT NULL
    AND x_posted_at IS NULL AND x_sent_at IS NOT NULL AND id > ? ORDER BY id LIMIT 8`, after);
  for (const p of rows) {
    if (env.budget.left < 6) return;
    await showApproved(env, p);
    await meta(env, "px_buttons", p.id);
  }
  if (rows.length < 8) await meta(env, "px_buttons", "done");
}

async function expire(env) {
  const cut = new Date(Date.now() - cfg(env).expireDays * 86400000).toISOString();
  const old = await all(env, "SELECT id, review_msg_id FROM posts WHERE status IN ('queued','in_review') AND created_at < ? LIMIT 200", cut);
  for (const p of old) {
    await run(env, "UPDATE posts SET status='expired', review_msg_id=NULL WHERE id=?", p.id);
    if (env.budget.left > 4) await dropMessage(env, p.review_msg_id, "<b>⌛ Прострочено</b>");
  }
}

async function refreshOld(env) {
  // Telegram дозволяє видаляти повідомлення лише 48 год: чернетку, що висить ~44 год, надсилаємо заново
  const cut = new Date(Date.now() - 44 * 3600000).toISOString();
  const rows = await all(env, "SELECT * FROM posts WHERE status='in_review' AND review_sent_at < ? LIMIT 6", cut);
  for (const p of rows) {
    const res = await tg(env, "sendMessage", { chat_id: reviewChat(env), text: reviewHtml(env, p), parse_mode: "HTML",
      disable_web_page_preview: true, reply_markup: buttons(p.id), disable_notification: true });
    await quietly(tg(env, "deleteMessage", { chat_id: reviewChat(env), message_id: p.review_msg_id }));
    await run(env, "UPDATE posts SET review_msg_id=?, review_sent_at=? WHERE id=?", res.message_id, nowIso(), p.id);
  }
}

async function publishX(env) {
  // X через API: схвалені, ще не опубліковані в X; поза тихими годинами, з інтервалом і добовим лімітом
  const c = cfg(env);
  if (!c.xApi || (await meta(env, "paused")) === "1" || isQuiet(env)) return;
  const last = await meta(env, "last_x_post");
  if (c.xMinutes && last && new Date(last).getTime() > Date.now() - (c.xMinutes - 2) * 60000) return;
  let room = c.xMinutes ? 1 : 5;
  if (c.xMaxPerDay) {
    const day = (await one(env, "SELECT COUNT(*) AS n FROM posts WHERE x_id IS NOT NULL AND x_sent_at >= ?",
      new Date(Date.now() - 86400000).toISOString())).n;
    room = Math.min(room, c.xMaxPerDay - day);
  }
  if (room <= 0) return;
  const rows = await all(env, `SELECT * FROM posts WHERE status IN ('approved','published') AND x_sent_at IS NULL
    AND decided_at IS NOT NULL ORDER BY decided_at, id LIMIT ${room | 0}`);
  for (const p of rows) {
    let xid;
    try {
      xid = await postToX(env, p.text_x);
    } catch (e) {
      await logEvent(env, "error", { post_id: p.id, data: `X: ${e.message}` });
      await run(env, "UPDATE posts SET status='failed', note=? WHERE id=?", `X: ${e.message}`.slice(0, 500), p.id);
      return showApproved(env, { ...p, status: "failed" }, `⚠️ Помилка публікації в X: ${e.message.slice(0, 200)}`);
    }
    const t = nowIso();
    await run(env, "UPDATE posts SET x_id=?, x_sent_at=?, status=CASE WHEN ? THEN status ELSE 'published' END WHERE id=?",
      xid, t, env.TG_CHANNEL_ID ? 1 : 0, p.id);
    await meta(env, "last_x_post", t);
    await showApproved(env, await getPost(env, p.id));
  }
}

async function publishChannel(env) {
  const c = cfg(env);
  if (!env.TG_CHANNEL_ID || (await meta(env, "paused")) === "1" || isQuiet(env)) return;
  const last = await meta(env, "last_channel_post");
  if (last && new Date(last).getTime() > Date.now() - (c.channelMinutes - 5) * 60000) return;
  const p = await one(env, "SELECT * FROM posts WHERE status='approved' AND channel_msg_id IS NULL ORDER BY decided_at, id LIMIT 1");
  if (!p) return;
  let mid;
  try {
    mid = (await tg(env, "sendMessage", { chat_id: env.TG_CHANNEL_ID, text: tgHtml(env, p.text_tg || p.text_x), parse_mode: "HTML" })).message_id;
  } catch (e) {
    await logEvent(env, "error", { post_id: p.id, data: `канал: ${e.message}` });
    await run(env, "UPDATE posts SET status='failed', note=? WHERE id=?", `канал: ${e.message}`.slice(0, 500), p.id);
    return showApproved(env, { ...p, status: "failed" }, `⚠️ Помилка публікації в канал: ${e.description}`);
  }
  const t = nowIso();
  await run(env, "UPDATE posts SET status='published', channel_msg_id=?, channel_at=? WHERE id=?", mid, t, p.id);
  await meta(env, "last_channel_post", t);
  await showApproved(env, await getPost(env, p.id));
}

async function scheduleHtml(env) {
  const c = cfg(env);
  const [a, b] = c.quiet;
  const k = kyiv();
  const lines = [`📅 <b>Розклад публікацій</b> (оновлено ${k.date} ${k.hm} за Києвом)`,
    `Тихі години ${String(a).padStart(2, "0")}:00–${String(b).padStart(2, "0")}:00.`,
    `Хештеги: #ready_to_x (готові до публікації в X) · ${esc(c.hashtags)} (у кожному дописі)`];
  if ((await meta(env, "paused")) === "1") lines.push("⏸ Публікацію в канал призупинено (/resume, щоб відновити).");
  const cnt = await one(env, "SELECT SUM(status='queued') AS q, SUM(status IN ('in_review','awaiting_edit')) AS r FROM posts");
  lines.push("", `<b>Перевірка:</b> у чаті ${cnt.r || 0}, у черзі ${cnt.q || 0}`);
  if (c.xApi) {
    const nx = (await one(env, "SELECT COUNT(*) AS n FROM posts WHERE status IN ('approved','published') AND x_sent_at IS NULL AND decided_at IS NOT NULL")).n;
    lines.push(`<b>X (через API):</b> у черзі ${nx}` + (c.xMinutes ? `, раз на ~${c.xMinutes} хв` : "")
      + (c.xMaxPerDay ? `, до ${c.xMaxPerDay} за добу` : ""));
  }
  if (env.TG_CHANNEL_ID) {
    const q = await all(env, "SELECT id, COALESCE(text_tg, text_x) AS text FROM posts WHERE status='approved' AND channel_msg_id IS NULL ORDER BY decided_at, id");
    const times = channelTimesFrom(env, await meta(env, "last_channel_post"), Math.min(q.length, 25));
    lines.push("", `<b>Telegram-канал</b> (по одному, раз на ~${c.channelMinutes} хв): ${q.length} шт.`);
    q.slice(0, 25).forEach((r, i) => {
      const head = esc((r.text || "").split("\n")[0].slice(0, 45));
      const link = r.id.startsWith("PAT-") ? esc(r.id) : `<a href="${PORTAL}${r.id}">${esc(r.id)}</a>`;
      lines.push(`≈ ${when(times[i])} · ${link} · ${head}`);
    });
    if (q.length > 25) lines.push(`... і ще ${q.length - 25}`);
  }
  return lines.join("\n");
}

async function updateSchedule(env) {
  const text = await scheduleHtml(env);
  const body = text.replace(/\(оновлено [^)]*\)/, "");
  if (body === (await meta(env, "schedule_body"))) return;
  const mid = await meta(env, "schedule_msg_id");
  if (mid) {
    try {
      await tg(env, "editMessageText", { chat_id: reviewChat(env), message_id: +mid, text, parse_mode: "HTML", disable_web_page_preview: true });
      return meta(env, "schedule_body", body);
    } catch (e) {
      if (/not modified/.test(e.description)) return meta(env, "schedule_body", body);
      await quietly(tg(env, "unpinChatMessage", { chat_id: reviewChat(env), message_id: +mid }));
    }
  }
  const res = await tg(env, "sendMessage", { chat_id: reviewChat(env), text, parse_mode: "HTML", disable_web_page_preview: true,
    disable_notification: true });
  await meta(env, "schedule_msg_id", res.message_id);
  await meta(env, "schedule_body", body);
  await quietly(tg(env, "pinChatMessage", { chat_id: reviewChat(env), message_id: res.message_id, disable_notification: true }));
}

async function weeklyDigest(env) {
  const k = kyiv(), week = isoWeek();
  if (k.wd !== "Sun" || k.h < cfg(env).digestHour || (await meta(env, "last_digest")) === week) return;
  const since = new Date(Date.now() - 7 * 86400000).toISOString();
  const s = await one(env, `SELECT COUNT(*) AS n, SUM(status IN ('approved','published')) AS ok, SUM(status='rejected') AS no,
    SUM(status IN ('queued','in_review','awaiting_edit')) AS wait FROM posts WHERE created_at >= ?`, since);
  const pub = await all(env, "SELECT id, COALESCE(text_tg, text_x) AS text FROM posts WHERE channel_at >= ? ORDER BY channel_at", since);
  const decided = (s.ok || 0) + (s.no || 0);
  const lines = [`📊 Тиждень ${week}`,
    `Нових чернеток: ${s.n}; схвалено ${s.ok || 0}, відхилено ${s.no || 0}, без рішення ${s.wait || 0}`
      + (decided ? ` (${Math.floor((s.ok || 0) * 100 / decided)}% схвалень)` : ""),
    `Опубліковано в каналі: ${pub.length}`];
  const model = await meta(env, "model_summary");
  if (model) lines.push(model);
  await say(env, lines.join("\n"));
  if (env.TG_CHANNEL_ID && pub.length) {
    const out = [`Підсумки тижня: сигналів ризику в закупівлях — ${pub.length}`, ""];
    for (const p of pub.slice(0, 5)) {
      out.push(`• ${(p.text || "").split("\n")[0]}`);
      if (!p.id.startsWith("PAT-")) out.push(`  ${PORTAL}${p.id}`);
    }
    out.push("", "Сигнал ризику, не доказ порушення.");
    await tg(env, "sendMessage", { chat_id: env.TG_CHANNEL_ID, text: out.join("\n"), disable_web_page_preview: true });
  }
  await meta(env, "last_digest", week);
}

async function pruneEvents(env) {
  await run(env, "DELETE FROM events WHERE at < ?", new Date(Date.now() - 60 * 86400000).toISOString());
}

// ------------------------------------------------------------------ API для GitHub Actions
async function api(request, env, url) {
  if (request.headers.get("Authorization") !== `Bearer ${await apiKey(env)}`) return json({ ok: false, error: "unauthorized" }, 401);
  const path = url.pathname.slice(5);
  const body = request.method === "POST" ? await request.json() : null;

  if (path === "health") {
    return json({ ok: true, last_tick: await meta(env, "last_tick"), webhook: await quietly(tg(env, "getWebhookInfo", {})) });
  }
  if (path === "setup") {
    // вебхук на цей Worker + команди бота
    const hook = `${url.origin}/tg/hook`;
    await tg(env, "setWebhook", { url: hook, secret_token: await hookSecret(env), allowed_updates: ["message", "callback_query"],
      max_connections: 5 });
    await quietly(tg(env, "setMyCommands", { commands: [
      { command: "queue", description: "стан черги" }, { command: "next", description: "ще порція чернеток" },
      { command: "model", description: "якість моделі" }, { command: "pause", description: "призупинити канал" }, { command: "resume", description: "відновити канал" },
      { command: "help", description: "довідка" }] }));
    await logEvent(env, "info", { data: `вебхук: ${hook}` });
    return json({ ok: true, webhook: await tg(env, "getWebhookInfo", {}) });
  }
  if (path === "export") {
    // сторінками (ліміт CPU безкоштовного тарифу); контекст - лише з full=1
    const offset = int(url.searchParams.get("offset"), 0), limit = Math.min(int(url.searchParams.get("limit"), 200), 500);
    const cols = url.searchParams.get("full") === "1" ? "*"
      : "id, kind, status, text_x, text_tg, features, priority, skips, edited, created_at, review_msg_id, review_sent_at, "
        + "decided_at, decided_by, x_sent_at, x_id, x_posted_at, channel_msg_id, channel_at, note, model_p, money";
    const posts = await all(env, `SELECT ${cols} FROM posts ORDER BY id LIMIT ${limit | 0} OFFSET ${Math.max(0, offset | 0)}`);
    return json({ ok: true, posts, more: posts.length === limit,
      meta: offset ? undefined : Object.fromEntries((await all(env, "SELECT k, v FROM meta")).map((r) => [r.k, r.v])) });
  }
  if (path === "events") {
    const since = url.searchParams.get("since") || new Date(Date.now() - 86400000).toISOString();
    return json({ ok: true, events: await all(env, "SELECT * FROM events WHERE at >= ? ORDER BY id LIMIT 2000", since) });
  }
  if (path === "drafts") {
    // нові чернетки; вже відомі id ігноруються
    const stmt = env.DB.prepare(`INSERT OR IGNORE INTO posts (id, kind, status, text_x, text_tg, context, features, priority, created_at, model_p, money)
      VALUES (?, ?, 'queued', ?, ?, ?, ?, ?, ?, ?, ?)`);
    const t = nowIso();
    const res = (body.drafts || []).length ? await env.DB.batch(body.drafts.map((d) => stmt.bind(d.id, d.kind || "tender",
      d.text_x, d.text_tg ?? null, d.context ?? null, d.features ? JSON.stringify(d.features) : null, d.priority ?? 0, t,
      d.model_p ?? null, d.money ?? null))) : [];
    const added = res.reduce((n, r) => n + r.meta.changes, 0);
    const sent = await fillWindow(env);
    return json({ ok: true, added, sent });
  }
  if (path === "suppress") {
    // прибрати з перевірки/черги (військові, зниклі сигнали); вже опубліковане в канал не чіпаємо
    let n = 0;
    for (const it of body.items || []) {
      const p = await getPost(env, it.id);
      if (!p) continue;
      if (["queued", ...ACTIVE].includes(p.status)) {
        await run(env, "UPDATE posts SET status='suppressed', note=?, review_msg_id=NULL WHERE id=?", it.reason ?? null, p.id);
        if (env.budget.left > 10) await dropMessage(env, p.review_msg_id, `<b>🚫 Прибрано: ${esc(it.reason || "")}</b>`);
        n++;
      } else if (p.status === "approved" && !p.channel_msg_id && it.channel) {
        await run(env, "UPDATE posts SET status='published', channel_msg_id=-1, note=? WHERE id=?", it.reason ?? null, p.id);
        n++;
      }
    }
    await fillWindow(env);
    return json({ ok: true, suppressed: n });
  }
  if (path === "texts") {
    // свіжий повний текст для схвалених, які ще чекають на канал (дані тендера могли змінитися); правлене вручну не чіпаємо
    let n = 0;
    for (const it of body.items || []) {
      const r = await run(env, "UPDATE posts SET text_tg=? WHERE id=? AND status='approved' AND channel_msg_id IS NULL AND edited=0",
        it.text_tg, it.id);
      n += r.meta.changes;
    }
    return json({ ok: true, updated: n });
  }
  if (path === "meta") {
    for (const [k, v] of Object.entries(body || {})) if (["model_summary", "model_report"].includes(k)) await meta(env, k, v);
    return json({ ok: true });
  }
  if (path === "scores") {
    // свіжі прогнози моделі й порядок для чернеток, що ще чекають
    const stmt = env.DB.prepare(`UPDATE posts SET priority=?, model_p=?, money=COALESCE(?, money)
      WHERE id=? AND status IN ('queued','in_review','awaiting_edit')`);
    const items = body.items || [];
    const res = items.length ? await env.DB.batch(items.map((it) => stmt.bind(it.priority ?? 0, it.model_p ?? null, it.money ?? null, it.id))) : [];
    return json({ ok: true, updated: res.reduce((n, r) => n + r.meta.changes, 0) });
  }
  if (path === "notify") {
    await say(env, String(body.text || "").slice(0, 3500));
    return json({ ok: true });
  }
  if (path === "import") {
    // одноразове перенесення зі старого журналу; повторний виклик нічого не перезаписує
    const cols = ["id", "kind", "status", "text_x", "text_tg", "context", "features", "priority", "skips", "edited", "created_at",
      "review_msg_id", "review_sent_at", "decided_at", "decided_by", "x_sent_at", "x_id", "channel_msg_id", "channel_at", "note"];
    const stmt = env.DB.prepare(`INSERT OR IGNORE INTO posts (${cols.join(",")}) VALUES (${cols.map(() => "?").join(",")})`);
    const posts = body.posts || [];
    let added = 0;
    for (let i = 0; i < posts.length; i += 50) {
      const res = await env.DB.batch(posts.slice(i, i + 50).map((p) => stmt.bind(...cols.map((c) => p[c] ?? (c === "priority" || c === "skips" || c === "edited" ? 0 : null)))));
      added += res.reduce((n, r) => n + r.meta.changes, 0);
    }
    for (const [k, v] of Object.entries(body.meta || {})) if (v !== null && v !== undefined && !(await meta(env, k))) await meta(env, k, v);
    return json({ ok: true, added });
  }
  if (path === "tick") {
    await tick(env);
    return json({ ok: true });
  }
  return json({ ok: false, error: "unknown" }, 404);
}
