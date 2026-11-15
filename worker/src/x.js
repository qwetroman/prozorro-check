// Публікація в X через API (OAuth 1.0a від імені користувача). Використовується, лише якщо задано всі чотири ключі:
// X_API_KEY, X_API_SECRET, X_ACCESS_TOKEN, X_ACCESS_SECRET (застосунок із правами Read and write).

export const hasXKeys = (env) => Boolean(env.X_API_KEY && env.X_API_SECRET && env.X_ACCESS_TOKEN && env.X_ACCESS_SECRET);

// RFC 3986, як вимагає OAuth 1.0a
export const pct = (s) => encodeURIComponent(String(s)).replace(/[!'()*]/g, (c) => "%" + c.charCodeAt(0).toString(16).toUpperCase());

async function hmacSha1Base64(key, msg) {
  const k = await crypto.subtle.importKey("raw", new TextEncoder().encode(key), { name: "HMAC", hash: "SHA-1" }, false, ["sign"]);
  const sig = new Uint8Array(await crypto.subtle.sign("HMAC", k, new TextEncoder().encode(msg)));
  return btoa(String.fromCharCode(...sig));
}

// signedParams: параметри, що входять у підпис (query або form); для JSON-тіла - лише oauth_*
export async function oauthHeader({ method, url, consumerKey, consumerSecret, token, tokenSecret, nonce, timestamp, signedParams = {} }) {
  const oauth = {
    oauth_consumer_key: consumerKey,
    oauth_nonce: nonce ?? [...crypto.getRandomValues(new Uint8Array(16))].map((b) => b.toString(16).padStart(2, "0")).join(""),
    oauth_signature_method: "HMAC-SHA1",
    oauth_timestamp: String(timestamp ?? Math.floor(Date.now() / 1000)),
    oauth_token: token,
    oauth_version: "1.0",
  };
  const all = { ...signedParams, ...oauth };
  const paramStr = Object.keys(all).map((k) => [pct(k), pct(all[k])]).sort(([a, x], [b, y]) => (a < b ? -1 : a > b ? 1 : x < y ? -1 : 1))
    .map(([k, v]) => `${k}=${v}`).join("&");
  const base = [method.toUpperCase(), pct(url), pct(paramStr)].join("&");
  oauth.oauth_signature = await hmacSha1Base64(`${pct(consumerSecret)}&${pct(tokenSecret)}`, base);
  return "OAuth " + Object.keys(oauth).sort().map((k) => `${pct(k)}="${pct(oauth[k])}"`).join(", ");
}

export async function postToX(env, text) {
  const url = `${env.X_API || "https://api.x.com"}/2/tweets`;
  const auth = await oauthHeader({ method: "POST", url, consumerKey: env.X_API_KEY, consumerSecret: env.X_API_SECRET,
    token: env.X_ACCESS_TOKEN, tokenSecret: env.X_ACCESS_SECRET });
  const r = await fetch(url, { method: "POST", headers: { Authorization: auth, "content-type": "application/json" },
    body: JSON.stringify({ text }) });
  const body = await r.text();
  if (r.status >= 300) throw new Error(`X API ${r.status}: ${body.slice(0, 300)}`);
  return JSON.parse(body).data.id;
}
