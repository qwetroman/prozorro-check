// Імітація Bot API для локальних тестів: записує всі виклики, повертає правдоподібні відповіді.
import http from "node:http";
const calls = [];
let nextId = 5000;
const oldBelow = Number(process.env.OLD_BELOW || 100);   // повідомлення з id < OLD_BELOW «старші за 48 год»
http.createServer((req, res) => {
  let body = "";
  req.on("data", (c) => (body += c));
  req.on("end", () => {
    if (req.url === "/__calls") { res.end(JSON.stringify(calls)); return; }
    if (req.url === "/__reset") { calls.length = 0; res.end("{}"); return; }
    if (req.url === "/2/tweets") {          // імітація X API
      calls.push({ method: "x:tweet", params: JSON.parse(body), auth: req.headers.authorization });
      res.setHeader("content-type", "application/json");
      res.statusCode = 201;
      res.end(JSON.stringify({ data: { id: String(1900000000000000000n + BigInt(calls.length)), text: "" } }));
      return;
    }
    const method = req.url.split("/").pop();
    const params = body ? JSON.parse(body) : {};
    calls.push({ method, params });
    let out = { ok: true, result: true };
    if (method === "sendMessage") out.result = { message_id: nextId++ };
    if (method === "getWebhookInfo") out.result = { url: "", pending_update_count: 0 };
    if ((method === "deleteMessage") && params.message_id < oldBelow)
      out = { ok: false, error_code: 400, description: "Bad Request: message can't be deleted for everyone" };
    res.setHeader("content-type", "application/json");
    res.end(JSON.stringify(out));
  });
}).listen(9999, () => console.log("mock telegram on 9999"));
