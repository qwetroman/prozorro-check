-- Єдине сховище бота: чернетки, рішення, публікації та журнал усіх подій Telegram.

CREATE TABLE IF NOT EXISTS posts (
  id             TEXT PRIMARY KEY,          -- UA-2026-... або PAT-...
  kind           TEXT NOT NULL DEFAULT 'tender',   -- tender | pattern | explore
  status         TEXT NOT NULL,             -- queued | in_review | awaiting_edit | approved | published
                                            -- | rejected | suppressed | expired | failed
  text_x         TEXT,                      -- коротка версія (X, до 280)
  text_tg        TEXT,                      -- повна версія (Telegram-канал); NULL = як text_x
  context        TEXT,                      -- лише для перевірки, не публікується
  features       TEXT,                      -- JSON ознак (для моделі)
  priority       REAL NOT NULL DEFAULT 0,   -- чим більше, тим раніше показуємо
  skips          INTEGER NOT NULL DEFAULT 0,  -- скільки разів відкладено кнопкою «Пізніше»
  edited         INTEGER NOT NULL DEFAULT 0,
  created_at     TEXT NOT NULL,
  review_msg_id  INTEGER,                   -- поточне повідомлення в чаті перевірки
  review_sent_at TEXT,
  edit_prompt_id INTEGER,
  decided_at     TEXT,
  decided_by     TEXT,
  x_sent_at      TEXT,                      -- коли надіслано посилання для X
  channel_msg_id INTEGER,                   -- -1 = у канал не публікується
  channel_at     TEXT,
  note           TEXT
);
CREATE INDEX IF NOT EXISTS posts_status ON posts (status);
CREATE INDEX IF NOT EXISTS posts_review_msg ON posts (review_msg_id);

CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT);

-- кожне оновлення від Telegram і кожна помилка: щоб жодне натискання не зникало безслідно
CREATE TABLE IF NOT EXISTS events (
  id        INTEGER PRIMARY KEY AUTOINCREMENT,
  at        TEXT NOT NULL,
  update_id INTEGER UNIQUE,
  kind      TEXT NOT NULL,                  -- click | message | error | info
  post_id   TEXT,
  user_id   TEXT,
  data      TEXT
);
CREATE INDEX IF NOT EXISTS events_at ON events (at);
