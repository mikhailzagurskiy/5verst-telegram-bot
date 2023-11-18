CREATE TABLE
  Participant (
    id INTEGER PRIMARY KEY,
    telegram_nickname TEXT NOT NULL,
    name TEXT,
    surname TEXT,
    verst_id INTEGER,
    FOREIGN KEY (verst_id) REFERENCES VerstParticipant (id) ON DELETE CASCADE
  );

CREATE TABLE
  VerstParticipant (id INTEGER PRIMARY KEY, link TEXT NOT NULL);