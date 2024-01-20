CREATE TABLE
  Participant (
    id BIGINT PRIMARY KEY,
    telegram_nickname TEXT NOT NULL,
    name TEXT,
    surname TEXT,
    age INTEGER,
    is_admin BOOLEAN DEFAULT FALSE,
    verst_id INTEGER
  );

CREATE TABLE
  Role (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    emoji TEXT NOT NULL,
    is_default BOOLEAN NOT NULL DEFAULT FALSE
  );

CREATE TABLE
  Event (
    id INTEGER PRIMARY KEY,
    event_date TEXT NOT NULL,
    event_time TEXT NOT NULL,
    description TEXT
  );

CREATE TABLE
  Volunteer (
    event_id INTEGER NOT NULL,
    role_id INTEGER NOT NULL,
    participant_id BIGINT,
    PRIMARY KEY (event_id, role_id),
    FOREIGN KEY (event_id) REFERENCES Event (id),
    FOREIGN KEY (role_id) REFERENCES Position(id),
    FOREIGN KEY (participant_id) REFERENCES Participant (id)
  );