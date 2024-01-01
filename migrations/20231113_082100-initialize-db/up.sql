CREATE TABLE
  Participant (
    id BIGINT PRIMARY KEY,
    telegram_nickname TEXT NOT NULL,
    name TEXT,
    surname TEXT,
    age INTEGER,
    verst_id INTEGER,
    FOREIGN KEY (verst_id) REFERENCES VerstParticipant (id) ON DELETE CASCADE
  );

CREATE TABLE
  VerstParticipant (id INTEGER PRIMARY KEY, link TEXT NOT NULL);

CREATE TABLE
  VolunteerPosition (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    emoji TEXT NOT NULL
  );

CREATE TABLE
  Event (id INTEGER PRIMARY KEY, date TEXT NOT NULL);

CREATE TABLE
  EventVolunteer (
    event_id INTEGER NOT NULL,
    position_id INTEGER NOT NULL,
    participant_id INTEGER NOT NULL,
    PRIMARY KEY (event_id, position_id),
    FOREIGN KEY (event_id) REFERENCES Event (id) ON DELETE CASCADE,
    FOREIGN KEY (position_id) REFERENCES VolunteerPosition (id) ON DELETE CASCADE,
    FOREIGN KEY (participant_id) REFERENCES Participant (id) ON DELETE CASCADE
  )