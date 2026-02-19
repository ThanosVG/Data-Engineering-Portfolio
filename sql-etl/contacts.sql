-- Day 2: Contact DB Script (Idempotent Version)

CREATE TABLE IF NOT EXISTS contacts (
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE,  -- Prevent duplicate names (optional but good practice)
    phone TEXT UNIQUE  -- Prevent duplicate phones
);

CREATE TABLE IF NOT EXISTS emails (
    contact_id INTEGER,
    email TEXT UNIQUE  -- Prevent duplicate emails
);

-- Clear old data before inserting (makes script rerun-safe)
DELETE FROM contacts;
DELETE FROM emails;

-- Insert fresh data (ids will auto-increment starting from 1)
INSERT INTO contacts (name, phone) VALUES ('Alice', '123-456');
INSERT INTO contacts (name, phone) VALUES ('Bob', '456-123');
INSERT INTO contacts (name, phone) VALUES ('Sudoku', '698-787-6763');
INSERT INTO contacts (name, phone) VALUES ('Tanas', '697-838-38383');
INSERT INTO contacts (name, phone) VALUES ('Todor', '688-939-3221');

INSERT INTO emails (contact_id, email) VALUES (1, 'alice@example.com');
INSERT INTO emails (contact_id, email) VALUES (2, 'bob@example.com');
INSERT INTO emails (contact_id, email) VALUES (3, 'sudoku@example.com');
INSERT INTO emails (contact_id, email) VALUES (4, 'tanas@example.com');
INSERT INTO emails (contact_id, email) VALUES (5, 'todor@example.com');

-- Test query: Join tables (should now show clean matches)
SELECT c.name, c.phone, e.email
FROM contacts c
JOIN emails e ON c.id = e.contact_id;