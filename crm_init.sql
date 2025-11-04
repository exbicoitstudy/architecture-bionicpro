CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    first_name VARCHAR(255) NOT NULL,
    last_name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE devices (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(id),
    serial_number VARCHAR(255) NOT NULL UNIQUE,
    activated_at TIMESTAMP
);

INSERT INTO users (first_name, last_name, email)
VALUES
    ('Prothetic', 'One',  'prothetic1@example.com'),
    ('Prothetic', 'Two',  'prothetic2@example.com'),
    ('Prothetic', 'Three',  'prothetic3@example.com');


INSERT INTO devices (user_id, serial_number, activated_at)
SELECT u.id, 'device0001', NOW() - INTERVAL '30 days'
FROM users u
WHERE u.email = 'prothetic1@example.com';

INSERT INTO devices (user_id, serial_number, activated_at)
SELECT u.id, 'device0002',  NOW() - INTERVAL '10 days'
FROM users u
WHERE u.email = 'prothetic2@example.com';

INSERT INTO devices (user_id, serial_number, activated_at)
SELECT u.id, 'device0003',  NOW() - INTERVAL '10 days'
FROM users u
WHERE u.email = 'prothetic3@example.com';