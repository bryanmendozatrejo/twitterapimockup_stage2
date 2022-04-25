PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS users (
    id        INTEGER PRIMARY KEY,
    username  TEXT NOT NULL UNIQUE,
    bio       TEXT NOT NULL,
    email     TEXT NOT NULL UNIQUE,
    password  TEXT NOT NULL
);
INSERT INTO users VALUES(1, 'ProfAvery', 'Hello I am cool', 'kavery@fullerton.edu', 'password');
INSERT INTO users VALUES(2, 'KevinAWortman', 'Hello I am better', 'kwortman@fullerton.edu', 'qwerty');
INSERT INTO users VALUES(3, 'Beth_CSUF', 'Hello I hate you', 'beth.harnick.shapiro@fullerton.edu', 'secret');

CREATE TABLE IF NOT EXISTS followers (
    id                  INTEGER PRIMARY KEY,
    username            TEXT NOT NULL,
    friend_username     TEXT NOT NULL,

    FOREIGN KEY(username) REFERENCES users(username),
    FOREIGN KEY(friend_username) REFERENCES users(username),
    UNIQUE(username, friend_username)
);
INSERT INTO followers VALUES(1, 'ProfAvery', 'KevinAWortman');

CREATE VIEW IF NOT EXISTS following
AS
    SELECT users.username, friends.username as friendname
    FROM users, followers, users AS friends
    WHERE
        users.id = followers.username AND
        followers.friend_username = friends.id;
