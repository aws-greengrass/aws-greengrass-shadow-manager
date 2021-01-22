
DROP TABLE IF EXISTS documents;

CREATE TABLE IF NOT EXISTS documents (
    thingName VARCHAR(255) NOT NULL,
    shadowName VARCHAR(255) NOT NULL,
    document TEXT,
    PRIMARY KEY (thingName, shadowName)
);