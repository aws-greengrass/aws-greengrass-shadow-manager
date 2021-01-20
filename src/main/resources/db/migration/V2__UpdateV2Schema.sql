
DROP TABLE IF EXISTS documents;

CREATE TABLE IF NOT EXISTS documents (
    thingName VARCHAR(255) NOT NULL,
    shadowName VARCHAR(255) NOT NULL,
    document TEXT,
    PRIMARY KEY (thingName, shadowName)
);
CREATE TABLE IF NOT EXISTS sync (
    thingName VARCHAR(255) NOT NULL,
    shadowName VARCHAR(255) NOT NULL,
    localDocument TEXT,
    cloudDocument TEXT,
    PRIMARY KEY (thingName, shadowName)
);

