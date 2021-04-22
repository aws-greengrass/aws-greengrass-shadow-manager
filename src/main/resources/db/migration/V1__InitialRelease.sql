CREATE TABLE documents (
    thingName VARCHAR(255) NOT NULL,
    shadowName VARCHAR(255) NOT NULL,
    document TEXT,
    version NUMBER DEFAULT 1,
    deleted BIT DEFAULT 0,
    updateTime NUMBER,
    PRIMARY KEY (thingName, shadowName)
);

CREATE TABLE sync (
    thingName VARCHAR(255) NOT NULL,
    shadowName VARCHAR(255) NOT NULL,
    lastSyncedDocument TEXT,
    localVersion NUMBER,
    cloudVersion NUMBER,
    cloudDeleted BIT,
    cloudUpdateTime NUMBER,
    lastSyncTime NUMBER,
    PRIMARY KEY (thingName, shadowName)
);
