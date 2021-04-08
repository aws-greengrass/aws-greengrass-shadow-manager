CREATE TABLE documents (
    thingName VARCHAR(255) NOT NULL,
    shadowName VARCHAR(255) NOT NULL,
    document TEXT,
    PRIMARY KEY (thingName, shadowName)
);

CREATE TABLE sync (
    thingName VARCHAR(255) NOT NULL,
    shadowName VARCHAR(255) NOT NULL,
    cloudDocument TEXT,
    localVersion NUMBER,
    cloudVersion NUMBER,
    localUpdateTime TIMESTAMP WITH TIME ZONE,
    cloudUpdateTime TIMESTAMP WITH TIME ZONE,
    lastSyncTime TIMESTAMP WITH TIME ZONE,
    PRIMARY KEY (thingName, shadowName)
);
