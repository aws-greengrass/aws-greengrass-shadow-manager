CREATE TABLE documents (
    thingName VARCHAR(255) NOT NULL,
    shadowName VARCHAR(255) NOT NULL,
    document TEXT,
    version NUMBER DEFAULT 1,
    deleted BIT DEFAULT 0,
    updateTime TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (thingName, shadowName)
);

CREATE TABLE sync (
    thingName VARCHAR(255) NOT NULL,
    shadowName VARCHAR(255) NOT NULL,
    cloudDocument TEXT,
    cloudVersion NUMBER,
    cloudDeleted BIT,
    cloudUpdateTime TIMESTAMP WITH TIME ZONE,
    lastSyncTime TIMESTAMP WITH TIME ZONE,
    PRIMARY KEY (thingName, shadowName)
);
