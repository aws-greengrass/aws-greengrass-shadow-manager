/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager;


import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.shadowmanager.exception.ShadowManagerDataException;
import com.aws.greengrass.shadowmanager.model.ShadowDocument;
import com.aws.greengrass.shadowmanager.model.dao.SyncInformation;
import com.aws.greengrass.util.Pair;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;

import static com.aws.greengrass.shadowmanager.model.Constants.LOG_CLOUD_VERSION_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_LOCAL_VERSION_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_SHADOW_NAME_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_THING_NAME_KEY;

public class ShadowManagerDAOImpl implements ShadowManagerDAO {
    private static final Logger logger = LogManager.getLogger(ShadowManagerDAOImpl.class);
    private final ShadowManagerDatabase database;

    @FunctionalInterface
    private interface SQLExecution<T> {
        T apply(PreparedStatement statement) throws SQLException;
    }

    @Inject
    public ShadowManagerDAOImpl(final ShadowManagerDatabase database) {
        this.database = database;
    }

    /**
     * Attempts to obtain a shadow document from the local shadow storage.
     *
     * @param thingName  Name of the Thing for the shadow topic prefix.
     * @param shadowName Name of shadow topic prefix for thing.
     * @return The queried shadow from the local shadow store
     */
    @Override
    public Optional<ShadowDocument> getShadowThing(String thingName, String shadowName) {
        String sql = "SELECT document, version, updateTime FROM documents  WHERE deleted = 0 AND "
                + "thingName = ? AND shadowName = ?";

        try (Connection c = database.getPool().getConnection();
             PreparedStatement preparedStatement = c.prepareStatement(sql)) {
            preparedStatement.setString(1, thingName);
            preparedStatement.setString(2, shadowName);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    ShadowDocument document = new ShadowDocument(resultSet.getBytes(1),
                            resultSet.getLong(2));
                    return Optional.of(document);
                }
                return Optional.empty();
            }
        } catch (SQLException | IOException | IllegalStateException e) {
            throw new ShadowManagerDataException(e);
        }
    }

    /**
     * Attempts to delete the shadow document from the local shadow storage.
     *
     * @param thingName  Name of the Thing for the shadow topic prefix.
     * @param shadowName Name of shadow topic prefix for thing.
     * @return The deleted shadow from the local shadow store
     */
    @Override
    public Optional<ShadowDocument> deleteShadowThing(String thingName, String shadowName) {
        // To be consistent with cloud, subsequent updates to the shadow should not start from version 0
        // https://docs.aws.amazon.com/iot/latest/developerguide/device-shadow-data-flow.html
        logger.atDebug()
                .kv(LOG_THING_NAME_KEY, thingName)
                .kv(LOG_SHADOW_NAME_KEY, shadowName)
                .log("Deleting shadow");
        String sql = "UPDATE documents SET deleted = 1, document = null, updateTime = ?, version = ?"
                + " WHERE thingName = ? AND shadowName = ?";
        return getShadowThing(thingName, shadowName)
                .flatMap(shadowDocument ->
                        executeWriteOperation(sql,
                                preparedStatement -> {
                                    preparedStatement.setLong(1, Instant.now().getEpochSecond());
                                    preparedStatement.setLong(2, shadowDocument.getVersion() + 1);
                                    preparedStatement.setString(3, thingName);
                                    preparedStatement.setString(4, shadowName);
                                    int result = preparedStatement.executeUpdate();
                                    if (result == 1) {
                                        return Optional.of(shadowDocument);
                                    }
                                    return Optional.empty();
                                }));
    }

    /**
     * Attempts to update a shadow document from the local shadow storage. Will create document if shadow did not exist.
     *
     * @param thingName   Name of the Thing for the shadow topic prefix.
     * @param shadowName  Name of shadow topic prefix for thing.
     * @param newDocument The new shadow document.
     * @param version     The new version of the shadow document.
     * @return The updated shadow document from the local shadow store
     */
    @Override
    public Optional<byte[]> updateShadowThing(String thingName, String shadowName, byte[] newDocument, long version) {
        logger.atTrace()
                .kv(LOG_THING_NAME_KEY, thingName)
                .kv(LOG_SHADOW_NAME_KEY, shadowName)
                .log("Updating shadow");
        String sql = "MERGE INTO documents(thingName, shadowName, document, version, deleted, updateTime) "
                + "KEY (thingName, shadowName) VALUES (?, ?, ?, ?, ?, ?)";
        return executeWriteOperation(sql,
                preparedStatement -> {
                    preparedStatement.setString(1, thingName);
                    preparedStatement.setString(2, shadowName);
                    preparedStatement.setBytes(3, newDocument);
                    preparedStatement.setLong(4, version);
                    preparedStatement.setBoolean(5, false);
                    preparedStatement.setLong(6, Instant.now().getEpochSecond());
                    int result = preparedStatement.executeUpdate();
                    if (result == 1) {
                        return Optional.ofNullable(newDocument);
                    }
                    return Optional.empty();
                });
    }

    /**
     * Attempts to retrieve list of named shadows for a specified thing from the local shadow storage.
     *
     * @param thingName Name of the Thing to check Named Shadows.
     * @param offset    Number of Named Shadows to bypass.
     * @param limit     Maximum number of Named Shadows to retrieve.
     * @return A limited list of named shadows matching the specified thingName
     */
    @Override
    public List<String> listNamedShadowsForThing(String thingName, int offset, int limit) {
        return execute("SELECT shadowName from documents WHERE deleted = 0 AND thingName = ? AND shadowName != ''"
                        + " LIMIT ? OFFSET ? ",
                preparedStatement -> {
                    preparedStatement.setString(1, thingName);
                    preparedStatement.setInt(2, limit);
                    preparedStatement.setInt(3, offset);
                    try (ResultSet resultSet = preparedStatement.executeQuery()) {
                        List<String> namedShadowList = new ArrayList<>();
                        while (resultSet.next()) {
                            namedShadowList.add(resultSet.getString(1));
                        }
                        return namedShadowList;
                    }
                });
    }

    /**
     * Attempts to update the sync information for a particular thing's shadow.
     *
     * @param request The update shadow sync information request containing the necessary information to update.
     * @return true if the update is successful; Else false.
     */
    @Override
    public boolean updateSyncInformation(final SyncInformation request) {
        logger.atInfo()
                .kv(LOG_THING_NAME_KEY, request.getThingName())
                .kv(LOG_SHADOW_NAME_KEY, request.getShadowName())
                .kv(LOG_LOCAL_VERSION_KEY, request.getLocalVersion())
                .kv(LOG_CLOUD_VERSION_KEY, request.getCloudVersion())
                .log("Updating sync info");
        String sql = "MERGE INTO sync(thingName, shadowName, lastSyncedDocument, cloudVersion, cloudDeleted, "
                + "cloudUpdateTime, lastSyncTime, localVersion) KEY (thingName, shadowName) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        return executeWriteOperation(sql,
                preparedStatement -> {
                    preparedStatement.setString(1, request.getThingName());
                    preparedStatement.setString(2, request.getShadowName());
                    preparedStatement.setBytes(3, request.getLastSyncedDocument());
                    preparedStatement.setLong(4, request.getCloudVersion());
                    preparedStatement.setBoolean(5, request.isCloudDeleted());
                    preparedStatement.setLong(6, request.getCloudUpdateTime());
                    preparedStatement.setLong(7, request.getLastSyncTime());
                    preparedStatement.setLong(8, request.getLocalVersion());
                    int result = preparedStatement.executeUpdate();
                    return result == 1;
                });
    }

    /**
     * Attempts to obtain the shadow sync information for a particular thing's shadow.
     *
     * @param thingName  Name of the Thing.
     * @param shadowName Name of shadow.
     * @return The queried shadow sync information from the local shadow store
     */
    @Override
    public Optional<SyncInformation> getShadowSyncInformation(String thingName, String shadowName) {
        return execute("SELECT lastSyncedDocument, cloudVersion, cloudUpdateTime, lastSyncTime, cloudDeleted, "
                        + "localVersion FROM sync WHERE thingName = ? AND shadowName = ?",
                preparedStatement -> {
                    preparedStatement.setString(1, thingName);
                    preparedStatement.setString(2, shadowName);
                    try (ResultSet resultSet = preparedStatement.executeQuery()) {
                        if (resultSet.next()) {
                            return Optional.ofNullable(SyncInformation.builder()
                                    .lastSyncedDocument(resultSet.getBytes(1))
                                    .cloudVersion(resultSet.getLong(2))
                                    .cloudUpdateTime(resultSet.getLong(3))
                                    .lastSyncTime(resultSet.getLong(4))
                                    .cloudDeleted(resultSet.getBoolean(5))
                                    .localVersion(resultSet.getLong(6))
                                    .shadowName(shadowName)
                                    .thingName(thingName)
                                    .build());
                        }
                        return Optional.empty();
                    }
                });
    }

    /**
     * Attempts to obtain a list of all synced shadow names.
     *
     * @return The queried synced shadow names list.
     */
    @Override
    public List<Pair<String, String>> listSyncedShadows() {
        return execute("SELECT thingName, shadowName FROM sync ",
                preparedStatement -> {
                    try (ResultSet resultSet = preparedStatement.executeQuery()) {
                        List<Pair<String, String>> syncedShadowList = new ArrayList<>();
                        while (resultSet.next()) {
                            syncedShadowList.add(new Pair<>(resultSet.getString(1),
                                    resultSet.getString(2)));
                        }
                        return syncedShadowList;
                    }
                });
    }

    /**
     * Get the shadow document version of a deleted shadow.
     *
     * @param thingName  Name of the Thing for the shadow topic prefix.
     * @param shadowName Name of shadow topic prefix for thing.
     * @return The deleted shadow version if it was deleted or exists; Else an empty optional
     */
    @Override
    public Optional<Long> getDeletedShadowVersion(String thingName, String shadowName) {
        String sql = "SELECT version FROM documents  WHERE deleted = 1 AND thingName = ? AND shadowName = ?";

        try (Connection c = database.getPool().getConnection();
             PreparedStatement preparedStatement = c.prepareStatement(sql)) {
            preparedStatement.setString(1, thingName);
            preparedStatement.setString(2, shadowName);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    return Optional.of(resultSet.getLong(1));
                }
                return Optional.empty();
            }
        } catch (SQLException | IllegalStateException e) {
            throw new ShadowManagerDataException(e);
        }
    }


    /**
     * Attempts to delete the cloud shadow document in the sync table.
     *
     * @param thingName  Name of the Thing.
     * @param shadowName Name of shadow.
     * @return true if the cloud document (soft) delete was successful or not.
     */
    @Override
    public boolean deleteSyncInformation(String thingName, String shadowName) {
        logger.atDebug()
                .kv(LOG_THING_NAME_KEY, thingName)
                .kv(LOG_SHADOW_NAME_KEY, shadowName)
                .log("Deleting sync info");
        return executeWriteOperation("DELETE FROM sync WHERE thingName = ? AND shadowName = ?",
                preparedStatement -> {
                    preparedStatement.setString(1, thingName);
                    preparedStatement.setString(2, shadowName);
                    int result = preparedStatement.executeUpdate();
                    return result == 1;
                });

    }

    /**
     * Attempts to get the shadow document version.
     *
     * @param thingName  Name of the Thing for the shadow topic prefix.
     * @param shadowName Name of shadow topic prefix for thing.
     * @return Optional containing the new shadow document version if document exists; Else an empty optional
     */
    @Override
    public Optional<Long> getShadowDocumentVersion(String thingName, String shadowName) {
        return execute("SELECT version FROM documents WHERE thingName = ? AND shadowName = ?",
                preparedStatement -> {
                    preparedStatement.setString(1, thingName);
                    preparedStatement.setString(2, shadowName);
                    try (ResultSet resultSet = preparedStatement.executeQuery()) {
                        if (resultSet.next()) {
                            return Optional.of(resultSet.getLong(1));
                        }
                    }
                    return Optional.empty();
                });
    }

    /**
     * Attempts to insert a new sync information row for a thing's shadow if it does not exist.
     *
     * @param request The update shadow sync information request containing the necessary information to update.
     * @return true if the insert is successful; Else false.
     */
    @Override
    public boolean insertSyncInfoIfNotExists(SyncInformation request) {
        logger.atTrace()
                .kv(LOG_THING_NAME_KEY, request.getThingName())
                .kv(LOG_SHADOW_NAME_KEY, request.getShadowName())
                .kv(LOG_LOCAL_VERSION_KEY, request.getLocalVersion())
                .kv(LOG_CLOUD_VERSION_KEY, request.getCloudVersion())
                .log("Inserting sync info");
        String sql = "INSERT INTO sync(thingName, shadowName, lastSyncedDocument, cloudVersion, cloudDeleted, "
                + "cloudUpdateTime, lastSyncTime, localVersion) SELECT ?, ?, ?, ?, ?, ?, ?, ? "
                + "WHERE NOT EXISTS(SELECT 1 FROM sync WHERE thingName = ? AND shadowName = ?)";
        return executeWriteOperation(sql,
                preparedStatement -> {
                    preparedStatement.setString(1, request.getThingName());
                    preparedStatement.setString(2, request.getShadowName());
                    preparedStatement.setBytes(3, request.getLastSyncedDocument());
                    preparedStatement.setLong(4, request.getCloudVersion());
                    preparedStatement.setBoolean(5, request.isCloudDeleted());
                    preparedStatement.setLong(6, request.getCloudUpdateTime());
                    preparedStatement.setLong(7, request.getLastSyncTime());
                    preparedStatement.setLong(8, request.getLocalVersion());
                    preparedStatement.setString(9, request.getThingName());
                    preparedStatement.setString(10, request.getShadowName());
                    int result = preparedStatement.executeUpdate();
                    return result == 1;
                });

    }

    @Override
    public void waitForDBOperationsToFinish() {
        if (!dbWriteOperations.tryAcquire(maxPermits)) {
            logger.atDebug().log("Waiting for the DB write operations to finish");
            dbWriteOperations.acquireUninterruptibly(maxPermits);
        }
        logger.atDebug().log("Finished all the DB write operations");
    }

    private <T> T execute(String sql, SQLExecution<T> thunk) {
        try (Connection c = database.getPool().getConnection();
             PreparedStatement statement = c.prepareStatement(sql)) {
            return thunk.apply(statement);
        } catch (SQLException | IllegalStateException e) {
            throw new ShadowManagerDataException(e);
        }
    }

    private <T> T executeWriteOperation(String sql, SQLExecution<T> thunk) {
        try {
            dbWriteOperations.acquire();
        } catch (InterruptedException e) {
            logger.atDebug().log("Interrupted before performing the DB operation");
            Thread.currentThread().interrupt();
        }
        T result = execute(sql, thunk);
        dbWriteOperations.release();
        return result;
    }
}
