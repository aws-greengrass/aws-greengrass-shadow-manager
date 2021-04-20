/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager;


import com.aws.greengrass.shadowmanager.exception.ShadowManagerDataException;
import com.aws.greengrass.shadowmanager.model.ShadowDocument;
import com.aws.greengrass.shadowmanager.model.dao.SyncInformation;
import com.aws.greengrass.util.Pair;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;

// TODO: record UTC epoch seconds when updating/deleting shadow
public class ShadowManagerDAOImpl implements ShadowManagerDAO {
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
        try (PreparedStatement preparedStatement = database.connection().prepareStatement(sql)) {
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
        } catch (SQLException | IOException e) {
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

        return getShadowThing(thingName, shadowName)
                .flatMap(shadowDocument ->
                        execute("UPDATE documents SET deleted = 1, document = null, updateTime = ?"
                                        + " WHERE thingName = ? AND shadowName = ?",
                                preparedStatement -> {
                                    preparedStatement.setLong(1, Instant.now().getEpochSecond());
                                    preparedStatement.setString(2, thingName);
                                    preparedStatement.setString(3, shadowName);
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
        return execute("MERGE INTO documents(thingName, shadowName, document, version, deleted, updateTime) "
                        + "KEY (thingName, shadowName) VALUES (?, ?, ?, ?, ?, ?)",
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
        return execute("MERGE INTO sync(thingName, shadowName, cloudDocument, cloudVersion, cloudDeleted, "
                        + "cloudUpdateTime, lastSyncTime) KEY (thingName, shadowName) VALUES (?, ?, ?, ?, ?, ?, ?)",
                preparedStatement -> {
                    preparedStatement.setString(1, request.getThingName());
                    preparedStatement.setString(2, request.getShadowName());
                    preparedStatement.setBytes(3, request.getCloudDocument());
                    preparedStatement.setLong(4, request.getCloudVersion());
                    preparedStatement.setBoolean(5, request.isCloudDeleted());
                    preparedStatement.setLong(6, request.getCloudUpdateTime());
                    preparedStatement.setLong(7, request.getLastSyncTime());
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
        return execute("SELECT cloudDocument, cloudVersion, cloudUpdateTime, lastSyncTime, cloudDeleted FROM sync "
                        + "WHERE thingName = ? AND shadowName = ?",
                preparedStatement -> {
                    preparedStatement.setString(1, thingName);
                    preparedStatement.setString(2, shadowName);
                    try (ResultSet resultSet = preparedStatement.executeQuery()) {
                        if (resultSet.next()) {
                            return Optional.ofNullable(SyncInformation.builder()
                                    .cloudDocument(resultSet.getBytes(1))
                                    .cloudVersion(resultSet.getLong(2))
                                    .cloudUpdateTime(resultSet.getLong(3))
                                    .lastSyncTime(resultSet.getLong(4))
                                    .cloudDeleted(resultSet.getBoolean(5))
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
     * Attempts to delete the cloud shadow document in the sync table.
     *
     * @param thingName  Name of the Thing.
     * @param shadowName Name of shadow.
     * @return true if the cloud document (soft) delete was successful or not.
     */
    @Override
    public boolean deleteSyncInformation(String thingName, String shadowName) {
        return execute("DELETE FROM sync WHERE thingName = ? AND shadowName = ?",
                preparedStatement -> {
                    preparedStatement.setString(1, thingName);
                    preparedStatement.setString(2, shadowName);
                    int result = preparedStatement.executeUpdate();
                    return result == 1;
                });

    }

    private <T> T execute(String sql, SQLExecution<T> thunk) {
        try (PreparedStatement statement = database.connection().prepareStatement(sql)) {
            return thunk.apply(statement);
        } catch (SQLException e) {
            throw new ShadowManagerDataException(e);
        }
    }
}
