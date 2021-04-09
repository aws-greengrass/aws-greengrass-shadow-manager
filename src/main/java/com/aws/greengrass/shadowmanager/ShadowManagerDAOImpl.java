/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager;


import com.aws.greengrass.shadowmanager.exception.ShadowManagerDataException;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;

public class ShadowManagerDAOImpl implements ShadowManagerDAO {
    private final ShadowManagerDatabase database;
    private static final String DOCUMENT = "document";

    @FunctionalInterface
    private interface SQLExecution<T> {
        T apply(PreparedStatement statement) throws SQLException;
    }

    @Inject
    public ShadowManagerDAOImpl(final ShadowManagerDatabase database) {
        this.database = database;
    }

    /**
     * Adds an entry for the specified thingName,shadowName with the newDocument.
     *
     * @param thingName       The thing namespace of the shadow document.
     * @param shadowName      Name of shadow topic prefix for thing.
     * @param initialDocument The initial shadow document.
     * @return The shadow document inserted into the local shadow store
     */
    @Override
    public Optional<byte[]> createShadowThing(String thingName, String shadowName, byte[] initialDocument) {
        return execute("INSERT INTO documents (thingName, shadowName, document) VALUES (?, ?, ?)",
                preparedStatement -> {
                    preparedStatement.setString(1, thingName);
                    preparedStatement.setString(2, shadowName);
                    preparedStatement.setBytes(3, initialDocument);

                    int result = preparedStatement.executeUpdate();
                    if (result == 1) {
                        return Optional.ofNullable(initialDocument);
                    }
                    return Optional.empty();
        });
    }

    /**
     * Attempts to obtain a shadow document from the local shadow storage.
     *
     * @param thingName  Name of the Thing for the shadow topic prefix.
     * @param shadowName Name of shadow topic prefix for thing.
     * @return The queried shadow from the local shadow store
     */
    @Override
    public Optional<byte[]> getShadowThing(String thingName, String shadowName) {
        return execute("SELECT thingName, shadowName, document FROM documents WHERE thingName = ? AND shadowName = ?",
                preparedStatement -> {
                    preparedStatement.setString(1, thingName);
                    preparedStatement.setString(2, shadowName);
                    try (ResultSet resultSet = preparedStatement.executeQuery()) {
                        if (resultSet.next()) {
                            return Optional.ofNullable(resultSet.getBytes(DOCUMENT));
                        }
                        return Optional.empty();
                    }
                });
    }

    /**
     * Attempts to delete the shadow document from the local shadow storage.
     *
     * @param thingName  Name of the Thing for the shadow topic prefix.
     * @param shadowName Name of shadow topic prefix for thing.
     * @return The deleted shadow from the local shadow store
     */
    @Override
    public Optional<byte[]> deleteShadowThing(String thingName, String shadowName) {
        return getShadowThing(thingName, shadowName)
                .flatMap(shadowDocument ->
                        execute("DELETE FROM documents WHERE thingName = ? AND shadowName = ?",
                                preparedStatement -> {
                                    preparedStatement.setString(1, thingName);
                                    preparedStatement.setString(2, shadowName);
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
     * @return The updated shadow document from the local shadow store
     */
    @Override
    public Optional<byte[]> updateShadowThing(String thingName, String shadowName, byte[] newDocument) {
        return execute("MERGE INTO documents(thingName, shadowName, document) KEY (thingName, shadowName)"
                        + " VALUES (?, ?, ?)",
                preparedStatement -> {
                    preparedStatement.setString(1, thingName);
                    preparedStatement.setString(2, shadowName);
                    preparedStatement.setBytes(3, newDocument);
                    int result = preparedStatement.executeUpdate();
                    if (result == 1) {
                        return Optional.ofNullable(newDocument);
                    }
                    return Optional.empty();
                });
    }

    /**
     * Attempts to retrieve list of named shadows for a specified thing from the local shadow storage.
     * @param thingName Name of the Thing to check Named Shadows.
     * @param offset Number of Named Shadows to bypass.
     * @param limit Maximum number of Named Shadows to retrieve.
     * @return A limited list of named shadows matching the specified thingName
     */
    @Override
    public List<String> listNamedShadowsForThing(String thingName, int offset, int limit) {
        return execute("SELECT shadowName from documents WHERE thingName = ? AND shadowName != '' LIMIT ? OFFSET ? ",
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

    private <T> T execute(String sql, SQLExecution<T> thunk) {
        try (PreparedStatement statement = database.connection().prepareStatement(sql)) {
            return thunk.apply(statement);
        } catch (SQLException e) {
            throw new ShadowManagerDataException(e);
        }
    }
}
