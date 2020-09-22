package com.aws.greengrass.shadowmanager;


import com.aws.greengrass.shadowmanager.exception.ShadowManagerDataException;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import javax.inject.Inject;

/**
 * This is a no-op DAO for now, while we wire in the data layer in a little bit.
 */
public class ShadowManagerDAOImpl implements ShadowManagerDAO {
    private final ShadowManagerDatabase database;
    private static final String STATE = "state";

    @FunctionalInterface
    private interface SQLExecution<T> {
        T apply(PreparedStatement statement) throws SQLException;
    }

    @Inject
    public ShadowManagerDAOImpl(final ShadowManagerDatabase database) {
        this.database = database;
    }

    /**
     * Adds an entry for the specified thingName with the newDocument.
     * @param thingName The thing namespace of the shadow document.
     * @param initialDocument The initial shadow document.
     * @return
     */
    @Override
    public Optional<byte[]> createShadowThing(String thingName, byte[] initialDocument) {
        return execute("INSERT INTO documents VALUES (?, ?, ?)", preparedStatement -> {
            preparedStatement.setString(1, thingName);
            preparedStatement.setBytes(2, initialDocument);
            preparedStatement.setString(3, thingName);
            int result = preparedStatement.executeUpdate();
            if (result == 1) {
                return Optional.ofNullable(initialDocument);
            }
            return Optional.empty();
        });
    }

    /**
     * No-op ... will never find a shadow document.
     * @param thingName The thing namespace of the shadow document.
     * @return
     */
    @Override
    public Optional<byte[]> getShadowThing(String thingName) {
        return execute("SELECT arn, state FROM documents WHERE name = ?", preparedStatement -> {
            preparedStatement.setString(1, thingName);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return Optional.ofNullable(resultSet.getBytes(STATE));
            }
            return Optional.empty();
        });
    }

    /**
     * No-op ... will never delete a shadow document.
     * @param thingName The thing namespace of the shadow document.
     * @return
     */
    @Override
    public Optional<byte[]> deleteShadowThing(String thingName) {
        Optional<byte[]> existingThing = getShadowThing(thingName);
        if (existingThing.isPresent()) {
            return execute("DELETE FROM documents WHERE name = ?", preparedStatement -> {
                preparedStatement.setString(1, thingName);
                int result = preparedStatement.executeUpdate();
                if (result == 1) {
                    return existingThing;
                }
                return Optional.empty();
            });
        } else {
            return Optional.empty();
        }
    }

    /**
     * No-op ... will never update a shadow document.
     * @param thingName The thing namespace of the shadow document.
     * @param newDocument The new shadow document.
     * @return
     */
    @Override
    public Optional<byte[]> updateShadowThing(String thingName, byte[] newDocument) {
        return execute("UPDATE documents SET state = ? WHERE name = ?", preparedStatement -> {
            preparedStatement.setBytes(1, newDocument);
            preparedStatement.setString(2, thingName);
            int result = preparedStatement.executeUpdate();
            if (result == 1) {
                return Optional.ofNullable(newDocument);
            }
            return Optional.empty();
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