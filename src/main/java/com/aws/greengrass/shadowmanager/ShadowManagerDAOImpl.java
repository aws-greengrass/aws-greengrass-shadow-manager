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
     * @return Optional
     */
    @Override
    public Optional<byte[]> createShadowThing(String thingName, String shadowName, byte[] initialDocument) {
        return execute("INSERT INTO documents VALUES (?, ?, ?)", preparedStatement -> {
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
     * @return Optional
     */
    @Override
    public Optional<byte[]> getShadowThing(String thingName, String shadowName) {
        return execute("SELECT thingName, shadowName, document FROM documents WHERE thingName = ? AND shadowName = ?",
                preparedStatement -> {
                    preparedStatement.setString(1, thingName);
                    preparedStatement.setString(2, shadowName);
                    ResultSet resultSet = preparedStatement.executeQuery();
                    if (resultSet.next()) {
                        return Optional.ofNullable(resultSet.getBytes(DOCUMENT));
                    }
                    return Optional.empty();
                });
    }

    /**
     * Attempts to delete the shadow document from the local shadow storage.
     *
     * @param thingName  Name of the Thing for the shadow topic prefix.
     * @param shadowName Name of shadow topic prefix for thing.
     * @return Optional
     */
    @Override
    public Optional<byte[]> deleteShadowThing(String thingName, String shadowName) {
        Optional<byte[]> existingThing = getShadowThing(thingName, shadowName);
        if (existingThing.isPresent()) {
            return execute("DELETE FROM documents WHERE thingName = ? AND shadowName = ?", preparedStatement -> {
                preparedStatement.setString(1, thingName);
                preparedStatement.setString(2, shadowName);
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
     * Attempts to update a shadow document from the local shadow storage.
     *
     * @param thingName   Name of the Thing for the shadow topic prefix.
     * @param shadowName  Name of shadow topic prefix for thing.
     * @param newDocument The new shadow document.
     * @return Optional
     */
    @Override
    public Optional<byte[]> updateShadowThing(String thingName, String shadowName, byte[] newDocument) {
        return execute("UPDATE documents SET document = ? WHERE thingName = ? AND shadowName = ?",
                preparedStatement -> {
                    preparedStatement.setBytes(1, newDocument);
                    preparedStatement.setString(2, thingName);
                    preparedStatement.setString(3, shadowName);
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