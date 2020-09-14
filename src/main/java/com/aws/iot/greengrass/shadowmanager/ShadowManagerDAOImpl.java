package com.aws.iot.greengrass.shadowmanager;


import com.aws.iot.greengrass.shadowmanager.exception.ShadowManagerDataException;

import java.nio.ByteBuffer;
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

    @FunctionalInterface
    private interface SQLExecution<T> {
        T apply(PreparedStatement statement) throws SQLException;
    }

    @Inject
    public ShadowManagerDAOImpl(final ShadowManagerDatabase database) {
        this.database = database;
    }

    /**
     * No-op ... will never find a shadow document.
     * @param thingName The thing namespace of the shadow document.
     * @return
     */
    @Override
    public Optional<ByteBuffer> getShadowThing(String thingName) {
        return execute("SELECT arn, state FROM documents WHERE name = ?", preparedStatement -> {
            preparedStatement.setString(1, thingName);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return Optional.ofNullable(ByteBuffer.wrap(resultSet.getBytes("state")));
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
