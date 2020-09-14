package com.aws.iot.greengrass.shadowmanager;

import com.aws.iot.evergreen.kernel.Kernel;
import org.flywaydb.core.Flyway;
import org.h2.jdbcx.JdbcDataSource;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import javax.inject.Inject;
import javax.inject.Singleton;

import static com.aws.iot.greengrass.shadowmanager.ShadowManager.SERVICE_NAME;

/**
 * Connection manager for the local shadow documents.
 */
@Singleton
public class ShadowManagerDatabase implements Closeable {
    // Configurable?
    private static final String DATABASE_FORMAT = "jdbc:h2:%s/shadow";
    private final JdbcDataSource dataSource;
    private Connection connection;

    /**
     * Creates a database with a {@link javax.sql.DataSource} using the kernel config.
     * @param kernel Kernel config for the database manager.
     */
    @Inject
    public ShadowManagerDatabase(final Kernel kernel) {
        this.dataSource = new JdbcDataSource();
        this.dataSource.setURL(String.format(DATABASE_FORMAT, kernel.getWorkPath().resolve(SERVICE_NAME)));
    }

    public Connection connection() {
        return connection;
    }

    /**
     * Performs the database installation. This includes any migrations that needs to be performed.
     * @throws SQLException When a connection to the local db fails for any reason.
     */
    public synchronized void install() throws SQLException {
        if (Objects.isNull(connection)) {
            connection = dataSource.getConnection();
        }
        Flyway flyway = Flyway.configure()
                .locations("db/migration")
                .dataSource(dataSource)
                .load();
        flyway.migrate();
    }

    @Override
    public void close() throws IOException {
        if (Objects.nonNull(connection)) {
            try {
                connection.close();
            } catch (SQLException e) {
                throw new IOException(e);
            }
        }
    }
}
