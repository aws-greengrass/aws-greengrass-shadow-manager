/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager;

import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.shadowmanager.db.DiskCapacityMonitor;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.Getter;
import lombok.Synchronized;
import org.flywaydb.core.Flyway;
import org.h2.jdbcx.JdbcDataSource;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import javax.inject.Inject;
import javax.inject.Singleton;

import static com.aws.greengrass.shadowmanager.ShadowManager.SERVICE_NAME;

/**
 * Connection manager for the local shadow documents.
 */
@Singleton
public class ShadowManagerDatabase implements Closeable {
    // Configurable?
    private static final String DATABASE_FORMAT = "jdbc:h2:%s/shadow";
    private final JdbcDataSource dataSource;
    private Connection connection;
    @Getter
    private final DiskCapacityMonitor capacityMonitor;

    /**
     * Creates a database with a {@link javax.sql.DataSource} using the kernel config.
     *
     * @param kernel Kernel config for the database manager.
     * @param executorService executor service for managing threads/tasks.
     */
    @Inject
    public ShadowManagerDatabase(final Kernel kernel, final ExecutorService executorService) {
        this.dataSource = new JdbcDataSource();
        Path path = kernel.getNucleusPaths().workPath().resolve(SERVICE_NAME);
        this.dataSource.setURL(String.format(DATABASE_FORMAT, path));
        this.capacityMonitor = new DiskCapacityMonitor(executorService, path);
    }

    @Synchronized
    public Connection connection() throws SQLException {
        return connection;
    }

    /**
     * Performs the database installation. This includes any migrations that needs to be performed.
     *
     * @throws SQLException When a connection to the local db fails for any reason.
     */
    @Synchronized
    public void install() throws SQLException {
        if (Objects.isNull(connection)) {
            connection = dataSource.getConnection();
        }
        Flyway flyway = Flyway.configure(getClass().getClassLoader())
                .locations("db/migration").dataSource(dataSource).load();
        flyway.migrate();
        capacityMonitor.start();
    }

    @Override
    @SuppressFBWarnings(value = "UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR", justification = "We check for null.")
    @Synchronized
    public void close() throws IOException {
        capacityMonitor.stop();
        if (Objects.nonNull(connection)) {
            try {
                connection.close();
            } catch (SQLException e) {
                throw new IOException(e);
            }
        }
    }

    /**
     * Sets the max disk utilization.
     *
     * @param maxDiskUtilization the max disk utilization in bytes.
     */
    public void setMaxDiskUtilization(int maxDiskUtilization) {
        this.capacityMonitor.setMaxDiskUsage(maxDiskUtilization);
    }
}
