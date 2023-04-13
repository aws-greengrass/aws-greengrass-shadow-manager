/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager;

import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.Getter;
import lombok.Synchronized;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.internal.exception.FlywaySqlException;
import org.h2.jdbc.JdbcSQLNonTransientException;
import org.h2.jdbcx.JdbcConnectionPool;
import org.h2.jdbcx.JdbcDataSource;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.inject.Inject;
import javax.inject.Singleton;

import static com.aws.greengrass.shadowmanager.ShadowManager.SERVICE_NAME;

/**
 * Connection manager for the local shadow documents.
 */
@Singleton
public class ShadowManagerDatabase implements Closeable {
    private static final String DATABASE_NAME = "shadow";
    // see https://www.h2database.com/javadoc/org/h2/engine/DbSettings.html
    // these setting optimize for minimal disk space over concurrent performance
    private static final String DATABASE_FORMAT = "jdbc:h2:%s/%s"
            + ";RETENTION_TIME=1000" // ms - time to keep values for before writing to disk (default is 45000)
            + ";DEFRAG_ALWAYS=TRUE" // defragment db on shutdown (ensures only a single value in db on close)
            + ";COMPRESS=TRUE" // compress large objects (clob/blob) (default false)
            ;
    private final JdbcDataSource dataSource;

    @Getter
    private JdbcConnectionPool pool;

    private boolean closed = true;
    private static final Logger logger = LogManager.getLogger(ShadowManagerDatabase.class);
    private final Path databasePath;

    /**
     * Creates a database with a {@link javax.sql.DataSource} using the kernel config.
     *
     * @param kernel Kernel config for the database manager.
     */
    @Inject
    public ShadowManagerDatabase(final Kernel kernel) {
        this(kernel.getNucleusPaths().workPath().resolve(SERVICE_NAME));
    }

    /**
     * Create a new instance at the specified path.
     * @param path a path to store the db.
     */
    public ShadowManagerDatabase(Path path) {
        this.dataSource = new JdbcDataSource();
        this.dataSource.setURL(String.format(DATABASE_FORMAT, path, DATABASE_NAME));
        this.databasePath = path;
    }

    /**
     * Performs the database installation. This includes any migrations that needs to be performed.
     *
     * @throws IOException     io exception
     * @throws FlywayException if an error occurs migrating the database.
     */
    @Synchronized
    public void install() throws IOException, FlywayException {
        Flyway flyway = Flyway.configure(getClass().getClassLoader())
                .locations("db/migration")
                .dataSource(dataSource)
                .load();
        migrateDB(flyway);
    }

    private void migrateDB(Flyway flyway) throws IOException, FlywayException {
        try {
            flyway.migrate();
        } catch (FlywaySqlException flywaySqlException) {
            if (flywaySqlException.getCause() instanceof JdbcSQLNonTransientException
                    && flywaySqlException.getCause().getCause() instanceof IllegalStateException) {
                logger.atWarn().cause(flywaySqlException).log("Shadow manager DB is corrupted. "
                        + "Removing it and creating a new one.");
                recreateDB(flyway);
            } else {
                throw flywaySqlException;
            }
        }
    }

    private void recreateDB(Flyway flyway) throws IOException {
        logger.atDebug().kv("database-path", databasePath.toString()).log("Deleting the existing DB");
        Files.list(databasePath).forEach((path -> {
            if (path.endsWith("db")) {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }));
        migrateDB(flyway);
    }

    /**
     * Open the database to allow connections.
     */
    @Synchronized
    public void open() {
        if (closed) {
            // defaults to 10 connections with a 30s timeout waiting for a connection
            pool = JdbcConnectionPool.create(dataSource);
            closed = false;
        }
    }

    @Override
    @Synchronized
    @SuppressFBWarnings(value = "UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR", justification = "Field gated by flag")
    public void close() throws IOException {
        if (!closed) {
            pool.dispose();
            closed = true;
        }
    }
}
