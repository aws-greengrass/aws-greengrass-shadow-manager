/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager;

import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.shadowmanager.exception.ShadowManagerDataException;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;
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
            + ";WRITE_DELAY=0" // maximum delay between commit and flushing the log
            ;
    private final JdbcDataSource dataSource;

    @Getter
    private JdbcConnectionPool pool;

    private boolean closed = true;
    private static final Logger logger = LogManager.getLogger(ShadowManagerDatabase.class);
    private final Path databasePath;
    @Getter
    private final ExecutorService dbWriteThreadPool = Executors.newCachedThreadPool();

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
     * @throws ShadowManagerDataException if flyway migration fails
     */
    @Synchronized
    public void install() throws ShadowManagerDataException {
        try {
            boolean isMigrationSuccessful = migrateAndGetResult();
            if (!isMigrationSuccessful) {
                logger.atWarn().log("Failed to migrate the existing shadow manager DB. "
                        + "Removing it and creating a new one.");
                deleteDB(databasePath);
                migrateDB();
            }
        } catch (FlywayException | IOException e) {
            throw new ShadowManagerDataException(e);
        }
    }

    private void migrateDB() {
        Flyway flyway = Flyway.configure(getClass().getClassLoader())
                .locations("db/migration")
                .dataSource(dataSource)
                .load();
        flyway.migrate();
    }

    private boolean migrateAndGetResult() {
        try {
            migrateDB();
            return true;
        } catch (FlywaySqlException flywaySqlException) {
            if (flywaySqlException.getCause() instanceof JdbcSQLNonTransientException
                    && flywaySqlException.getCause().getCause() instanceof IllegalStateException) {
                logger.atError().cause(flywaySqlException).log("Shadow manager DB is corrupted.");
                return false;
            }
            throw flywaySqlException;
        }
    }

    private void deleteDB(Path databasePath) throws IOException {
        try (Stream<Path> workPathFiles = Files.list(databasePath)) {
            workPathFiles.filter(path -> path.toString().endsWith("db"))
            .forEach(path -> {
                try {
                    logger.atDebug().kv("file", path).log("Deleting db file");
                    Files.deleteIfExists(path);
                } catch (IOException e) {
                    throw new ShadowManagerDataException(e);
                }
            });
        }
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
            dbWriteThreadPool.shutdown();
            pool.dispose();
            closed = true;
        }
    }
}
