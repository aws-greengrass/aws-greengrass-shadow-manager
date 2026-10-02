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
import org.h2.api.ErrorCode;
import org.h2.jdbcx.JdbcConnectionPool;
import org.h2.jdbcx.JdbcDataSource;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
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
            ;
    private final JdbcDataSource dataSource;

    private JdbcConnectionPool pool;

    private static final Logger logger = LogManager.getLogger(ShadowManagerDatabase.class);
    // H2 error codes indicating the local database is corrupted or otherwise unusable (vs. an ordinary
    // SQL error). GENERAL_ERROR_1 covers MVStore-internal failures, including the chunk-id wraparound.
    private static final Set<Integer> CORRUPTION_ERROR_CODES = new HashSet<>(Arrays.asList(
            ErrorCode.GENERAL_ERROR_1,
            ErrorCode.FILE_CORRUPTED_1,
            ErrorCode.IO_EXCEPTION_1,
            ErrorCode.IO_EXCEPTION_2,
            ErrorCode.FILE_VERSION_ERROR_1));
    private final Path databasePath;
    @Getter
    private boolean initialized = false;

    /**
     * Whether the failure indicates the local shadow database is corrupted/unusable and must be recreated.
     * Scans the cause chain for an H2 {@link SQLException} carrying a corruption or I/O error code (which
     * covers the MVStore chunk-id wraparound, reported as {@link ErrorCode#GENERAL_ERROR_1}). Narrow by
     * design: ordinary SQL errors (syntax, constraint, etc.) carry other codes and are not corruption.
     *
     * @param failure the failure observed while accessing the shadow database
     * @return true if the local database is corrupted and should be recreated
     */
    public static boolean isDatabaseCorrupted(Throwable failure) {
        for (Throwable t = failure; t != null; t = t.getCause()) {
            if (t instanceof SQLException && CORRUPTION_ERROR_CODES.contains(((SQLException) t).getErrorCode())) {
                return true;
            }
        }
        return false;
    }

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
        if (initializeAndVerify()) {
            initialized = true;
        } else {
            logger.atWarn().log("Failed to migrate the existing shadow manager DB. "
                    + "Removing it and creating a new one.");
            rebuild();
        }
    }

    /**
     * Delete and recreate the local shadow database in place: dispose the connection pool, delete the
     * on-disk db files, and re-run the Flyway migration to create an empty schema. Used to recover from
     * an unusable database (e.g. the H2 MVStore chunk-id wraparound). Orchestration (when to call this,
     * off which thread, and any follow-up resync) is the caller's responsibility.
     *
     * @throws ShadowManagerDataException if the database could not be rebuilt
     */
    @Synchronized
    public void rebuild() throws ShadowManagerDataException {
        try {
            initialized = false;
            close();
            deleteDB(databasePath);
            migrateDB();
            initialized = true;
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

    private boolean initializeAndVerify() {
        try {
            migrateDB();
        } catch (FlywaySqlException flywaySqlException) {
            if (isDatabaseCorrupted(flywaySqlException)) {
                logger.atError().cause(flywaySqlException).log("Shadow manager DB is corrupted");
                return false;
            }
            throw flywaySqlException;
        }

        // Validate that after migration we're actually able to open and connect to the DB.
        // A DB we cannot open/checkpoint is unusable, so recreate it.
        try {
            try (Connection p = getPool().getConnection(); Statement st = p.createStatement()) {
                st.execute("SELECT 1");
                st.execute("CHECKPOINT");
            }
            return true;
        } catch (SQLException e) {
            logger.atError().cause(e).log("Shadow manager DB could not be opened; deleting and recreating it");
            close();
            return false;
        }
    }

    /**
     * Get a reference to the connection pool.
     * @return JDBC connection pool
     */
    public synchronized JdbcConnectionPool getPool() {
        if (pool == null) {
            pool = JdbcConnectionPool.create(dataSource);
        }
        return pool;
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

    @Override
    @Synchronized
    @SuppressWarnings("PMD.NullAssignment")
    @SuppressFBWarnings(value = "UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR", justification = "Field gated by flag")
    public void close() {
        if (pool != null) {
            pool.dispose();
            pool = null;
        }
    }
}
