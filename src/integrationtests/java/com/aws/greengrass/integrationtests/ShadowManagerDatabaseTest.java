/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests;

import com.aws.greengrass.dependency.State;
import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.shadowmanager.ShadowManagerDAOImpl;
import com.aws.greengrass.shadowmanager.ShadowManagerDatabase;
import com.aws.greengrass.shadowmanager.model.ShadowDocument;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import vendored.com.google.common.util.concurrent.RateLimiter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.runAsync;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith({MockitoExtension.class, GGExtension.class})
@SuppressWarnings("PMD.CloseResource")
class ShadowManagerDatabaseTest extends NucleusLaunchUtils {
    private static final Long MAX_DB_SIZE_BYTES = 1536 * 1024L; // 1.5 MiB

    ShadowManagerDatabase db;

    @BeforeEach
    void initializeShadowManagerDatabase() {
        db = new ShadowManagerDatabase(rootDir);
        db.install();
        db.open();
    }

    @AfterEach
    void close() throws IOException {
        db.close();
    }

    @Test
    void GIVEN_data_WHEN_restart_THEN_data_still_exists() throws Exception {
        kernel = new Kernel();
        try {
            startNucleusWithConfig("config.yaml", State.RUNNING, false);
            ShadowManagerDatabase shadowManagerDatabase = kernel.getContext().get(ShadowManagerDatabase.class);
            ShadowManagerDAOImpl dao = new ShadowManagerDAOImpl(shadowManagerDatabase);

            // GIVEN
            byte[] doc = "{\"version\": 1, \"state\": {\"reported\": {\"name\": \"The Beatles\"}}}".getBytes(StandardCharsets.UTF_8);
            dao.updateShadowThing("foo", "bar", doc, 1);
            Optional<ShadowDocument> data = dao.getShadowThing("foo", "bar");

            assertThat(data.isPresent(), is(true));
            assertThat(data.get().toJson(true), is(new ShadowDocument(doc).toJson(true)));

            // WHEN
            kernel.shutdown();

            kernel = new Kernel();
            startNucleusWithConfig("config.yaml", State.RUNNING, false);

            shadowManagerDatabase = kernel.getContext().get(ShadowManagerDatabase.class);
            dao = new ShadowManagerDAOImpl(shadowManagerDatabase);

            // THEN
            data = dao.getShadowThing("foo", "bar");
            assertThat(data.isPresent(), is(true));
            assertThat(data.get().toJson(true), is(new ShadowDocument(doc).toJson(true)));
        } finally {
            kernel.shutdown();
        }
    }

    @Test
    void GIVEN_migrations_WHEN_install_THEN_shadow_manager_database_installs_and_starts_successfully() throws Exception {
        // GIVEN
        db.open();
        assertNotNull(db.getPool());

        // WHEN
        db.install();

        // THEN
        List<String> tables = loadTables(db.getPool().getConnection());
        // flyway installed
        assertThat(tables, hasItem(equalToIgnoringCase("flyway_schema_history")));

        // expected tables
        assertThat(tables, hasItems(equalToIgnoringCase("documents"),
                equalToIgnoringCase("sync")));

        // tables loaded by migrations provided as test resources
        assertThat(tables, hasItems(equalToIgnoringCase("foo"),
                equalToIgnoringCase("baz")));

        // table removed from migration
        assertThat(tables, not(hasItem(equalToIgnoringCase("bar"))));
    }

    @Test
    void GIVEN_shadow_manager_database_connected_WHEN_close_THEN_shadow_manager_database_connection_closes_successfully() throws Exception {
        assertNotNull(db.getPool());
        Connection c = assertDoesNotThrow(() -> db.getPool().getConnection());
        assertNotNull(c, "connection should not be null");
        assertThat("connection is not closed", c.isClosed(), is(false));
        c.close();
        db.close();
        assertThat("active connections", db.getPool().getActiveConnections(), is(0));
        assertThrows(IllegalStateException.class, () -> db.getPool().getConnection());
    }

    @Test
    void GIVEN_shadow_manager_database_open_WHEN_closed_and_opened_THEN_shadow_manager_database_can_return_connections() throws Exception {
        db.close();
        db.open();
        Connection c = assertDoesNotThrow(() -> db.getPool().getConnection());
        assertNotNull(c, "connection should not be null");
        assertThat("connection is not closed", c.isClosed(), is(false));
        c.close();
    }

    @Test
    void GIVEN_db_open_WHEN_thousands_of_shadow_updates_THEN_db_size_remains_under_limit() throws Exception {
        ExecutorService s = Executors.newCachedThreadPool();

        ShadowManagerDAOImpl dao = new ShadowManagerDAOImpl(db);

        // ensure update rate is "reasonably" large to force db to have a lot of writes but still has time to do I/O
        RateLimiter limit = RateLimiter.create(2000);

        size(rootDir);
        AtomicInteger x = new AtomicInteger(0);
        AtomicReference<Throwable> thrown = new AtomicReference<>();

        // spread 10_000 random updates over 5 shadows for a thing
        Function<String, Runnable> create = (thingName) -> () -> {
            Random r = new Random();
            byte[] bytes = new byte[1024];
            try {
                for (int i = 1; i <= 2000; i++) {
                    for (int shadowIndex = 0; shadowIndex < 5; shadowIndex++) {
                        if (thrown.get() != null) {
                            return;
                        }
                        r.nextBytes(bytes);
                        dao.updateShadowThing(thingName, "shadow" + shadowIndex, bytes, i);
                        limit.acquire();

                        int count = x.incrementAndGet();
                        if (count % 5000 == 0) {
                            assertThat(size(rootDir), is(lessThanOrEqualTo(MAX_DB_SIZE_BYTES)));
                        }
                    }
                }
            } catch (Throwable t) { // NOPMD: Catching in runnable so we can rethrow later
                thrown.compareAndSet(null, t);
            }

        };

        // start 5 threads
        try {
            allOf(runAsync(create.apply("foo"), s),
                    runAsync(create.apply("bar"), s),
                    runAsync(create.apply("baz"), s),
                    runAsync(create.apply("alpha"), s),
                    runAsync(create.apply("beta"), s)).join();

            Throwable t = thrown.get();
            if (t != null) {
                fail(t);
            }
            db.close();
            TimeUnit.SECONDS.sleep(5); // wait for compaction
            assertThat(size(rootDir), is(lessThan(100 * 1024L)));
        } finally {
            s.shutdownNow();
        }
    }

    private long size(Path p) throws IOException {
        AtomicLong size = new AtomicLong(0);
        Files.walkFileTree(p, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (Files.isRegularFile(file)) {
                    size.addAndGet(Files.size(file));
                }
                return super.visitFile(file, attrs);
            }
        });
        LogManager.getLogger("db").atDebug().kv("size (kb)", size.get() / 1024.0).log();
        return size.get();
    }

    static List<String> loadTables(Connection connection) throws SQLException {
        ResultSet rs = connection.createStatement().executeQuery("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES");
        List<String> tables = new ArrayList<>();
        while (rs.next()) {
            tables.add(rs.getString("TABLE_NAME"));
        }
        return tables;
    }
}
