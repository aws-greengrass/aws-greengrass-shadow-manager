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
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
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
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.runAsync;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@ExtendWith({MockitoExtension.class, GGExtension.class})
@SuppressWarnings("PMD.CloseResource")
class ShadowManagerDatabaseTest extends NucleusLaunchUtils {
    ShadowManagerDatabase db;

    @BeforeEach
    void initializeShadowManagerDatabase() {
        System.setProperty("aws.greengrass.scanSelfClasspath", "true");
        db = new ShadowManagerDatabase(rootDir);
        db.install();
        db.open();
    }

    @AfterEach
    void close() throws IOException {
        db.close();
        FileUtils.deleteDirectory(rootDir.toFile());
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

    static Stream<Arguments> shadowUpdateArgs() {
        return Stream.of(
                arguments("updates across 5 thing with 5 shadows", 5, 50000, 1536, 128, 5, 5),
                arguments("updates across 50 thing with 2 shadows", 5, 50000, 3072, 256, 50, 2),
                arguments("updates across 500 thing with 1 shadow", 5, 50000, 20*1024, 1536, 500, 1));
    }

    @ParameterizedTest
    @MethodSource("shadowUpdateArgs")
    void GIVEN_db_open_WHEN_thousands_of_shadow_updates_THEN_db_size_remains_under_limit(String description,
            int numThreads, int numUpdates, long maxSizeKb, long afterCloseSizeKb, int numThings, int numShadows) throws Exception {
        ExecutorService s = Executors.newCachedThreadPool();

        ShadowManagerDAOImpl dao = new ShadowManagerDAOImpl(db);

        // ensure update rate is "reasonably" large to force db to have a lot of writes but still has time to do I/O
        RateLimiter limit = RateLimiter.create(2000);

        AtomicInteger updates = new AtomicInteger(0);
        AtomicReference<Throwable> thrown = new AtomicReference<>();

        final String[] things = new String[numThings];
        for (int i = 0; i < numThings; i++) {
            things[i] = "thing-"+i;
        }
        final String[] shadows = new String[numShadows];
        for (int i = 0; i < numShadows; i++) {
            shadows[i] = "shadow-"+i;
        }

        List<Long> sizes = Collections.synchronizedList(new ArrayList<>());

        final int checkCount = numUpdates / 10;
        Runnable create = () -> {
            Random r = new Random();
            byte[] bytes = new byte[1024];
            try {
                while (true) {
                    for (int shadowUpdate = 0; shadowUpdate < numShadows; shadowUpdate++) {
                        int updateCount = updates.getAndIncrement();
                        if (updateCount >= numUpdates) {
                            return;
                        }

                        if (thrown.get() != null) {
                            return;
                        }
                        r.nextBytes(bytes);
                        // pick a random thing so we don't have sequential updates
                        int thingIndex = r.nextInt(numThings);
                        int shadowIndex = r.nextInt(numShadows);

                        byte[] docBytes = getDocBytes(things[thingIndex], shadowIndex, bytes);
                        dao.updateShadowThing(things[thingIndex],
                                shadows[shadowIndex],
                                docBytes,
                                updateCount);
                        limit.acquire();

                        if (updateCount % checkCount == 0) {
                            sizes.add(size(rootDir));
                        }
                    }
                }
            } catch (Throwable t) { // NOPMD: Catching in runnable so we can rethrow later
                thrown.compareAndSet(null, t);
            }

        };

        try {
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (int i = 0; i < numThreads; i++) {
                futures.add(runAsync(create, s));
            }
            allOf(futures.toArray(new CompletableFuture[0])).join();

            Throwable t = thrown.get();
            if (t != null) {
                fail(description, t);
            }
            db.close();
            TimeUnit.SECONDS.sleep(5); // wait for compaction
            long lastSize = size(rootDir);
            sizes.forEach(size -> assertThat(sizes.toString(), size, is(lessThanOrEqualTo(maxSizeKb * 1024L))));
            assertThat(lastSize, is(lessThanOrEqualTo(afterCloseSizeKb * 1024L)));
        } finally {
            s.shutdownNow();
        }
    }

    private byte[] getDocBytes(String thingName, int shadowIndex, byte[] bytes) {
        String random = new String(Base64.getEncoder().encode(bytes), StandardCharsets.UTF_8);
        String s= "{\"version\": 101, "
                + "\"state\":\"desired\":"
                + "{\"foo\":\"bar\""
                + ",\"status\": \"on\""
                + ",\"random\":\"" + random + "\""
                + ",\"nested\": {"
                + ",\"uuid1\":\"" + UUID.randomUUID().toString() + "\""
                + ",\"uuid2\":\"" + UUID.randomUUID().toString() + "\""
                + ",\"uuid3\":\"" + UUID.randomUUID().toString() + "\""
                + ",\"thingDetails\": {"
                + ",\"thing\":\"" + thingName + "\""
                + ",\"shadow\":" + shadowIndex
                + "}}}}";
        return s.getBytes(StandardCharsets.UTF_8);
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
