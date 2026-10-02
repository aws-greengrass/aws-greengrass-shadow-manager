/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests;

import com.aws.greengrass.shadowmanager.ShadowManagerDAOImpl;
import com.aws.greengrass.shadowmanager.ShadowManagerDatabase;
import com.aws.greengrass.shadowmanager.model.ShadowDocument;
import com.aws.greengrass.shadowmanager.util.JsonUtil;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * Integration tests for the runtime self-heal rebuild of the local shadow database, exercised against a
 * real H2 database (not mocks).
 */
@ExtendWith({MockitoExtension.class, GGExtension.class})
class ShadowManagerSelfHealTest {
    private static final String THING = "thing";
    private static final String SHADOW = "shadow";
    private static final byte[] DOC =
            "{\"version\": 1, \"state\": {\"reported\": {\"name\": \"x\"}}}".getBytes(StandardCharsets.UTF_8);

    @TempDir
    Path rootDir;

    private ShadowManagerDatabase database;

    @BeforeEach
    void before() throws IOException {
        JsonUtil.loadSchema();
        database = new ShadowManagerDatabase(rootDir);
        database.install();
    }

    @AfterEach
    void after() throws IOException {
        database.close();
        FileUtils.deleteDirectory(rootDir.toFile());
    }

    @Test
    void GIVEN_populated_database_WHEN_rebuild_THEN_schema_is_recreated_empty_and_usable() {
        ShadowManagerDAOImpl dao = new ShadowManagerDAOImpl(database);
        dao.updateShadowThing(THING, SHADOW, DOC, 1);
        assertThat(dao.getShadowThing(THING, SHADOW).isPresent(), is(true));

        // WHEN the local database is rebuilt
        assertDoesNotThrow(database::rebuild);

        // THEN the database is usable again and the previous (local-only) shadow is gone
        assertThat(database.isInitialized(), is(true));
        Optional<ShadowDocument> afterRebuild = dao.getShadowThing(THING, SHADOW);
        assertThat(afterRebuild.isPresent(), is(false));

        // AND the recreated database accepts new writes
        dao.updateShadowThing(THING, SHADOW, DOC, 1);
        assertThat(dao.getShadowThing(THING, SHADOW).isPresent(), is(true));
    }

    @Test
    void GIVEN_rebuilt_database_WHEN_reads_and_writes_THEN_no_error() {
        ShadowManagerDAOImpl dao = new ShadowManagerDAOImpl(database);

        // rebuilding an empty database is safe and leaves it usable
        assertDoesNotThrow(database::rebuild);
        assertThat(database.isInitialized(), is(true));
        assertDoesNotThrow(() -> dao.getShadowThing(THING, SHADOW));
    }
}
