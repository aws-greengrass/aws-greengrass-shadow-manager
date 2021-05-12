/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests;

import com.aws.greengrass.dependency.State;
import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.shadowmanager.ShadowManagerDAOImpl;
import com.aws.greengrass.shadowmanager.ShadowManagerDatabase;
import com.aws.greengrass.shadowmanager.model.ShadowDocument;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith({MockitoExtension.class, GGExtension.class})
@SuppressWarnings("PMD.CloseResource")
class ShadowManagerDatabaseTest extends NucleusLaunchUtils {
    @BeforeEach
    void setup() {
        kernel = new Kernel();
    }

    @AfterEach
    void cleanup() {
        kernel.shutdown();
    }

    private ShadowManagerDatabase initializeShadowManagerDatabase() throws InterruptedException, SQLException {
        startNucleusWithConfig("config.yaml", State.RUNNING, false);
        ShadowManagerDatabase shadowManagerDatabase = new ShadowManagerDatabase(kernel);
        shadowManagerDatabase.install();
        return shadowManagerDatabase;
    }

    @Test
    void GIVEN_data_WHEN_restart_THEN_data_still_exists() throws Exception {
        ShadowManagerDatabase shadowManagerDatabase = initializeShadowManagerDatabase();
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
        shadowManagerDatabase = initializeShadowManagerDatabase();

        // THEN
        dao = new ShadowManagerDAOImpl(shadowManagerDatabase);
        data = dao.getShadowThing("foo", "bar");
        assertThat(data.isPresent(), is(true));
        assertThat(data.get().toJson(true), is(new ShadowDocument(doc).toJson(true)));
    }

    @Test
    void GIVEN_nucleus_WHEN_install_THEN_shadow_manager_database_installs_and_starts_successfully() throws Exception {
        ShadowManagerDatabase shadowManagerDatabase = initializeShadowManagerDatabase();
        assertNotNull(shadowManagerDatabase.connection());

        List<String> tables = loadTables(shadowManagerDatabase.connection());
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
    void GIVEN_shadow_manager_database_connected_WHEN_install_again_THEN_shadow_manager_database_connection_does_not_change() throws Exception {
        ShadowManagerDatabase shadowManagerDatabase = initializeShadowManagerDatabase();
        Connection connection = shadowManagerDatabase.connection();
        assertNotNull(connection);
        shadowManagerDatabase.install();
        assertEquals(shadowManagerDatabase.connection(), connection);
    }

    @Test
    void GIVEN_shadow_manager_database_connected_WHEN_close_THEN_shadow_manager_database_connection_closes_successfully() throws Exception {
        ShadowManagerDatabase shadowManagerDatabase = initializeShadowManagerDatabase();
        shadowManagerDatabase.close();
        assertTrue(shadowManagerDatabase.connection().isClosed());
    }

    @Test
    void GIVEN_shadow_manager_database_not_connected_WHEN_close_THEN_shadow_manager_database_connection_does_nothing() throws Exception {
        startNucleusWithConfig("config.yaml", State.RUNNING, false);
        ShadowManagerDatabase shadowManagerDatabase = new ShadowManagerDatabase(kernel);
        assertNull(shadowManagerDatabase.connection());
        shadowManagerDatabase.close();
        assertNull(shadowManagerDatabase.connection());
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
