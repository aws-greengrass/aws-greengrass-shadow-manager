package com.aws.greengrass.shadowmanager;

import com.aws.greengrass.dependency.State;
import com.aws.greengrass.lifecyclemanager.GlobalStateChangeListener;
import com.aws.greengrass.lifecyclemanager.GreengrassService;
import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.testcommons.testutilities.GGServiceTestUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith({MockitoExtension.class, GGExtension.class})
@SuppressWarnings("PMD.CloseResource")
class ShadowManagerDatabaseTest extends GGServiceTestUtil {
    private static final long TEST_TIME_OUT_SEC = 30L;

    private Kernel kernel;
    private GlobalStateChangeListener listener;

    @TempDir
    Path rootDir;

    @BeforeEach
    void setup() {
        kernel = new Kernel();
    }

    @AfterEach
    void cleanup() {
        kernel.shutdown();
    }

    private void startNucleusWithConfig(String configFile, State expectedState) throws InterruptedException {
        CountDownLatch shadowManagerRunning = new CountDownLatch(1);
        kernel.parseArgs("-r", rootDir.toAbsolutePath().toString(), "-i",
                getClass().getResource(configFile).toString());
        listener = (GreengrassService service, State was, State newState) -> {
            if (service.getName().equals(ShadowManager.SERVICE_NAME) && service.getState().equals(expectedState)) {
                shadowManagerRunning.countDown();
            }
        };
        kernel.getContext().addGlobalStateChangeListener(listener);
        kernel.launch();

        assertTrue(shadowManagerRunning.await(TEST_TIME_OUT_SEC, TimeUnit.SECONDS));
    }

    private ShadowManagerDatabase initializeShadowManagerDatabase() throws InterruptedException, SQLException {
        startNucleusWithConfig("config.yaml", State.RUNNING);
        ShadowManagerDatabase shadowManagerDatabase = new ShadowManagerDatabase(kernel);
        shadowManagerDatabase.install();
        return shadowManagerDatabase;
    }

    @Test
    void GIVEN_nucleus_WHEN_install_THEN_shadow_manager_database_installs_and_starts_successfully() throws Exception {
        ShadowManagerDatabase shadowManagerDatabase = initializeShadowManagerDatabase();
        assertNotNull(shadowManagerDatabase.connection());

        List<String> tables = loadTables(shadowManagerDatabase.connection());
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
        startNucleusWithConfig("config.yaml", State.RUNNING);
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
