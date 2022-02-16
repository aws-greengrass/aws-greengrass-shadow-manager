/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests;

import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.shadowmanager.ShadowManagerDAOImpl;
import com.aws.greengrass.shadowmanager.ShadowManagerDatabase;
import com.aws.greengrass.shadowmanager.model.ShadowDocument;
import com.aws.greengrass.shadowmanager.model.dao.SyncInformation;
import com.aws.greengrass.shadowmanager.util.JsonUtil;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import javax.inject.Inject;

import static com.aws.greengrass.shadowmanager.TestUtils.SHADOW_NAME;
import static com.aws.greengrass.shadowmanager.TestUtils.THING_NAME;
import static com.aws.greengrass.shadowmanager.model.Constants.CLASSIC_SHADOW_IDENTIFIER;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class ShadowManagerDAOImplTest {

    private static final String MISSING_THING_NAME = "missingTestThing";
    private static final String CLASSIC_SHADOW_THING = "classicThing";
    private static final byte[] BASE_DOCUMENT = "{\"version\": 1, \"state\": {\"reported\": {\"name\": \"The Beatles\"}}}".getBytes();
    private static final byte[] NO_SHADOW_NAME_BASE_DOCUMENT = "{\"version\": 1, \"state\": {\"reported\": {\"name\": \"The Beach Boys\"}}}".getBytes();
    private static final byte[] UPDATED_DOCUMENT = "{\"version\": 1, \"state\": {\"reported\": {\"name\": \"New Name\"}}}".getBytes();
    private static final List<String> SHADOW_NAME_LIST = Arrays.asList("alpha", "bravo", "charlie", "delta");
    private static final int DEFAULT_OFFSET = 0;
    private static final int DEFAULT_LIMIT = 25;


    @TempDir
    Path rootDir;

    private Kernel kernel;
    private ShadowManagerDatabase database;
    private ShadowManagerDAOImpl dao;

    @Inject
    public ShadowManagerDAOImplTest() {
    }

    @BeforeEach
    public void before() throws IOException {
        JsonUtil.loadSchema();
        kernel = new Kernel();
        // Might need to start the Nucleus here
        kernel.parseArgs("-r", rootDir.toAbsolutePath().toString());

        database = new ShadowManagerDatabase(kernel);
        database.install();
        JsonUtil.loadSchema();
        database.open();
        dao = new ShadowManagerDAOImpl(database);
    }

    @AfterEach
    void cleanup() throws IOException {
        database.close();
        kernel.shutdown();
    }

    @SuppressWarnings("PMD.UnusedPrivateMethod")
    private static Stream<Arguments> classicAndNamedShadow() {
        return Stream.of(
                arguments(SHADOW_NAME, BASE_DOCUMENT),
                arguments(CLASSIC_SHADOW_IDENTIFIER, NO_SHADOW_NAME_BASE_DOCUMENT)
        );
    }

    private void createNamedShadow() {
        Optional<byte[]> result = dao.updateShadowThing(THING_NAME, SHADOW_NAME, BASE_DOCUMENT, 1);
        assertThat("Created named shadow", result.isPresent(), is(true));
        assertThat(result.get(), is(equalTo(BASE_DOCUMENT)));
    }

    private void createClassicShadow() {
        Optional<byte[]> result = dao.updateShadowThing(THING_NAME, CLASSIC_SHADOW_IDENTIFIER, NO_SHADOW_NAME_BASE_DOCUMENT, 1);
        assertThat("Created classic shadow", result.isPresent(), is(true));
        assertThat(result.get(), is(equalTo(NO_SHADOW_NAME_BASE_DOCUMENT)));
    }

    @ParameterizedTest
    @MethodSource("classicAndNamedShadow")
    void GIVEN_named_and_classic_shadow_WHEN_get_shadow_thing_THEN_return_correct_payload(String shadowName, byte[] expectedPayload) throws Exception {
        createNamedShadow();
        createClassicShadow();
        Optional<ShadowDocument> result = dao.getShadowThing(THING_NAME, shadowName); // NOPMD
        assertThat("Retrieved shadow", result.isPresent(), is(true));
        assertThat(result.get().toJson(true), is(equalTo(new ShadowDocument(expectedPayload).toJson(true))));
    }

    @Test
    void GIVEN_no_shadow_WHEN_get_shadow_thing_THEN_return_nothing() {
        Optional<ShadowDocument> result = dao.getShadowThing(MISSING_THING_NAME, SHADOW_NAME);
        assertThat("No shadow found", result.isPresent(), is(false));
    }

    @ParameterizedTest
    @MethodSource("classicAndNamedShadow")
    void GIVEN_named_and_classic_shadow_WHEN_delete_shadow_thing_THEN_shadow_deleted(String shadowName, byte[] expectedPayload) throws Exception {
        createNamedShadow();
        createClassicShadow();
        Optional<ShadowDocument> result = dao.deleteShadowThing(THING_NAME, shadowName); //NOPMD
        assertThat("Correct payload returned", result.isPresent(), is(true));
        assertThat(result.get().toJson(true), is(equalTo(new ShadowDocument(expectedPayload).toJson(true))));

        Optional<ShadowDocument> getResults = dao.getShadowThing(THING_NAME, shadowName);
        assertThat("Should not get the shadow document.", getResults.isPresent(), is(false));
    }

    @Test
    void GIVEN_no_shadow_WHEN_delete_shadow_thing_THEN_return_nothing() {
        Optional<ShadowDocument> result = dao.deleteShadowThing(THING_NAME, SHADOW_NAME);
        assertThat("No shadow found", result.isPresent(), is(false));
    }

    @SuppressWarnings("PMD.UnusedPrivateMethod")
    private static Stream<Arguments> namedShadowUpdateSupport() {
        return Stream.of(
                arguments(SHADOW_NAME, CLASSIC_SHADOW_IDENTIFIER, NO_SHADOW_NAME_BASE_DOCUMENT),
                arguments(CLASSIC_SHADOW_IDENTIFIER, SHADOW_NAME, BASE_DOCUMENT)
        );
    }

    @ParameterizedTest
    @MethodSource("namedShadowUpdateSupport")
    void GIVEN_named_and_classic_shadow_WHEN_update_shadow_thing_THEN_correct_shadow_updated(String shadowName, String ignoredShadowName, byte[] ignoredPayload) throws Exception {
        createNamedShadow();
        createClassicShadow();
        Optional<byte[]> result = dao.updateShadowThing(THING_NAME, shadowName, UPDATED_DOCUMENT, 1); //NOPMD
        assertThat("Updated shadow", result.isPresent(), is(true));
        assertThat(result.get(), is(equalTo(UPDATED_DOCUMENT)));

        // Verify we can get the new document
        Optional<ShadowDocument> shadowDocument = dao.getShadowThing(THING_NAME, shadowName);
        assertThat("Can GET updated shadow", shadowDocument.isPresent(), is(true));
        assertThat(shadowDocument.get().toJson(true), is(equalTo(new ShadowDocument(UPDATED_DOCUMENT).toJson(true))));

        // Verify that the original shadow with shadowName has not been updated
        shadowDocument = dao.getShadowThing(THING_NAME, ignoredShadowName);
        assertThat("Can GET other shadow that was not updated", shadowDocument.isPresent(), is(true));
        assertThat("Other shadow not updated", shadowDocument.get().toJson(true), is(equalTo(new ShadowDocument(ignoredPayload).toJson(true))));
    }

    @Test
    void GIVEN_no_shadow_WHEN_update_shadow_thing_THEN_shadow_created() {
        Optional<byte[]> result = dao.updateShadowThing(THING_NAME, SHADOW_NAME, UPDATED_DOCUMENT, 1);
        assertThat("Shadow created", result.isPresent(), is(true));
        assertThat(result.get(), is(equalTo(UPDATED_DOCUMENT)));
    }

    @Test
    void GIVEN_multiple_named_shadows_for_thing_WHEN_list_named_shadows_for_thing_THEN_return_named_shadow_list() {
        for (String shadowName : SHADOW_NAME_LIST) {
            dao.updateShadowThing(THING_NAME, shadowName, UPDATED_DOCUMENT, 1);
        }

        List<String> listShadowResults = dao.listNamedShadowsForThing(THING_NAME, DEFAULT_OFFSET, DEFAULT_LIMIT);
        assertThat(listShadowResults, is(notNullValue()));
        assertThat(listShadowResults, is(not(empty())));
        assertThat(listShadowResults, is(equalTo(SHADOW_NAME_LIST)));
    }

    @Test
    void GIVEN_classic_and_named_shadows_WHEN_list_named_shadows_for_thing_THEN_return_list_does_not_include_classic_shadow() {
        for (String shadowName : SHADOW_NAME_LIST) {
            dao.updateShadowThing(THING_NAME, shadowName, UPDATED_DOCUMENT, 1);
        }
        dao.updateShadowThing(THING_NAME, CLASSIC_SHADOW_IDENTIFIER, UPDATED_DOCUMENT, 1);

        List<String> listShadowResults = dao.listNamedShadowsForThing(THING_NAME, DEFAULT_OFFSET, SHADOW_NAME_LIST.size());
        assertThat(listShadowResults, is(notNullValue()));
        assertThat(listShadowResults, is(not(empty())));
        assertThat(listShadowResults, is(equalTo(SHADOW_NAME_LIST)));
        assertThat(listShadowResults.size(), is(equalTo(SHADOW_NAME_LIST.size())));
    }

    @Test
    void GIVEN_offset_and_limit_WHEN_list_named_shadows_for_thing_THEN_return_named_shadow_subset() {
        for (String shadowName : SHADOW_NAME_LIST) {
            dao.updateShadowThing(THING_NAME, shadowName, UPDATED_DOCUMENT, 1);
        }

        int offset = 1;
        int limit = 2;
        List<String> listShadowResults = dao.listNamedShadowsForThing(THING_NAME, offset, limit);
        List<String> expected_paginated_list = Arrays.asList("bravo", "charlie");
        assertThat(listShadowResults, is(notNullValue()));
        assertThat(listShadowResults, is(not(empty())));
        assertThat(listShadowResults, is(equalTo(expected_paginated_list)));
    }

    @Test
    void GIVEN_one_deleted_shadow_WHEN_list_named_shadows_for_thing_THEN_return_non_deleted_named_shadow() throws IOException {
        for (String shadowName : SHADOW_NAME_LIST) {
            dao.updateShadowThing(THING_NAME, shadowName, UPDATED_DOCUMENT, 1);
        }

        Optional<ShadowDocument> result = dao.deleteShadowThing(THING_NAME, "charlie"); //NOPMD
        assertThat("Correct payload returned", result.isPresent(), is(true));
        assertThat(result.get().toJson(true), is(equalTo(new ShadowDocument(UPDATED_DOCUMENT).toJson(true))));

        List<String> listShadowResults = dao.listNamedShadowsForThing(THING_NAME, DEFAULT_OFFSET, SHADOW_NAME_LIST.size());
        List<String> expected_list = Arrays.asList("alpha", "bravo", "delta");
        assertThat(listShadowResults, is(notNullValue()));
        assertThat(listShadowResults, is(not(empty())));
        assertThat(listShadowResults, is(equalTo(expected_list)));
    }

    static Stream<Arguments> validListTestParameters() {
        return Stream.of(
                arguments(THING_NAME, 0, 5),   // limit greater than number of named shadows
                arguments(THING_NAME, 0, 2),   // limit is less than number of named shadows
                arguments(THING_NAME, 0, -10), // limit is negative
                arguments(THING_NAME, 4, 5),   // offset is equal to or greater than number of named shadows
                arguments(THING_NAME, -10, 5), // offset is negative
                arguments(MISSING_THING_NAME, 0, 5),  // list for thing that does not exist
                arguments(CLASSIC_SHADOW_THING, 0, 5) // list for thing that does not have named shadows
        );
    }

    @ParameterizedTest
    @MethodSource("validListTestParameters")
    void GIVEN_valid_edge_inputs_WHEN_list_named_shadows_for_thing_THEN_return_valid_results(String thingName, int offset, int pageSize) {
        for (String shadowName : SHADOW_NAME_LIST) {
            dao.updateShadowThing(THING_NAME, shadowName, UPDATED_DOCUMENT, 1);
        }

        dao.updateShadowThing(CLASSIC_SHADOW_THING, CLASSIC_SHADOW_IDENTIFIER, UPDATED_DOCUMENT, 1);

        List<String> listShadowResults = dao.listNamedShadowsForThing(thingName, offset, pageSize);
        assertThat(listShadowResults, is(notNullValue()));

        // cases where valid results are empty (missing thing, thing with no named shadows, offset greater/equal to number of named shadows)
        if (thingName.equals(MISSING_THING_NAME)
                || thingName.equals(CLASSIC_SHADOW_THING)
                || offset >= SHADOW_NAME_LIST.size()) {
            assertThat(listShadowResults, is(empty()));
        }

        // cases where offset and limit are ignored (offset/limit are negative)
        if (offset < 0 || pageSize < 0) {
            assertThat("Original results remained the same", listShadowResults, is(equalTo(SHADOW_NAME_LIST)));
        }
    }

    static Stream<Arguments> validUpdateSyncInformationTests() {
        return Stream.of(
                arguments(BASE_DOCUMENT, 1, false, 5),
                arguments(null, 2, true, 6),
                arguments(UPDATED_DOCUMENT, 3, false, 7 )
        );
    }

    @ParameterizedTest
    @MethodSource("validUpdateSyncInformationTests")
    void GIVEN_valid_sync_information_WHEN_update_and_get_THEN_successfully_updates_and_gets_sync_information(byte[] lastSyncedDocument, long cloudVersion, boolean cloudDeleted, long localVersion) {
        long epochMinus60Seconds = Instant.now().minusSeconds(60).getEpochSecond();
        SyncInformation syncInformation = SyncInformation.builder()
                .thingName(THING_NAME)
                .shadowName(SHADOW_NAME)
                .cloudDeleted(cloudDeleted)
                .cloudVersion(cloudVersion)
                .cloudUpdateTime(epochMinus60Seconds)
                .lastSyncedDocument(lastSyncedDocument)
                .localVersion(localVersion)
                .build();
        assertTrue(dao.updateSyncInformation(syncInformation));

        Optional<SyncInformation> shadowSyncInformation = dao.getShadowSyncInformation(THING_NAME, SHADOW_NAME);
        assertThat(shadowSyncInformation, is(not(Optional.empty())));
        assertThat(shadowSyncInformation.get(), is(syncInformation));
    }

    @ParameterizedTest
    @MethodSource("validUpdateSyncInformationTests")
    void GIVEN_valid_sync_information_WHEN_update_delete_and_get_THEN_successfully_updates_and_deletes_sync_information(byte[] lastSyncedDocument, long cloudVersion, boolean cloudDeleted, long localVersion) {
        long epochMinus60Seconds = Instant.now().minusSeconds(60).getEpochSecond();
        SyncInformation syncInformation = SyncInformation.builder()
                .thingName(THING_NAME)
                .shadowName(SHADOW_NAME)
                .cloudDeleted(cloudDeleted)
                .cloudVersion(cloudVersion)
                .cloudUpdateTime(epochMinus60Seconds)
                .lastSyncedDocument(lastSyncedDocument)
                .localVersion(localVersion)
                .build();
        assertTrue(dao.updateSyncInformation(syncInformation));

        assertTrue(dao.deleteSyncInformation(THING_NAME, SHADOW_NAME));

        Optional<SyncInformation> shadowSyncInformation = dao.getShadowSyncInformation(THING_NAME, SHADOW_NAME);
        assertThat(shadowSyncInformation, is(Optional.empty()));


        assertFalse(dao.deleteSyncInformation(THING_NAME, SHADOW_NAME));
    }

    @Test
    void GIVEN_non_existent_sync_info_WHEN_insert_THEN_successfully_inserts_sync_information() {
        long epochSeconds = Instant.EPOCH.getEpochSecond();
        SyncInformation syncInformation = SyncInformation.builder()
                .thingName(THING_NAME)
                .shadowName(SHADOW_NAME)
                .cloudDeleted(false)
                .cloudVersion(0)
                .cloudUpdateTime(epochSeconds)
                .lastSyncedDocument(null)
                .localVersion(0)
                .lastSyncTime(epochSeconds)
                .build();
        assertTrue(dao.insertSyncInfoIfNotExists(syncInformation));

        Optional<SyncInformation> shadowSyncInformation = dao.getShadowSyncInformation(THING_NAME, SHADOW_NAME);
        assertThat(shadowSyncInformation, is(not(Optional.empty())));
        assertThat(shadowSyncInformation.get(), is(syncInformation));
    }

    @Test
    void GIVEN_existent_sync_info_WHEN_insert_THEN_does_not_insert_new_sync_information() {
        long epochSeconds = Instant.EPOCH.getEpochSecond();
        long epochMinus60Seconds = Instant.now().minusSeconds(60).getEpochSecond();
        SyncInformation initialSyncInformation = SyncInformation.builder()
                .thingName(THING_NAME)
                .shadowName(SHADOW_NAME)
                .cloudDeleted(false)
                .cloudVersion(10)
                .cloudUpdateTime(epochMinus60Seconds)
                .lastSyncedDocument(BASE_DOCUMENT)
                .localVersion(20)
                .lastSyncTime(epochMinus60Seconds)
                .build();

        SyncInformation syncInformationToUpdate = SyncInformation.builder()
                .thingName(THING_NAME)
                .shadowName(SHADOW_NAME)
                .cloudDeleted(false)
                .cloudVersion(0)
                .cloudUpdateTime(epochSeconds)
                .lastSyncedDocument(null)
                .localVersion(0)
                .lastSyncTime(epochSeconds)
                .build();

        assertTrue(dao.insertSyncInfoIfNotExists(initialSyncInformation));

        Optional<SyncInformation> shadowSyncInformation = dao.getShadowSyncInformation(THING_NAME, SHADOW_NAME);
        assertThat(shadowSyncInformation, is(not(Optional.empty())));
        assertThat(shadowSyncInformation.get(), is(initialSyncInformation));

        assertFalse(dao.insertSyncInfoIfNotExists(syncInformationToUpdate));

        shadowSyncInformation = dao.getShadowSyncInformation(THING_NAME, SHADOW_NAME);
        assertThat(shadowSyncInformation, is(not(Optional.empty())));
        assertThat(shadowSyncInformation.get(), is(initialSyncInformation));
    }

    @ParameterizedTest
    @MethodSource("classicAndNamedShadow")
    void GIVEN_named_and_classic_shadow_WHEN_delete_shadow_and_get_deleted_version_THEN_gets_shadow_deleted_version(String shadowName, byte[] expectedPayload) throws Exception {
        createNamedShadow();
        createClassicShadow();

        // Before deleting the shadow, we should not get the shadow version.
        Optional<Long> deletedShadowVersion = dao.getDeletedShadowVersion(THING_NAME, shadowName);
        assertThat(deletedShadowVersion, is(Optional.empty()));

        Optional<ShadowDocument> result = dao.deleteShadowThing(THING_NAME, shadowName); //NOPMD
        assertThat("Correct payload returned", result.isPresent(), is(true));
        assertThat(result.get().toJson(true), is(equalTo(new ShadowDocument(expectedPayload).toJson(true))));

        Optional<ShadowDocument> getResults = dao.getShadowThing(THING_NAME, shadowName);
        assertThat("Should not get the shadow document.", getResults.isPresent(), is(false));

        // After the shadow has been deleted, we should get the correct deleted shadow version.
        deletedShadowVersion = dao.getDeletedShadowVersion(THING_NAME, shadowName);
        assertThat(deletedShadowVersion, is(not(Optional.empty())));
        assertThat(deletedShadowVersion.get(), is(2L));
    }

}
