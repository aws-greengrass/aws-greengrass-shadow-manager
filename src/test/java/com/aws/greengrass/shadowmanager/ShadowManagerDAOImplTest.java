/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager;

import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.inject.Inject;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith({MockitoExtension.class, GGExtension.class})
public class ShadowManagerDAOImplTest {

    private static final String THING_NAME = "testThing";
    private static final String SHADOW_NAME = "testShadow";
    private static final String NO_SHADOW_NAME = "";
    private static final String MISSING_THING_NAME = "missingTestThing";
    private static final byte[] BASE_DOCUMENT = "{\"id\": 1, \"name\": \"The Beatles\"}".getBytes();
    private static final byte[] NO_SHADOW_NAME_BASE_DOCUMENT = "{\"id\": 2, \"name\": \"The Beach Boys\"}".getBytes();
    private static final byte[] UPDATED_DOCUMENT = "{\"id\": 1, \"name\": \"New Name\"}".getBytes();
    private static final List<String> SHADOW_NAME_LIST = Arrays.asList("alpha", "bravo", "charlie", "delta");
    private static final int DEFAULT_OFFSET = 0;
    private static final int DEFAULT_LIMIT = 100;


    @TempDir
    Path rootDir;

    private Kernel kernel;
    private ShadowManagerDatabase database;
    private ShadowManagerDAOImpl dao;

    @Inject
    public ShadowManagerDAOImplTest() {
    }

    @BeforeEach
    public void before() throws SQLException {
        kernel = new Kernel();
        // Might need to start the kernel here
        kernel.parseArgs("-r", rootDir.toAbsolutePath().toString());

        database = new ShadowManagerDatabase(kernel);
        database.install();
        dao = new ShadowManagerDAOImpl(database);
    }

    @AfterEach
    void cleanup() throws IOException {
        database.close();
        kernel.shutdown();
    }

    @Test
    void testCreateShadowThing() throws Exception {
        Optional<byte[]> result = dao.createShadowThing(THING_NAME, SHADOW_NAME, BASE_DOCUMENT);
        assertTrue(result.isPresent());
        assertArrayEquals(BASE_DOCUMENT, result.get());
    }

    @Test
    void testCreateShadowThingWithNoShadowName() throws Exception {
        Optional<byte[]> result = dao.createShadowThing(THING_NAME, NO_SHADOW_NAME, NO_SHADOW_NAME_BASE_DOCUMENT);
        assertTrue(result.isPresent());
        assertArrayEquals(NO_SHADOW_NAME_BASE_DOCUMENT, result.get());
    }

    @Test
    void testGetShadowThing() throws Exception {
        testCreateShadowThing();
        Optional<byte[]> result = dao.getShadowThing(THING_NAME, SHADOW_NAME); // NOPMD
        assertTrue(result.isPresent());
        assertArrayEquals(BASE_DOCUMENT, result.get());
    }

    @Test
    void testGetShadowThingWithNoShadowName() throws Exception {
        testCreateShadowThing();
        testCreateShadowThingWithNoShadowName();
        Optional<byte[]> result = dao.getShadowThing(THING_NAME, NO_SHADOW_NAME); // NOPMD
        assertTrue(result.isPresent());
        assertArrayEquals(NO_SHADOW_NAME_BASE_DOCUMENT, result.get());
    }

    @Test
    void testGetShadowThingWithNoMatchingThing() throws Exception {
        Optional<byte[]> result = dao.getShadowThing(MISSING_THING_NAME, SHADOW_NAME);
        assertFalse(result.isPresent());
    }

    @Test
    void testDeleteShadowThing() throws Exception {
        testCreateShadowThing();
        Optional<byte[]> result = dao.deleteShadowThing(THING_NAME, SHADOW_NAME); //NOPMD
        assertTrue(result.isPresent());
        assertArrayEquals(BASE_DOCUMENT, result.get());
    }

    @Test
    void testDeleteShadowThingWithNoShadowName() throws Exception {
        testCreateShadowThing();
        testCreateShadowThingWithNoShadowName();

        // check that deleted object was the one without a shadow name
        Optional<byte[]> result = dao.deleteShadowThing(THING_NAME, NO_SHADOW_NAME); // NOPMD
        assertTrue(result.isPresent());
        assertArrayEquals(NO_SHADOW_NAME_BASE_DOCUMENT, result.get());

        // check that the original object still exists
        result = dao.getShadowThing(THING_NAME, SHADOW_NAME);
        assertTrue(result.isPresent());
    }

    @Test
    void testDeleteShadowThingWithNoMatchingThing() throws Exception {
        Optional<byte[]> result = dao.deleteShadowThing(THING_NAME, SHADOW_NAME);
        assertFalse(result.isPresent());
    }

    @Test
    void testUpdateShadowThing() throws Exception {
        testCreateShadowThing();
        Optional<byte[]> result = dao.updateShadowThing(THING_NAME, SHADOW_NAME, UPDATED_DOCUMENT); //NOPMD
        assertTrue(result.isPresent());
        assertArrayEquals(UPDATED_DOCUMENT, result.get());

        // Verify we can get the new document
        result = dao.getShadowThing(THING_NAME, SHADOW_NAME);
        assertTrue(result.isPresent());
        assertArrayEquals(UPDATED_DOCUMENT, result.get());
    }

    @Test
    void testUpdateShadowThingWithNoShadowName() throws Exception {
        testCreateShadowThing();
        testCreateShadowThingWithNoShadowName();
        Optional<byte[]> result = dao.updateShadowThing(THING_NAME, NO_SHADOW_NAME, UPDATED_DOCUMENT); //NOPMD
        assertTrue(result.isPresent());
        assertArrayEquals(UPDATED_DOCUMENT, result.get());

        // Verify we can get the new document
        result = dao.getShadowThing(THING_NAME, NO_SHADOW_NAME);
        assertTrue(result.isPresent());
        assertArrayEquals(UPDATED_DOCUMENT, result.get());

        // Verify that the original shadow with shadowName has not been updated
        result = dao.getShadowThing(THING_NAME, SHADOW_NAME);
        assertTrue(result.isPresent());
        assertArrayEquals(BASE_DOCUMENT, result.get());
    }

    @Test
    void testUpdateShadowThingWithNoMatchingThing() throws Exception {
        Optional<byte[]> result = dao.updateShadowThing(THING_NAME, SHADOW_NAME, UPDATED_DOCUMENT);
        assertTrue(result.isPresent());
        assertArrayEquals(UPDATED_DOCUMENT, result.get());
    }

    @Test
    void GIVEN_multiple_named_shadows_for_thing_WHEN_list_named_shadows_for_thing_THEN_return_named_shadow_list() throws Exception {
        for (String shadowName : SHADOW_NAME_LIST) {
            dao.updateShadowThing(THING_NAME, shadowName, UPDATED_DOCUMENT);
        }

        Optional<List<String>> listShadowResults = dao.listNamedShadowsForThing(THING_NAME, DEFAULT_OFFSET, DEFAULT_LIMIT);
        assertThat("has named shadow results", listShadowResults.isPresent(), is(true));
        assertThat(listShadowResults.get(), is(equalTo(SHADOW_NAME_LIST)));
    }

    @Test
    void GIVEN_offset_and_limit_WHEN_list_named_shadows_for_thing_THEN_return_named_shadow_subset() throws Exception {
        for (String shadowName : SHADOW_NAME_LIST) {
            dao.updateShadowThing(THING_NAME, shadowName, UPDATED_DOCUMENT);
        }

        int offset = 1;
        int limit = 2;
        Optional<List<String>> listShadowResults = dao.listNamedShadowsForThing(THING_NAME, offset, limit);
        List<String> expected_paginated_list = Arrays.asList("bravo", "charlie");
        assertThat("has named shadow results", listShadowResults.isPresent(), is(true));
        assertThat(listShadowResults.get(), is(equalTo(expected_paginated_list)));
    }

    @ParameterizedTest
    @CsvSource({
            "testThing, 0, 5",   // limit greater than number of named shadows
            "testThing, 0, 2",   // limit is less than number of named shadows
            "testThing, 0, -10", // limit is negative
            "testThing, 4, 5",   // offset is equal to or greater than number of named shadows
            "testThing, -10, 5", // offset is negative
            "missingTestThing, 0, 5", // list for thing that does not exist
            "classicThing, 0, 5"      // list for thing that does not have named shadows
    })
    void GIVEN_valid_edge_inputs_WHEN_list_named_shadows_for_thing_THEN_return_valid_results(String thingName, String offsetString, String pageSizeString) throws Exception {
        for (String shadowName : SHADOW_NAME_LIST) {
            dao.updateShadowThing(THING_NAME, shadowName, UPDATED_DOCUMENT);
        }

        final String CLASSIC_SHADOW_THING = "classicThing";
        dao.updateShadowThing(CLASSIC_SHADOW_THING, NO_SHADOW_NAME, UPDATED_DOCUMENT);

        int offset = Integer.parseInt(offsetString);
        int pageSize = Integer.parseInt(pageSizeString);

        Optional<List<String>> listShadowResults = dao.listNamedShadowsForThing(thingName, offset, pageSize);
        assertThat("has valid named shadow results", listShadowResults.isPresent(), is(true));

        // cases where valid results are empty (missing thing, thing with no named shadows, offset greater/equal to number of named shadows)
        if (thingName.equals(MISSING_THING_NAME)
                || thingName.equals(CLASSIC_SHADOW_THING)
                || offset >= SHADOW_NAME_LIST.size()) {
            assertThat(Collections.emptyList(), is(equalTo(listShadowResults.get())));
        }

        // cases where offset and limit are ignored (offset/limit are negative)
        if (offset < 0 || pageSize < 0) {
            assertThat("Original results remained the same", SHADOW_NAME_LIST, is(equalTo(listShadowResults.get())));
        }
    }
}