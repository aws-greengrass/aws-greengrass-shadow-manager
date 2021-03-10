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
import org.mockito.junit.jupiter.MockitoExtension;

import javax.inject.Inject;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Array;
import java.sql.SQLException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith({MockitoExtension.class, GGExtension.class})
public class ShadowManagerDAOImplTest {

    private static final String THING_NAME = "testThing";
    private static final String SHADOW_NAME = "testShadow";
    private static final String NO_SHADOW_NAME = "";
    private static final String MISSING_THING_NAME = "missingTestThing";
    private static final byte[] BASE_DOCUMENT =  "{\"id\": 1, \"name\": \"The Beatles\"}".getBytes();
    private static final byte[] NO_SHADOW_NAME_BASE_DOCUMENT =  "{\"id\": 2, \"name\": \"The Beach Boys\"}".getBytes();
    private static final byte[] UPDATED_DOCUMENT =  "{\"id\": 1, \"name\": \"New Name\"}".getBytes();
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
    void testListNamedShadowsForThing() throws Exception {
        for (String shadowName : SHADOW_NAME_LIST) {
            System.out.println(shadowName);
            dao.updateShadowThing(THING_NAME, shadowName, UPDATED_DOCUMENT);
        }

        Optional<List<String>> listShadowResults = dao.listNamedShadowsForThing(THING_NAME, DEFAULT_OFFSET, DEFAULT_LIMIT);
        assertTrue(listShadowResults.isPresent());
        assertEquals(SHADOW_NAME_LIST, listShadowResults.get());
    }

    @Test
    void testListNamedShadowsForThingWithOffsetAndLimit() throws Exception {
        for (String shadowName : SHADOW_NAME_LIST) {
            System.out.println(shadowName);
            dao.updateShadowThing(THING_NAME, shadowName, UPDATED_DOCUMENT);
        }

        int offset = 1;
        int limit = 2;
        Optional<List<String>> listShadowResults = dao.listNamedShadowsForThing(THING_NAME, offset, limit);
        List<String> expected_paginated_list = Arrays.asList("bravo", "charlie");
        assertTrue(listShadowResults.isPresent());
        assertEquals(expected_paginated_list, listShadowResults.get());
    }

    @Test
    void testListNamedShadowsForThingWithNoShadowNames() throws Exception {
        Optional<List<String>> listShadowResults = dao.listNamedShadowsForThing(THING_NAME, DEFAULT_OFFSET, DEFAULT_LIMIT);
        assertTrue(listShadowResults.isPresent());
        assertTrue(listShadowResults.get().isEmpty());
    }

}