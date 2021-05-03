/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager;

import com.aws.greengrass.shadowmanager.exception.ShadowManagerDataException;
import com.aws.greengrass.shadowmanager.model.ShadowDocument;
import com.aws.greengrass.shadowmanager.model.dao.SyncInformation;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.util.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class ShadowManagerDAOImplTest {
    private static final byte[] BASE_DOCUMENT = "{\"version\": 1, \"state\": {\"reported\": {\"name\": \"The Beatles\"}}}".getBytes();
    private static final String THING_NAME = "thingName";
    private static final String SHADOW_NAME = "shadowName";

    @Mock
    private ShadowManagerDatabase mockDatabase;

    @Mock
    private Connection mockConnection;

    @Mock
    private PreparedStatement mockPreparedStatement;

    @Mock
    private ResultSet mockResultSet;

    @Captor
    private ArgumentCaptor<String> stringArgumentCaptor;
    @Captor
    private ArgumentCaptor<Boolean> booleanArgumentCaptor;
    @Captor
    private ArgumentCaptor<Long> longArgumentCaptor;
    @Captor
    private ArgumentCaptor<Integer> integerArgumentCaptor;
    @Captor
    private ArgumentCaptor<byte[]> bytesArgumentCaptor;

    private void assertUpdateShadowStatementMocks(long epochNow) {
        assertThat(stringArgumentCaptor.getAllValues().size(), is(2));
        assertThat(longArgumentCaptor.getAllValues().size(), is(2));
        assertThat(bytesArgumentCaptor.getValue(), is(notNullValue()));
        assertThat(booleanArgumentCaptor.getValue(), is(notNullValue()));

        assertThat(stringArgumentCaptor.getAllValues().get(0), is(THING_NAME));
        assertThat(stringArgumentCaptor.getAllValues().get(1), is(SHADOW_NAME));

        assertThat(longArgumentCaptor.getAllValues().get(0), is(1L));
        assertThat(longArgumentCaptor.getAllValues().get(1), is(greaterThanOrEqualTo(epochNow)));

        assertThat(bytesArgumentCaptor.getValue(), is(BASE_DOCUMENT));
        assertThat(booleanArgumentCaptor.getValue(), is(false));
    }

    private void setupUpdateShadowStatementMocks() throws SQLException {
        doNothing().when(mockPreparedStatement).setString(eq(1), stringArgumentCaptor.capture());
        doNothing().when(mockPreparedStatement).setString(eq(2), stringArgumentCaptor.capture());
        doNothing().when(mockPreparedStatement).setBytes(eq(3), bytesArgumentCaptor.capture());
        doNothing().when(mockPreparedStatement).setLong(eq(4), longArgumentCaptor.capture());
        doNothing().when(mockPreparedStatement).setBoolean(eq(5), booleanArgumentCaptor.capture());
        doNothing().when(mockPreparedStatement).setLong(eq(6), longArgumentCaptor.capture());
    }

    private void assertGetShadowStatementMocks() {
        assertThat(stringArgumentCaptor.getAllValues().size(), is(2));
        assertThat(stringArgumentCaptor.getAllValues().get(0), is(THING_NAME));
        assertThat(stringArgumentCaptor.getAllValues().get(1), is(SHADOW_NAME));
    }

    private void setupGetShadowStatementMocks() throws SQLException {
        doNothing().when(mockPreparedStatement).setString(eq(1), stringArgumentCaptor.capture());
        doNothing().when(mockPreparedStatement).setString(eq(2), stringArgumentCaptor.capture());
    }

    private void assertDeleteShadowStatementMocks(long epochNow) {
        assertThat(stringArgumentCaptor.getAllValues().size(), is(4));
        assertThat(longArgumentCaptor.getValue(), is(notNullValue()));

        assertThat(stringArgumentCaptor.getAllValues().get(0), is(THING_NAME));
        assertThat(stringArgumentCaptor.getAllValues().get(1), is(SHADOW_NAME));
        assertThat(stringArgumentCaptor.getAllValues().get(2), is(THING_NAME));
        assertThat(stringArgumentCaptor.getAllValues().get(3), is(SHADOW_NAME));

        assertThat(longArgumentCaptor.getValue(), is(greaterThanOrEqualTo(epochNow)));

    }

    private void setupDeleteShadowStatementMocks() throws SQLException {
        doNothing().when(mockPreparedStatement).setString(eq(1), stringArgumentCaptor.capture());
        doNothing().when(mockPreparedStatement).setString(eq(2), stringArgumentCaptor.capture());
        doNothing().when(mockPreparedStatement).setString(eq(3), stringArgumentCaptor.capture());
        doNothing().when(mockPreparedStatement).setLong(eq(1), longArgumentCaptor.capture());
    }

    private void assertListShadowStatementMocks() {
        assertThat(integerArgumentCaptor.getAllValues().size(), is(2));
        assertThat(stringArgumentCaptor.getValue(), is(notNullValue()));

        assertThat(stringArgumentCaptor.getValue(), is(THING_NAME));
        assertThat(integerArgumentCaptor.getAllValues().get(0), is(10));
        assertThat(integerArgumentCaptor.getAllValues().get(1), is(10));
    }

    private void setupListShadowStatementMocks() throws SQLException {
        doNothing().when(mockPreparedStatement).setString(eq(1), stringArgumentCaptor.capture());
        doNothing().when(mockPreparedStatement).setInt(eq(2), integerArgumentCaptor.capture());
        doNothing().when(mockPreparedStatement).setInt(eq(3), integerArgumentCaptor.capture());
    }

    private void assertDeleteShadowSyncMocks() {
        assertThat(stringArgumentCaptor.getAllValues().size(), is(2));
        assertThat(stringArgumentCaptor.getAllValues().get(0), is(THING_NAME));
        assertThat(stringArgumentCaptor.getAllValues().get(1), is(SHADOW_NAME));
    }

    private void setupDeleteShadowSyncMocks() throws SQLException {
        doNothing().when(mockPreparedStatement).setString(eq(1), stringArgumentCaptor.capture());
        doNothing().when(mockPreparedStatement).setString(eq(2), stringArgumentCaptor.capture());
    }

    private void setupUpdateShadowSyncStatementMocks() throws SQLException {
        doNothing().when(mockPreparedStatement).setString(eq(1), stringArgumentCaptor.capture());
        doNothing().when(mockPreparedStatement).setString(eq(2), stringArgumentCaptor.capture());
        doNothing().when(mockPreparedStatement).setBytes(eq(3), bytesArgumentCaptor.capture());
        doNothing().when(mockPreparedStatement).setLong(eq(4), longArgumentCaptor.capture());
        doNothing().when(mockPreparedStatement).setBoolean(eq(5), booleanArgumentCaptor.capture());
        doNothing().when(mockPreparedStatement).setLong(eq(6), longArgumentCaptor.capture());
        doNothing().when(mockPreparedStatement).setLong(eq(7), longArgumentCaptor.capture());
        doNothing().when(mockPreparedStatement).setLong(eq(8), longArgumentCaptor.capture());
    }

    private void assertUpdateShadowSyncStatementMocks(long epochNow) {
        assertThat(stringArgumentCaptor.getAllValues().size(), is(2));
        assertThat(longArgumentCaptor.getAllValues().size(), is(4));
        assertThat(bytesArgumentCaptor.getValue(), is(notNullValue()));
        assertThat(booleanArgumentCaptor.getValue(), is(notNullValue()));

        assertThat(bytesArgumentCaptor.getValue(), is(BASE_DOCUMENT));
        assertThat(booleanArgumentCaptor.getValue(), is(false));
        assertThat(stringArgumentCaptor.getAllValues().get(0), is(THING_NAME));
        assertThat(stringArgumentCaptor.getAllValues().get(1), is(SHADOW_NAME));
        assertThat(longArgumentCaptor.getAllValues().get(0), is(2L));
        assertThat(longArgumentCaptor.getAllValues().get(1), is(lessThanOrEqualTo(epochNow)));
        assertThat(longArgumentCaptor.getAllValues().get(2), is(greaterThanOrEqualTo(epochNow)));
        assertThat(longArgumentCaptor.getAllValues().get(3), is(5L));
    }

    private void setupInsertShadowSyncStatementMocks() throws SQLException {
        doNothing().when(mockPreparedStatement).setString(eq(1), stringArgumentCaptor.capture());
        doNothing().when(mockPreparedStatement).setString(eq(2), stringArgumentCaptor.capture());
        doNothing().when(mockPreparedStatement).setBytes(eq(3), bytesArgumentCaptor.capture());
        doNothing().when(mockPreparedStatement).setLong(eq(4), longArgumentCaptor.capture());
        doNothing().when(mockPreparedStatement).setBoolean(eq(5), booleanArgumentCaptor.capture());
        doNothing().when(mockPreparedStatement).setLong(eq(6), longArgumentCaptor.capture());
        doNothing().when(mockPreparedStatement).setLong(eq(7), longArgumentCaptor.capture());
        doNothing().when(mockPreparedStatement).setLong(eq(8), longArgumentCaptor.capture());
        doNothing().when(mockPreparedStatement).setString(eq(9), stringArgumentCaptor.capture());
        doNothing().when(mockPreparedStatement).setString(eq(10), stringArgumentCaptor.capture());
    }

    private void assertInsertShadowSyncStatementMocks() {
        assertThat(stringArgumentCaptor.getAllValues().size(), is(4));
        assertThat(longArgumentCaptor.getAllValues().size(), is(4));
        assertThat(bytesArgumentCaptor.getValue(), is(nullValue()));
        assertThat(booleanArgumentCaptor.getValue(), is(notNullValue()));

        assertThat(booleanArgumentCaptor.getValue(), is(false));
        assertThat(stringArgumentCaptor.getAllValues().get(0), is(THING_NAME));
        assertThat(stringArgumentCaptor.getAllValues().get(1), is(SHADOW_NAME));
        assertThat(stringArgumentCaptor.getAllValues().get(2), is(THING_NAME));
        assertThat(stringArgumentCaptor.getAllValues().get(3), is(SHADOW_NAME));
        assertThat(longArgumentCaptor.getAllValues().get(0), is(0L));
        assertThat(longArgumentCaptor.getAllValues().get(1), is(0L));
        assertThat(longArgumentCaptor.getAllValues().get(2), is(0L));
        assertThat(longArgumentCaptor.getAllValues().get(3), is(0L));
    }

    @BeforeEach
    void setup() throws SQLException {
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
        when(mockDatabase.connection()).thenReturn(mockConnection);
    }

    @Test
    void GIVEN_updated_shadow_document_WHEN_updateShadowThing_THEN_successfully_updates_shadow() throws SQLException {
        long epochNow = Instant.now().getEpochSecond();
        setupUpdateShadowStatementMocks();

        when(mockPreparedStatement.executeUpdate()).thenReturn(1);
        ShadowManagerDAOImpl impl = new ShadowManagerDAOImpl(mockDatabase);
        Optional<byte[]> updatedShadow = impl.updateShadowThing(THING_NAME, SHADOW_NAME, BASE_DOCUMENT, 1);
        assertThat(updatedShadow, is(notNullValue()));
        assertThat(updatedShadow.get(), is(BASE_DOCUMENT));

        assertUpdateShadowStatementMocks(epochNow);
    }

    @Test
    void GIVEN_updated_shadow_document_WHEN_updateShadowThing_and_h2_returns_0_rows_updated_THEN_does_not_return_shadow_document() throws SQLException {
        long epochNow = Instant.now().getEpochSecond();
        setupUpdateShadowStatementMocks();

        when(mockPreparedStatement.executeUpdate()).thenReturn(0);
        ShadowManagerDAOImpl impl = new ShadowManagerDAOImpl(mockDatabase);
        Optional<byte[]> updatedShadow = impl.updateShadowThing(THING_NAME, SHADOW_NAME, BASE_DOCUMENT, 1);
        assertThat(updatedShadow, is(Optional.empty()));

        assertUpdateShadowStatementMocks(epochNow);
    }

    @Test
    void GIVEN_updated_shadow_document_WHEN_updateShadowThing_and_h2_throws_SQL_exception_THEN_ShadowManagerDataException_is_thrown() throws SQLException {
        long epochNow = Instant.now().getEpochSecond();
        setupUpdateShadowStatementMocks();

        when(mockPreparedStatement.executeUpdate()).thenThrow(SQLException.class);
        ShadowManagerDAOImpl impl = new ShadowManagerDAOImpl(mockDatabase);
        assertThrows(ShadowManagerDataException.class, () -> impl.updateShadowThing(THING_NAME, SHADOW_NAME, BASE_DOCUMENT, 1));
        assertUpdateShadowStatementMocks(epochNow);
    }

    @Test
    void GIVEN_existing_shadow_WHEN_getShadowThing_THEN_returns_shadow_document() throws SQLException, IOException {
        setupGetShadowStatementMocks();
        when(mockResultSet.next()).thenReturn(true);
        when(mockResultSet.getBytes(1)).thenReturn(BASE_DOCUMENT);
        when(mockResultSet.getLong(2)).thenReturn(1L);
        when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
        ShadowManagerDAOImpl impl = new ShadowManagerDAOImpl(mockDatabase);
        Optional<ShadowDocument> updatedShadow = impl.getShadowThing(THING_NAME, SHADOW_NAME);
        assertThat(updatedShadow, is(notNullValue()));
        assertThat(updatedShadow.get().toJson(true), is(new ShadowDocument(BASE_DOCUMENT).toJson(true)));
        assertGetShadowStatementMocks();
    }

    @Test
    void GIVEN_no_existing_shadow_WHEN_getShadowThing_THEN_returns_empty_optional() throws SQLException {
        setupGetShadowStatementMocks();
        when(mockResultSet.next()).thenReturn(false);
        when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
        ShadowManagerDAOImpl impl = new ShadowManagerDAOImpl(mockDatabase);
        Optional<ShadowDocument> updatedShadow = impl.getShadowThing(THING_NAME, SHADOW_NAME);
        assertThat(updatedShadow, is(Optional.empty()));
        assertGetShadowStatementMocks();
    }

    @Test
    void GIVEN_existing_shadow_WHEN_getShadowThing_and_h2_throws_SQL_exception_THEN_ShadowManagerDataException_is_thrown() throws SQLException {
        setupGetShadowStatementMocks();

        when(mockPreparedStatement.executeQuery()).thenThrow(SQLException.class);
        ShadowManagerDAOImpl impl = new ShadowManagerDAOImpl(mockDatabase);
        assertThrows(ShadowManagerDataException.class, () -> impl.getShadowThing(THING_NAME, SHADOW_NAME));
        assertGetShadowStatementMocks();
    }

    @Test
    void GIVEN_existing_shadow_WHEN_deleteShadowThing_THEN_deletes_shadow_document() throws SQLException, IOException {
        long epochNow = Instant.now().getEpochSecond();
        setupDeleteShadowStatementMocks();
        when(mockResultSet.next()).thenReturn(true);
        when(mockResultSet.getBytes(1)).thenReturn(BASE_DOCUMENT);
        when(mockResultSet.getLong(2)).thenReturn(1L);
        when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
        when(mockPreparedStatement.executeUpdate()).thenReturn(1);
        ShadowManagerDAOImpl impl = new ShadowManagerDAOImpl(mockDatabase);
        Optional<ShadowDocument> updatedShadow = impl.deleteShadowThing(THING_NAME, SHADOW_NAME);
        assertThat(updatedShadow, is(notNullValue()));
        assertThat(updatedShadow.get().toJson(true), is(new ShadowDocument(BASE_DOCUMENT).toJson(true)));

        assertDeleteShadowStatementMocks(epochNow);
    }

    @Test
    void GIVEN_existing_shadow_WHEN_deleteShadowThing_and_h2_returns_0_rows_deleted_THEN_returns_empty_optional() throws SQLException {
        long epochNow = Instant.now().getEpochSecond();
        setupDeleteShadowStatementMocks();
        when(mockResultSet.next()).thenReturn(true);
        when(mockResultSet.getBytes(1)).thenReturn(BASE_DOCUMENT);
        when(mockResultSet.getLong(2)).thenReturn(1L);
        when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
        when(mockPreparedStatement.executeUpdate()).thenReturn(0);
        ShadowManagerDAOImpl impl = new ShadowManagerDAOImpl(mockDatabase);
        Optional<ShadowDocument> updatedShadow = impl.deleteShadowThing(THING_NAME, SHADOW_NAME);
        assertThat(updatedShadow, is(Optional.empty()));

        assertDeleteShadowStatementMocks(epochNow);
    }

    @Test
    void GIVEN_existing_shadow_WHEN_deleteShadowThing_and_h2_throws_SQL_exception_THEN_ShadowManagerDataException_is_thrown() throws SQLException {
        long epochNow = Instant.now().getEpochSecond();
        setupDeleteShadowStatementMocks();

        when(mockResultSet.next()).thenReturn(true);
        when(mockResultSet.getBytes(1)).thenReturn(BASE_DOCUMENT);
        when(mockResultSet.getLong(2)).thenReturn(1L);
        when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
        when(mockPreparedStatement.executeUpdate()).thenThrow(SQLException.class);
        ShadowManagerDAOImpl impl = new ShadowManagerDAOImpl(mockDatabase);
        assertThrows(ShadowManagerDataException.class, () -> impl.deleteShadowThing(THING_NAME, SHADOW_NAME));
        assertDeleteShadowStatementMocks(epochNow);
    }

    @Test
    void GIVEN_existing_shadows_for_thing_WHEN_listNamedShadowsForThing_THEN_returns_a_list_of_shadow_names() throws SQLException {
        setupListShadowStatementMocks();
        AtomicInteger count = new AtomicInteger(0);
        when(mockResultSet.next()).thenAnswer(invocationOnMock -> {
            count.set(count.get() + 1);
            return count.get() <= 10;
        });
        AtomicInteger currentValueCounter = new AtomicInteger(0);
        when(mockResultSet.getString(anyInt())).thenAnswer(invocationOnMock -> {
            currentValueCounter.set(currentValueCounter.get() + 1);
            return "SomeShadow-" + currentValueCounter.get();
        });
        when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
        ShadowManagerDAOImpl impl = new ShadowManagerDAOImpl(mockDatabase);
        List<String> results = impl.listNamedShadowsForThing(THING_NAME, 10, 10);

        assertThat(results.toArray(), is(arrayContaining("SomeShadow-1", "SomeShadow-2", "SomeShadow-3", "SomeShadow-4", "SomeShadow-5", "SomeShadow-6", "SomeShadow-7", "SomeShadow-8", "SomeShadow-9", "SomeShadow-10")));
        assertListShadowStatementMocks();
    }

    @Test
    void GIVEN_no_existing_shadows_for_thing_WHEN_listNamedShadowsForThing_THEN_returns_an_empty_list() throws SQLException {
        setupListShadowStatementMocks();
        when(mockResultSet.next()).thenReturn(false);
        when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
        ShadowManagerDAOImpl impl = new ShadowManagerDAOImpl(mockDatabase);
        List<String> results = impl.listNamedShadowsForThing(THING_NAME, 10, 10);

        assertThat(results.size(), is(0));
        assertListShadowStatementMocks();
    }


    @Test
    void GIVEN_existing_shadows_for_thing_WHEN_listNamedShadowsForThing_and_h2_throws_SQL_exception_THEN_ShadowManagerDataException_is_thrown() throws SQLException {
        setupListShadowStatementMocks();

        when(mockPreparedStatement.executeQuery()).thenThrow(SQLException.class);
        ShadowManagerDAOImpl impl = new ShadowManagerDAOImpl(mockDatabase);
        assertThrows(ShadowManagerDataException.class, () -> impl.listNamedShadowsForThing(THING_NAME, 10, 10));
        assertListShadowStatementMocks();
    }

    @Test
    void GIVEN_existing_sync_information_WHEN_updateSyncInformation_THEN_successfully_updates_sync_information() throws SQLException {
        long epochNow = Instant.now().getEpochSecond();
        setupUpdateShadowSyncStatementMocks();
        when(mockPreparedStatement.executeUpdate()).thenReturn(1);

        ShadowManagerDAOImpl impl = new ShadowManagerDAOImpl(mockDatabase);
        assertTrue(impl.updateSyncInformation(SyncInformation.builder()
                .thingName(THING_NAME)
                .shadowName(SHADOW_NAME)
                .cloudUpdateTime(Instant.now().minusSeconds(60).getEpochSecond())
                .cloudVersion(2)
                .lastSyncedDocument(BASE_DOCUMENT)
                .localVersion(5)
                .build()));

        assertUpdateShadowSyncStatementMocks(epochNow);
    }

    @Test
    void GIVEN_existing_sync_information_WHEN_updateSyncInformation_and_h2_returns_0_rows_updated_THEN_does_not_update_sync_information() throws SQLException {
        long epochNow = Instant.now().getEpochSecond();
        setupUpdateShadowSyncStatementMocks();
        when(mockPreparedStatement.executeUpdate()).thenReturn(0);

        ShadowManagerDAOImpl impl = new ShadowManagerDAOImpl(mockDatabase);
        assertFalse(impl.updateSyncInformation(SyncInformation.builder()
                .thingName(THING_NAME)
                .shadowName(SHADOW_NAME)
                .cloudUpdateTime(Instant.now().minusSeconds(60).getEpochSecond())
                .cloudVersion(2)
                .localVersion(5)
                .lastSyncedDocument(BASE_DOCUMENT)
                .build()));

        assertUpdateShadowSyncStatementMocks(epochNow);
    }

    @Test
    void GIVEN_existing_sync_information_WHEN_updateSyncInformation_and_h2_throws_SQL_exception_THEN_ShadowManagerDataException_is_thrown() throws SQLException {
        long epochNow = Instant.now().getEpochSecond();
        setupUpdateShadowSyncStatementMocks();

        when(mockPreparedStatement.executeUpdate()).thenThrow(SQLException.class);
        ShadowManagerDAOImpl impl = new ShadowManagerDAOImpl(mockDatabase);
        assertThrows(ShadowManagerDataException.class, () -> impl.updateSyncInformation(SyncInformation.builder()
                .thingName(THING_NAME)
                .shadowName(SHADOW_NAME)
                .cloudUpdateTime(Instant.now().minusSeconds(60).getEpochSecond())
                .cloudVersion(2)
                .lastSyncedDocument(BASE_DOCUMENT)
                .localVersion(5)
                .build()));
        assertUpdateShadowSyncStatementMocks(epochNow);
    }

    @Test
    void GIVEN_existing_sync_information_WHEN_getShadowSyncInformation_THEN_successfully_gets_sync_information() throws SQLException {
        setupGetShadowStatementMocks();
        long epochNow = Instant.now().getEpochSecond();
        long epochMinus60Seconds = Instant.now().minusSeconds(60).getEpochSecond();
        when(mockResultSet.getBytes(1)).thenReturn(BASE_DOCUMENT);
        when(mockResultSet.getLong(2)).thenReturn(2L);
        when(mockResultSet.getLong(3)).thenReturn(epochMinus60Seconds);
        when(mockResultSet.getLong(4)).thenReturn(epochNow);
        when(mockResultSet.getBoolean(5)).thenReturn(true);
        when(mockResultSet.getLong(6)).thenReturn(5L);
        when(mockResultSet.next()).thenReturn(true);
        when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);

        ShadowManagerDAOImpl impl = new ShadowManagerDAOImpl(mockDatabase);
        Optional<SyncInformation> shadowSyncInformation = impl.getShadowSyncInformation(THING_NAME, SHADOW_NAME);

        assertThat(shadowSyncInformation, is(notNullValue()));
        assertThat(shadowSyncInformation.get().getLastSyncedDocument(), is(BASE_DOCUMENT));
        assertThat(shadowSyncInformation.get().getCloudUpdateTime(), is(epochMinus60Seconds));
        assertThat(shadowSyncInformation.get().getLastSyncTime(), is(epochNow));
        assertThat(shadowSyncInformation.get().getCloudVersion(), is(2L));
        assertThat(shadowSyncInformation.get().getLocalVersion(), is(5L));
        assertThat(shadowSyncInformation.get().getShadowName(), is(SHADOW_NAME));
        assertThat(shadowSyncInformation.get().getThingName(), is(THING_NAME));
        assertGetShadowStatementMocks();
    }

    @Test
    void GIVEN_no_existing_sync_information_WHEN_getShadowSyncInformation_and_h2_returns_0_rows_updated_THEN_does_not_get_sync_information() throws SQLException {
        setupGetShadowStatementMocks();
        when(mockResultSet.next()).thenReturn(false);
        when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);

        ShadowManagerDAOImpl impl = new ShadowManagerDAOImpl(mockDatabase);
        Optional<SyncInformation> shadowSyncInformation = impl.getShadowSyncInformation(THING_NAME, SHADOW_NAME);

        assertThat(shadowSyncInformation, is(Optional.empty()));
        assertGetShadowStatementMocks();
    }

    @Test
    void GIVEN_existing_sync_information_WHEN_getShadowSyncInformation_and_h2_throws_SQL_exception_THEN_ShadowManagerDataException_is_thrown() throws SQLException {
        setupGetShadowStatementMocks();
        when(mockPreparedStatement.executeQuery()).thenThrow(SQLException.class);

        ShadowManagerDAOImpl impl = new ShadowManagerDAOImpl(mockDatabase);
        assertThrows(ShadowManagerDataException.class, () -> impl.getShadowSyncInformation(THING_NAME, SHADOW_NAME));
        assertGetShadowStatementMocks();
    }

    @Test
    void GIVEN_existing_shadow_WHEN_deleteSyncInformation_THEN_deletes_shadow_document() throws SQLException {
        setupDeleteShadowSyncMocks();

        when(mockPreparedStatement.executeUpdate()).thenReturn(1);
        ShadowManagerDAOImpl impl = new ShadowManagerDAOImpl(mockDatabase);
        assertTrue(impl.deleteSyncInformation(THING_NAME, SHADOW_NAME));

        assertDeleteShadowSyncMocks();
    }

    @Test
    void GIVEN_existing_shadow_WHEN_deleteSyncInformation_and_h2_returns_0_rows_deleted_THEN_returns_empty_optional() throws SQLException {
        setupDeleteShadowSyncMocks();

        when(mockPreparedStatement.executeUpdate()).thenReturn(0);
        ShadowManagerDAOImpl impl = new ShadowManagerDAOImpl(mockDatabase);
        assertFalse(impl.deleteSyncInformation(THING_NAME, SHADOW_NAME));

        assertDeleteShadowSyncMocks();
    }

    @Test
    void GIVEN_existing_shadow_WHEN_deleteSyncInformation_and_h2_throws_SQL_exception_THEN_ShadowManagerDataException_is_thrown() throws SQLException {
        setupDeleteShadowSyncMocks();

        when(mockPreparedStatement.executeUpdate()).thenThrow(SQLException.class);
        ShadowManagerDAOImpl impl = new ShadowManagerDAOImpl(mockDatabase);
        assertThrows(ShadowManagerDataException.class, () -> impl.deleteSyncInformation(THING_NAME, SHADOW_NAME));
        assertDeleteShadowSyncMocks();
    }

    @Test
    void GIVEN_existing_shadow_WHEN_insertSyncInfoIfNotExists_THEN_deletes_shadow_document() throws SQLException {
        setupInsertShadowSyncStatementMocks();

        when(mockPreparedStatement.executeUpdate()).thenReturn(1);
        ShadowManagerDAOImpl impl = new ShadowManagerDAOImpl(mockDatabase);
        assertTrue(impl.insertSyncInfoIfNotExists(SyncInformation.builder()
                .thingName(THING_NAME)
                .shadowName(SHADOW_NAME)
                .cloudUpdateTime(Instant.EPOCH.getEpochSecond())
                .lastSyncTime(Instant.EPOCH.getEpochSecond())
                .cloudVersion(0)
                .localVersion(0)
                .lastSyncedDocument(null)
                .build()));

        assertInsertShadowSyncStatementMocks();
    }

    @Test
    void GIVEN_existing_shadow_WHEN_insertSyncInfoIfNotExists_and_h2_returns_0_rows_deleted_THEN_returns_empty_optional() throws SQLException {
        setupInsertShadowSyncStatementMocks();

        when(mockPreparedStatement.executeUpdate()).thenReturn(0);
        ShadowManagerDAOImpl impl = new ShadowManagerDAOImpl(mockDatabase);
        assertFalse(impl.insertSyncInfoIfNotExists(SyncInformation.builder()
                .thingName(THING_NAME)
                .shadowName(SHADOW_NAME)
                .cloudUpdateTime(Instant.EPOCH.getEpochSecond())
                .lastSyncTime(Instant.EPOCH.getEpochSecond())
                .cloudVersion(0)
                .localVersion(0)
                .lastSyncedDocument(null)
                .build()));

        assertInsertShadowSyncStatementMocks();
    }

    @Test
    void GIVEN_existing_shadow_WHEN_insertSyncInfoIfNotExists_and_h2_throws_SQL_exception_THEN_ShadowManagerDataException_is_thrown() throws SQLException {
        setupInsertShadowSyncStatementMocks();

        when(mockPreparedStatement.executeUpdate()).thenThrow(SQLException.class);
        ShadowManagerDAOImpl impl = new ShadowManagerDAOImpl(mockDatabase);
        assertThrows(ShadowManagerDataException.class, () -> impl.insertSyncInfoIfNotExists(SyncInformation.builder()
                .thingName(THING_NAME)
                .shadowName(SHADOW_NAME)
                .cloudUpdateTime(Instant.EPOCH.getEpochSecond())
                .lastSyncTime(Instant.EPOCH.getEpochSecond())
                .cloudVersion(0)
                .localVersion(0)
                .lastSyncedDocument(null)
                .build()));
        assertInsertShadowSyncStatementMocks();
    }

    @Test
    void GIVEN_existing_shadow_WHEN_getAllSyncedShadowNames_THEN_deletes_shadow_document() throws SQLException {
        AtomicInteger count = new AtomicInteger(0);
        when(mockResultSet.next()).thenAnswer(invocationOnMock -> {
            count.set(count.get() + 1);
            return count.get() <= 10;
        });
        AtomicInteger currentValueCounter = new AtomicInteger(0);
        when(mockResultSet.getString(1)).thenAnswer(invocationOnMock -> {
            currentValueCounter.set(currentValueCounter.get() + 1);
            return "SomeThing-" + currentValueCounter.get();
        });
        AtomicInteger currentValueCounter2 = new AtomicInteger(0);
        when(mockResultSet.getString(2)).thenAnswer(invocationOnMock -> {
            currentValueCounter2.set(currentValueCounter2.get() + 1);
            return "SomeShadow-" + currentValueCounter2.get();
        });
        when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
        ShadowManagerDAOImpl impl = new ShadowManagerDAOImpl(mockDatabase);
        List<Pair<String, String>> allSyncedShadowNames = impl.listSyncedShadows();
        assertThat(allSyncedShadowNames, containsInAnyOrder(
                new Pair<>("SomeThing-1", "SomeShadow-1"),
                new Pair<>("SomeThing-2", "SomeShadow-2"),
                new Pair<>("SomeThing-3", "SomeShadow-3"),
                new Pair<>("SomeThing-4", "SomeShadow-4"),
                new Pair<>("SomeThing-5", "SomeShadow-5"),
                new Pair<>("SomeThing-6", "SomeShadow-6"),
                new Pair<>("SomeThing-7", "SomeShadow-7"),
                new Pair<>("SomeThing-8", "SomeShadow-8"),
                new Pair<>("SomeThing-9", "SomeShadow-9"),
                new Pair<>("SomeThing-10", "SomeShadow-10")));

    }

    @Test
    void GIVEN_existing_shadow_WHEN_getAllSyncedShadowNames_and_h2_returns_0_rows_deleted_THEN_returns_empty_optional() throws SQLException {
        when(mockResultSet.next()).thenReturn(false);
        when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
        ShadowManagerDAOImpl impl = new ShadowManagerDAOImpl(mockDatabase);
        List<Pair<String, String>> allSyncedShadowNames = impl.listSyncedShadows();
        assertThat(allSyncedShadowNames.size(), is(0));
    }

    @Test
    void GIVEN_existing_shadow_WHEN_getAllSyncedShadowNames_and_h2_throws_SQL_exception_THEN_ShadowManagerDataException_is_thrown() throws SQLException {
        when(mockPreparedStatement.executeQuery()).thenThrow(SQLException.class);
        ShadowManagerDAOImpl impl = new ShadowManagerDAOImpl(mockDatabase);
        assertThrows(ShadowManagerDataException.class, impl::listSyncedShadows);
    }

    @Test
    void GIVEN_existing_shadow_WHEN_getShadowDocumentVersion_THEN_gets_the_correct_version() throws SQLException {
        when(mockResultSet.getLong(1)).thenReturn(1L);
        when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
        when(mockResultSet.next()).thenReturn(true);
        ShadowManagerDAOImpl impl = new ShadowManagerDAOImpl(mockDatabase);
        Optional<Long> version = impl.getShadowDocumentVersion(THING_NAME, SHADOW_NAME);
        assertThat(version, is(Optional.of(1L)));
    }

    @Test
    void GIVEN_non_existing_shadow_WHEN_getShadowDocumentVersion_THEN_gets_an_empty_optional() throws SQLException {
        when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
        when(mockResultSet.next()).thenReturn(false);
        ShadowManagerDAOImpl impl = new ShadowManagerDAOImpl(mockDatabase);
        Optional<Long> version = impl.getShadowDocumentVersion(THING_NAME, SHADOW_NAME);
        assertThat(version, is(Optional.empty()));
    }

    @Test
    void GIVEN_existing_shadow_WHEN_getShadowDocumentVersion_throws_sql_excpetion_THEN_throws_sql_exception() throws SQLException {
        when(mockPreparedStatement.executeQuery()).thenThrow(SQLException.class);
        ShadowManagerDAOImpl impl = new ShadowManagerDAOImpl(mockDatabase);
        assertThrows(ShadowManagerDataException.class, () -> impl.getShadowDocumentVersion(THING_NAME, SHADOW_NAME));
    }
}
