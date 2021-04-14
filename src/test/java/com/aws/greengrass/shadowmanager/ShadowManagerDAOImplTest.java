/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager;

import com.aws.greengrass.shadowmanager.exception.ShadowManagerDataException;
import com.aws.greengrass.shadowmanager.model.ShadowDocument;
import com.aws.greengrass.shadowmanager.model.dao.SyncInformation;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
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

    private void assertUpdateShadowSyncStatementMocks(long epochNow) {
        assertThat(stringArgumentCaptor.getAllValues().size(), is(2));
        assertThat(longArgumentCaptor.getAllValues().size(), is(3));
        assertThat(bytesArgumentCaptor.getValue(), is(notNullValue()));
        assertThat(booleanArgumentCaptor.getValue(), is(notNullValue()));

        assertThat(bytesArgumentCaptor.getValue(), is(BASE_DOCUMENT));
        assertThat(booleanArgumentCaptor.getValue(), is(false));
        assertThat(stringArgumentCaptor.getAllValues().get(0), is(THING_NAME));
        assertThat(stringArgumentCaptor.getAllValues().get(1), is(SHADOW_NAME));
        assertThat(longArgumentCaptor.getAllValues().get(0), is(2L));
        assertThat(longArgumentCaptor.getAllValues().get(1), is(lessThanOrEqualTo(epochNow)));
        assertThat(longArgumentCaptor.getAllValues().get(2), is(greaterThanOrEqualTo(epochNow)));
    }

    private void assertDeleteShadowSyncStatementMocks(long epochNow) {
        assertThat(stringArgumentCaptor.getAllValues().size(), is(2));
        assertThat(longArgumentCaptor.getAllValues().size(), is(3));
        assertThat(bytesArgumentCaptor.getValue(), is(nullValue()));
        assertThat(booleanArgumentCaptor.getValue(), is(notNullValue()));

        assertThat(booleanArgumentCaptor.getValue(), is(true));
        assertThat(stringArgumentCaptor.getAllValues().get(0), is(THING_NAME));
        assertThat(stringArgumentCaptor.getAllValues().get(1), is(SHADOW_NAME));
        assertThat(longArgumentCaptor.getAllValues().get(0), is(2L));
        assertThat(longArgumentCaptor.getAllValues().get(1), is(lessThanOrEqualTo(epochNow)));
        assertThat(longArgumentCaptor.getAllValues().get(2), is(greaterThanOrEqualTo(epochNow)));
    }

    private void setupUpdateShadowSyncStatementMocks() throws SQLException {
        doNothing().when(mockPreparedStatement).setString(eq(1), stringArgumentCaptor.capture());
        doNothing().when(mockPreparedStatement).setString(eq(2), stringArgumentCaptor.capture());
        doNothing().when(mockPreparedStatement).setBytes(eq(3), bytesArgumentCaptor.capture());
        doNothing().when(mockPreparedStatement).setLong(eq(4), longArgumentCaptor.capture());
        doNothing().when(mockPreparedStatement).setBoolean(eq(5), booleanArgumentCaptor.capture());
        doNothing().when(mockPreparedStatement).setLong(eq(6), longArgumentCaptor.capture());
        doNothing().when(mockPreparedStatement).setLong(eq(7), longArgumentCaptor.capture());
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
                .cloudDocument(BASE_DOCUMENT)
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
                .cloudDocument(BASE_DOCUMENT)
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
                .cloudDocument(BASE_DOCUMENT)
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
        when(mockResultSet.next()).thenReturn(true);
        when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);

        ShadowManagerDAOImpl impl = new ShadowManagerDAOImpl(mockDatabase);
        Optional<SyncInformation> shadowSyncInformation = impl.getShadowSyncInformation(THING_NAME, SHADOW_NAME);

        assertThat(shadowSyncInformation, is(notNullValue()));
        assertThat(shadowSyncInformation.get().getCloudDocument(), is(BASE_DOCUMENT));
        assertThat(shadowSyncInformation.get().getCloudUpdateTime(), is(epochMinus60Seconds));
        assertThat(shadowSyncInformation.get().getLastSyncTime(), is(epochNow));
        assertThat(shadowSyncInformation.get().getCloudVersion(), is(2L));
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
    void GIVEN_existing_shadow_WHEN_deleteCloudDocumentInformationInSync_THEN_deletes_shadow_document() throws SQLException {
        long epochNow = Instant.now().getEpochSecond();
        long epochMinus60Seconds = Instant.now().minusSeconds(60).getEpochSecond();
        setupUpdateShadowSyncStatementMocks();

        when(mockPreparedStatement.executeUpdate()).thenReturn(1);
        ShadowManagerDAOImpl impl = new ShadowManagerDAOImpl(mockDatabase);
        assertTrue(impl.deleteCloudDocumentInformationInSync(THING_NAME, SHADOW_NAME, epochMinus60Seconds, 2));

        assertDeleteShadowSyncStatementMocks(epochNow);
    }

    @Test
    void GIVEN_existing_shadow_WHEN_deleteCloudDocumentInformationInSync_and_h2_returns_0_rows_deleted_THEN_returns_empty_optional() throws SQLException {
        long epochNow = Instant.now().getEpochSecond();
        long epochMinus60Seconds = Instant.now().minusSeconds(60).getEpochSecond();
        setupUpdateShadowSyncStatementMocks();

        when(mockPreparedStatement.executeUpdate()).thenReturn(0);
        ShadowManagerDAOImpl impl = new ShadowManagerDAOImpl(mockDatabase);
        assertFalse(impl.deleteCloudDocumentInformationInSync(THING_NAME, SHADOW_NAME, epochMinus60Seconds, 2));

        assertDeleteShadowSyncStatementMocks(epochNow);
    }

    @Test
    void GIVEN_existing_shadow_WHEN_deleteCloudDocumentInformationInSync_and_h2_throws_SQL_exception_THEN_ShadowManagerDataException_is_thrown() throws SQLException {
        long epochNow = Instant.now().getEpochSecond();
        long epochMinus60Seconds = Instant.now().minusSeconds(60).getEpochSecond();
        setupUpdateShadowSyncStatementMocks();

        when(mockPreparedStatement.executeUpdate()).thenThrow(SQLException.class);
        ShadowManagerDAOImpl impl = new ShadowManagerDAOImpl(mockDatabase);
        assertThrows(ShadowManagerDataException.class, () -> impl.deleteCloudDocumentInformationInSync(THING_NAME, SHADOW_NAME, epochMinus60Seconds, 2));
        assertDeleteShadowSyncStatementMocks(epochNow);
    }
}
