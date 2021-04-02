/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.ipc;

import com.aws.greengrass.authorization.AuthorizationHandler;
import com.aws.greengrass.authorization.Permission;
import com.aws.greengrass.authorization.exceptions.AuthorizationException;
import com.aws.greengrass.shadowmanager.util.JsonUtil;
import com.aws.greengrass.shadowmanager.ShadowManagerDAO;
import com.aws.greengrass.shadowmanager.exception.InvalidRequestParametersException;
import com.aws.greengrass.shadowmanager.exception.ShadowManagerDataException;
import com.aws.greengrass.shadowmanager.ipc.model.AcceptRequest;
import com.aws.greengrass.shadowmanager.ipc.model.Operation;
import com.aws.greengrass.shadowmanager.ipc.model.RejectRequest;
import com.aws.greengrass.shadowmanager.model.ErrorMessage;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.aws.greengrass.model.*;
import software.amazon.awssdk.crt.eventstream.ServerConnectionContinuation;
import software.amazon.awssdk.eventstreamrpc.AuthenticationData;
import software.amazon.awssdk.eventstreamrpc.OperationContinuationHandlerContext;
import software.amazon.awssdk.eventstreamrpc.model.EventStreamJsonMessage;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.time.Instant;
import java.util.Optional;

import static com.aws.greengrass.shadowmanager.model.Constants.DEFAULT_DOCUMENT_SIZE;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_STATE;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_TIMESTAMP;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_VERSION;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

//TODO: Change the names of the tests to be in the correct format.
//TODO: Use Hamcrest assertions
@ExtendWith({MockitoExtension.class, GGExtension.class})
class UpdateThingShadowIPCHandlerTest {

    private static final String TEST_SERVICE = "TestService";
    private static final String THING_NAME = "testThingName";
    private static final String SHADOW_NAME = "testShadowName";
    private final static String RESOURCE_DIRECTORY_NAME = "json_shadow_examples/";
    private final static String GOOD_INITIAL_DOCUMENT_FILE_NAME = "good_initial_document.json";
    private final static String GOOD_UPDATE_DOCUMENT_WITH_DESIRED_REQUEST_FILE_NAME = "good_update_document_with_desired.json";
    private final static String GOOD_UPDATE_DOCUMENT_WITH_DESIRED_AND_ADD_REMOVE_NODE_REQUEST_FILE_NAME = "good_update_document_with_desired_and_add_remove_node.json";
    private final static String GOOD_UPDATE_DOCUMENT_WITH_SAME_DESIRED_AND_REPORTED_FILE_NAME = "good_update_document_with_same_reported_and_desired.json";
    private final static String GOOD_UPDATE_DOCUMENT_WITH_REPORTED_REQUEST_FILE_NAME = "good_update_document_with_reported.json";
    private final static String GOOD_UPDATED_DOCUMENT_FILE_NAME = "good_new_document.json";
    private final static String GOOD_DOCUMENTS_PAYLOAD_FILE_NAME = "good_documents_payload.json";
    private final static String GOOD_DOCUMENTS_PAYLOAD_WITH_NO_PREVIOUS_FILE_NAME = "good_documents_payload_with_no_previous.json";
    private final static String GOOD_DELTA_FILE_NAME = "good_delta_node.json";
    private final static String BAD_UPDATE_DOCUMENT_WITHOUT_STATE_NODE_FILE_NAME = "bad_update_document_without_state_node.json";
    @Mock
    OperationContinuationHandlerContext mockContext;

    @Mock
    AuthenticationData mockAuthenticationData;

    @Mock
    AuthorizationHandler mockAuthorizationHandler;

    @Mock
    ShadowManagerDAO mockDao;

    @Mock
    PubSubClientWrapper mockPubSubClientWrapper;

    @Captor
    ArgumentCaptor<RejectRequest> rejectRequestCaptor;
    @Captor
    ArgumentCaptor<AcceptRequest> acceptRequestCaptor;

    private byte[] getJsonFromResource(String fileName) throws IOException, URISyntaxException {
        File f = new File(getClass().getResource(fileName).toURI());
        return Files.readAllBytes(f.toPath());
    }

    @BeforeEach
    void setup() throws IOException, ProcessingException {
        when(mockContext.getContinuation()).thenReturn(mock(ServerConnectionContinuation.class));
        when(mockContext.getAuthenticationData()).thenReturn(mockAuthenticationData);
        when(mockAuthenticationData.getIdentityLabel()).thenReturn(TEST_SERVICE);
        JsonUtil.setUpdateShadowJsonSchema();
    }

    @ParameterizedTest
    @ValueSource(strings = {SHADOW_NAME, ""})
    void GIVEN_update_thing_shadow_request_with_desried_and_existing_shadow_WHEN_handle_request_THEN_update_thing_shadow(String shadowName) throws IOException, URISyntaxException {
        byte[] initialDocument = getJsonFromResource(RESOURCE_DIRECTORY_NAME + GOOD_INITIAL_DOCUMENT_FILE_NAME);
        byte[] updateRequest = getJsonFromResource(RESOURCE_DIRECTORY_NAME + GOOD_UPDATE_DOCUMENT_WITH_DESIRED_REQUEST_FILE_NAME);
        byte[] updateDocument = getJsonFromResource(RESOURCE_DIRECTORY_NAME + GOOD_UPDATED_DOCUMENT_FILE_NAME);
        byte[] documentsPayload = getJsonFromResource(RESOURCE_DIRECTORY_NAME + GOOD_DOCUMENTS_PAYLOAD_FILE_NAME);
        byte[] deltaPayload = getJsonFromResource(RESOURCE_DIRECTORY_NAME + GOOD_DELTA_FILE_NAME);
        UpdateThingShadowRequest request = new UpdateThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(shadowName);
        request.setPayload(updateRequest);

        UpdateThingShadowResponse expectedResponse = new UpdateThingShadowResponse();
        expectedResponse.setPayload(updateDocument);

        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubClientWrapper);
        when(mockDao.getShadowThing(any(), any())).thenReturn(Optional.of(initialDocument));
        when(mockDao.updateShadowThing(any(), any(), any())).thenReturn(Optional.of(updateDocument));

        UpdateThingShadowResponse actualResponse = updateThingShadowIPCHandler.handleRequest(request);
        Optional<JsonNode> updatedDocumentJson = JsonUtil.getPayloadJson(actualResponse.getPayload());
        assertTrue(updatedDocumentJson.isPresent());
        assertTrue(updatedDocumentJson.get().has(SHADOW_DOCUMENT_TIMESTAMP));
        ((ObjectNode) updatedDocumentJson.get()).remove(SHADOW_DOCUMENT_TIMESTAMP);
        Optional<JsonNode> expectedAcceptedJson = JsonUtil.getPayloadJson(updateRequest);
        assertTrue(expectedAcceptedJson.isPresent());
        ((ObjectNode) expectedAcceptedJson.get()).set(SHADOW_DOCUMENT_VERSION, new IntNode(1));

        assertThat(updatedDocumentJson.get(), Matchers.is(expectedAcceptedJson.get()));

        verify(mockPubSubClientWrapper, times(1)).accept(acceptRequestCaptor.capture());
        verify(mockPubSubClientWrapper, times(1)).delta(acceptRequestCaptor.capture());
        verify(mockPubSubClientWrapper, times(1)).documents(acceptRequestCaptor.capture());
        assertEquals(3, acceptRequestCaptor.getAllValues().size());

        Optional<JsonNode> expectedDeltaJson = JsonUtil.getPayloadJson(deltaPayload);
        assertTrue(expectedDeltaJson.isPresent());
        Optional<JsonNode> expectedDocumentsJson = JsonUtil.getPayloadJson(documentsPayload);
        assertTrue(expectedDocumentsJson.isPresent());

        Optional<JsonNode> acceptedJson = JsonUtil.getPayloadJson(acceptRequestCaptor.getAllValues().get(0).getPayload());
        assertTrue(acceptedJson.isPresent());
        assertTrue(acceptedJson.get().has(SHADOW_DOCUMENT_TIMESTAMP));
        ((ObjectNode) acceptedJson.get()).remove(SHADOW_DOCUMENT_TIMESTAMP);
        Optional<JsonNode> deltaJson = JsonUtil.getPayloadJson(acceptRequestCaptor.getAllValues().get(1).getPayload());
        assertTrue(deltaJson.isPresent());
        assertTrue(deltaJson.get().has(SHADOW_DOCUMENT_TIMESTAMP));
        ((ObjectNode) deltaJson.get()).remove(SHADOW_DOCUMENT_TIMESTAMP);
        Optional<JsonNode> documentsJson = JsonUtil.getPayloadJson(acceptRequestCaptor.getAllValues().get(2).getPayload());
        assertTrue(documentsJson.isPresent());
        assertTrue(documentsJson.get().has(SHADOW_DOCUMENT_TIMESTAMP));
        ((ObjectNode) documentsJson.get()).remove(SHADOW_DOCUMENT_TIMESTAMP);

        assertEquals(shadowName, acceptRequestCaptor.getAllValues().get(0).getShadowName());
        assertThat(acceptedJson.get(), Matchers.is(expectedAcceptedJson.get()));
        assertEquals(THING_NAME, acceptRequestCaptor.getAllValues().get(0).getThingName());
        assertEquals(IPCUtil.LogEvents.UPDATE_THING_SHADOW.code, acceptRequestCaptor.getAllValues().get(0).getPublishOperation().getLogEventType());
        assertEquals(Operation.UPDATE_SHADOW, acceptRequestCaptor.getAllValues().get(0).getPublishOperation());

        assertEquals(shadowName, acceptRequestCaptor.getAllValues().get(1).getShadowName());
        assertThat(deltaJson.get(), Matchers.is(expectedDeltaJson.get()));
        assertEquals(THING_NAME, acceptRequestCaptor.getAllValues().get(1).getThingName());
        assertEquals(IPCUtil.LogEvents.UPDATE_THING_SHADOW.code, acceptRequestCaptor.getAllValues().get(1).getPublishOperation().getLogEventType());
        assertEquals(Operation.UPDATE_SHADOW, acceptRequestCaptor.getAllValues().get(1).getPublishOperation());

        assertEquals(shadowName, acceptRequestCaptor.getAllValues().get(2).getShadowName());
        assertThat(documentsJson.get(), Matchers.is(expectedDocumentsJson.get()));
        assertEquals(THING_NAME, acceptRequestCaptor.getAllValues().get(2).getThingName());
        assertEquals(IPCUtil.LogEvents.UPDATE_THING_SHADOW.code, acceptRequestCaptor.getAllValues().get(2).getPublishOperation().getLogEventType());
        assertEquals(Operation.UPDATE_SHADOW, acceptRequestCaptor.getAllValues().get(2).getPublishOperation());
    }

    @ParameterizedTest
    @ValueSource(strings = {SHADOW_NAME, ""})
    void GIVEN_update_thing_shadow_request_with_reported_and_existing_shadow_WHEN_handle_request_THEN_update_thing_shadow(String shadowName) throws IOException, URISyntaxException {
        byte[] initialDocument = getJsonFromResource(RESOURCE_DIRECTORY_NAME + GOOD_INITIAL_DOCUMENT_FILE_NAME);
        byte[] updateRequest = getJsonFromResource(RESOURCE_DIRECTORY_NAME + GOOD_UPDATE_DOCUMENT_WITH_REPORTED_REQUEST_FILE_NAME);
        byte[] updateDocument = getJsonFromResource(RESOURCE_DIRECTORY_NAME + GOOD_UPDATED_DOCUMENT_FILE_NAME);
        byte[] documentsPayload = getJsonFromResource(RESOURCE_DIRECTORY_NAME + "good_documents_payload_with_reported_updated.json");
        byte[] deltaPayload = getJsonFromResource(RESOURCE_DIRECTORY_NAME + GOOD_DELTA_FILE_NAME);
        UpdateThingShadowRequest request = new UpdateThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(shadowName);
        request.setPayload(updateRequest);

        UpdateThingShadowResponse expectedResponse = new UpdateThingShadowResponse();
        expectedResponse.setPayload(updateDocument);

        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubClientWrapper);
        when(mockDao.getShadowThing(any(), any())).thenReturn(Optional.of(initialDocument));
        when(mockDao.updateShadowThing(any(), any(), any())).thenReturn(Optional.of(updateDocument));

        UpdateThingShadowResponse actualResponse = updateThingShadowIPCHandler.handleRequest(request);
        Optional<JsonNode> updatedDocumentJson = JsonUtil.getPayloadJson(actualResponse.getPayload());
        assertTrue(updatedDocumentJson.isPresent());
        assertTrue(updatedDocumentJson.get().has(SHADOW_DOCUMENT_TIMESTAMP));
        ((ObjectNode) updatedDocumentJson.get()).remove(SHADOW_DOCUMENT_TIMESTAMP);
        Optional<JsonNode> expectedAcceptedJson = JsonUtil.getPayloadJson(updateRequest);
        assertTrue(expectedAcceptedJson.isPresent());
        ((ObjectNode) expectedAcceptedJson.get()).set(SHADOW_DOCUMENT_VERSION, new IntNode(1));

        assertThat(updatedDocumentJson.get(), Matchers.is(expectedAcceptedJson.get()));

        verify(mockPubSubClientWrapper, times(1)).accept(acceptRequestCaptor.capture());
        verify(mockPubSubClientWrapper, times(0)).delta(acceptRequestCaptor.capture());
        verify(mockPubSubClientWrapper, times(1)).documents(acceptRequestCaptor.capture());
        assertEquals(2, acceptRequestCaptor.getAllValues().size());

        Optional<JsonNode> expectedDeltaJson = JsonUtil.getPayloadJson(deltaPayload);
        assertTrue(expectedDeltaJson.isPresent());
        Optional<JsonNode> expectedDocumentsJson = JsonUtil.getPayloadJson(documentsPayload);
        assertTrue(expectedDocumentsJson.isPresent());

        Optional<JsonNode> acceptedJson = JsonUtil.getPayloadJson(acceptRequestCaptor.getAllValues().get(0).getPayload());
        assertTrue(acceptedJson.isPresent());
        assertTrue(acceptedJson.get().has(SHADOW_DOCUMENT_TIMESTAMP));
        ((ObjectNode) acceptedJson.get()).remove(SHADOW_DOCUMENT_TIMESTAMP);
        Optional<JsonNode> documentsJson = JsonUtil.getPayloadJson(acceptRequestCaptor.getAllValues().get(1).getPayload());
        assertTrue(documentsJson.isPresent());
        assertTrue(documentsJson.get().has(SHADOW_DOCUMENT_TIMESTAMP));
        ((ObjectNode) documentsJson.get()).remove(SHADOW_DOCUMENT_TIMESTAMP);

        assertEquals(shadowName, acceptRequestCaptor.getAllValues().get(0).getShadowName());
        assertThat(acceptedJson.get(), Matchers.is(expectedAcceptedJson.get()));
        assertEquals(THING_NAME, acceptRequestCaptor.getAllValues().get(0).getThingName());
        assertEquals(IPCUtil.LogEvents.UPDATE_THING_SHADOW.code, acceptRequestCaptor.getAllValues().get(0).getPublishOperation().getLogEventType());
        assertEquals(Operation.UPDATE_SHADOW, acceptRequestCaptor.getAllValues().get(0).getPublishOperation());

        assertEquals(shadowName, acceptRequestCaptor.getAllValues().get(1).getShadowName());
        assertThat(documentsJson.get(), Matchers.is(expectedDocumentsJson.get()));
        assertEquals(THING_NAME, acceptRequestCaptor.getAllValues().get(1).getThingName());
        assertEquals(IPCUtil.LogEvents.UPDATE_THING_SHADOW.code, acceptRequestCaptor.getAllValues().get(1).getPublishOperation().getLogEventType());
        assertEquals(Operation.UPDATE_SHADOW, acceptRequestCaptor.getAllValues().get(1).getPublishOperation());
    }

    @ParameterizedTest
    @ValueSource(strings = {SHADOW_NAME, ""})
    void GIVEN_update_thing_shadow_request_with_same_desired_and_reported_and_existing_shadow_WHEN_handle_request_THEN_update_thing_shadow(String shadowName) throws IOException, URISyntaxException {
        byte[] initialDocument = getJsonFromResource(RESOURCE_DIRECTORY_NAME + GOOD_INITIAL_DOCUMENT_FILE_NAME);
        byte[] updateRequest = getJsonFromResource(RESOURCE_DIRECTORY_NAME + GOOD_UPDATE_DOCUMENT_WITH_SAME_DESIRED_AND_REPORTED_FILE_NAME);
        byte[] updateDocument = getJsonFromResource(RESOURCE_DIRECTORY_NAME + GOOD_UPDATED_DOCUMENT_FILE_NAME);
        byte[] documentsPayload = getJsonFromResource(RESOURCE_DIRECTORY_NAME + "good_documents_payload_with_same_desired_and_reported.json");
        byte[] deltaPayload = getJsonFromResource(RESOURCE_DIRECTORY_NAME + GOOD_DELTA_FILE_NAME);
        UpdateThingShadowRequest request = new UpdateThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(shadowName);
        request.setPayload(updateRequest);

        UpdateThingShadowResponse expectedResponse = new UpdateThingShadowResponse();
        expectedResponse.setPayload(updateDocument);

        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubClientWrapper);
        when(mockDao.getShadowThing(any(), any())).thenReturn(Optional.of(initialDocument));
        when(mockDao.updateShadowThing(any(), any(), any())).thenReturn(Optional.of(updateDocument));

        UpdateThingShadowResponse actualResponse = updateThingShadowIPCHandler.handleRequest(request);
        Optional<JsonNode> updatedDocumentJson = JsonUtil.getPayloadJson(actualResponse.getPayload());
        assertTrue(updatedDocumentJson.isPresent());
        assertTrue(updatedDocumentJson.get().has(SHADOW_DOCUMENT_TIMESTAMP));
        ((ObjectNode) updatedDocumentJson.get()).remove(SHADOW_DOCUMENT_TIMESTAMP);
        Optional<JsonNode> expectedAcceptedJson = JsonUtil.getPayloadJson(updateRequest);
        assertTrue(expectedAcceptedJson.isPresent());
        ((ObjectNode) expectedAcceptedJson.get()).set(SHADOW_DOCUMENT_VERSION, new IntNode(1));

        assertThat(updatedDocumentJson.get(), Matchers.is(expectedAcceptedJson.get()));

        verify(mockPubSubClientWrapper, times(1)).accept(acceptRequestCaptor.capture());
        verify(mockPubSubClientWrapper, times(0)).delta(acceptRequestCaptor.capture());
        verify(mockPubSubClientWrapper, times(1)).documents(acceptRequestCaptor.capture());
        assertEquals(2, acceptRequestCaptor.getAllValues().size());

        Optional<JsonNode> expectedDeltaJson = JsonUtil.getPayloadJson(deltaPayload);
        assertTrue(expectedDeltaJson.isPresent());
        Optional<JsonNode> expectedDocumentsJson = JsonUtil.getPayloadJson(documentsPayload);
        assertTrue(expectedDocumentsJson.isPresent());

        Optional<JsonNode> acceptedJson = JsonUtil.getPayloadJson(acceptRequestCaptor.getAllValues().get(0).getPayload());
        assertTrue(acceptedJson.isPresent());
        assertTrue(acceptedJson.get().has(SHADOW_DOCUMENT_TIMESTAMP));
        ((ObjectNode) acceptedJson.get()).remove(SHADOW_DOCUMENT_TIMESTAMP);
        Optional<JsonNode> documentsJson = JsonUtil.getPayloadJson(acceptRequestCaptor.getAllValues().get(1).getPayload());
        assertTrue(documentsJson.isPresent());
        assertTrue(documentsJson.get().has(SHADOW_DOCUMENT_TIMESTAMP));
        ((ObjectNode) documentsJson.get()).remove(SHADOW_DOCUMENT_TIMESTAMP);

        assertEquals(shadowName, acceptRequestCaptor.getAllValues().get(0).getShadowName());
        assertThat(acceptedJson.get(), Matchers.is(expectedAcceptedJson.get()));
        assertEquals(THING_NAME, acceptRequestCaptor.getAllValues().get(0).getThingName());
        assertEquals(IPCUtil.LogEvents.UPDATE_THING_SHADOW.code, acceptRequestCaptor.getAllValues().get(0).getPublishOperation().getLogEventType());
        assertEquals(Operation.UPDATE_SHADOW, acceptRequestCaptor.getAllValues().get(0).getPublishOperation());

        assertEquals(shadowName, acceptRequestCaptor.getAllValues().get(1).getShadowName());
        assertThat(documentsJson.get(), Matchers.is(expectedDocumentsJson.get()));
        assertEquals(THING_NAME, acceptRequestCaptor.getAllValues().get(1).getThingName());
        assertEquals(IPCUtil.LogEvents.UPDATE_THING_SHADOW.code, acceptRequestCaptor.getAllValues().get(1).getPublishOperation().getLogEventType());
        assertEquals(Operation.UPDATE_SHADOW, acceptRequestCaptor.getAllValues().get(1).getPublishOperation());
    }

    @ParameterizedTest
    @ValueSource(strings = {SHADOW_NAME, ""})
    void GIVEN_update_thing_shadow_request_with_add_and_remove_nodes_in_desired_and_existing_shadow_WHEN_handle_request_THEN_update_thing_shadow(String shadowName) throws IOException, URISyntaxException {
        byte[] initialDocument = getJsonFromResource(RESOURCE_DIRECTORY_NAME + GOOD_INITIAL_DOCUMENT_FILE_NAME);
        byte[] updateRequest = getJsonFromResource(RESOURCE_DIRECTORY_NAME + GOOD_UPDATE_DOCUMENT_WITH_DESIRED_AND_ADD_REMOVE_NODE_REQUEST_FILE_NAME);
        byte[] updateDocument = getJsonFromResource(RESOURCE_DIRECTORY_NAME + "good_new_document_with_add_delete_update.json");
        byte[] documentsPayload = getJsonFromResource(RESOURCE_DIRECTORY_NAME + "good_documents_payload_after_new_node_desired.json");
        byte[] deltaPayload = getJsonFromResource(RESOURCE_DIRECTORY_NAME + "good_delta_node_with_new_node.json");
        Optional<JsonNode> expectedAcceptedJson = JsonUtil.getPayloadJson(updateRequest);
        assertTrue(expectedAcceptedJson.isPresent());
        ((ObjectNode) expectedAcceptedJson.get()).set(SHADOW_DOCUMENT_VERSION, new IntNode(1));
        Optional<JsonNode> expectedDeltaJson = JsonUtil.getPayloadJson(deltaPayload);
        assertTrue(expectedDeltaJson.isPresent());
        Optional<JsonNode> expectedDocumentsJson = JsonUtil.getPayloadJson(documentsPayload);
        assertTrue(expectedDocumentsJson.isPresent());

        UpdateThingShadowRequest request = new UpdateThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(shadowName);
        request.setPayload(updateRequest);

        UpdateThingShadowResponse expectedResponse = new UpdateThingShadowResponse();
        expectedResponse.setPayload(updateDocument);

        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubClientWrapper);
        when(mockDao.getShadowThing(any(), any())).thenReturn(Optional.of(initialDocument));
        when(mockDao.updateShadowThing(any(), any(), any())).thenReturn(Optional.of(updateDocument));

        UpdateThingShadowResponse actualResponse = updateThingShadowIPCHandler.handleRequest(request);
        Optional<JsonNode> updatedDocumentJson = JsonUtil.getPayloadJson(actualResponse.getPayload());
        assertTrue(updatedDocumentJson.isPresent());
        assertTrue(updatedDocumentJson.get().has(SHADOW_DOCUMENT_TIMESTAMP));
        ((ObjectNode) updatedDocumentJson.get()).remove(SHADOW_DOCUMENT_TIMESTAMP);

        assertThat(updatedDocumentJson.get(), Matchers.is(expectedAcceptedJson.get()));

        verify(mockPubSubClientWrapper, times(1)).accept(acceptRequestCaptor.capture());
        verify(mockPubSubClientWrapper, times(1)).delta(acceptRequestCaptor.capture());
        verify(mockPubSubClientWrapper, times(1)).documents(acceptRequestCaptor.capture());
        assertEquals(3, acceptRequestCaptor.getAllValues().size());

        Optional<JsonNode> acceptedJson = JsonUtil.getPayloadJson(acceptRequestCaptor.getAllValues().get(0).getPayload());
        assertTrue(acceptedJson.isPresent());
        assertTrue(acceptedJson.get().has(SHADOW_DOCUMENT_TIMESTAMP));
        ((ObjectNode) acceptedJson.get()).remove(SHADOW_DOCUMENT_TIMESTAMP);
        Optional<JsonNode> deltaJson = JsonUtil.getPayloadJson(acceptRequestCaptor.getAllValues().get(1).getPayload());
        assertTrue(deltaJson.isPresent());
        assertTrue(deltaJson.get().has(SHADOW_DOCUMENT_TIMESTAMP));
        ((ObjectNode) deltaJson.get()).remove(SHADOW_DOCUMENT_TIMESTAMP);
        Optional<JsonNode> documentsJson = JsonUtil.getPayloadJson(acceptRequestCaptor.getAllValues().get(2).getPayload());
        assertTrue(documentsJson.isPresent());
        assertTrue(documentsJson.get().has(SHADOW_DOCUMENT_TIMESTAMP));
        ((ObjectNode) documentsJson.get()).remove(SHADOW_DOCUMENT_TIMESTAMP);

        assertEquals(shadowName, acceptRequestCaptor.getAllValues().get(0).getShadowName());
        assertThat(acceptedJson.get(), Matchers.is(expectedAcceptedJson.get()));
        assertEquals(THING_NAME, acceptRequestCaptor.getAllValues().get(0).getThingName());
        assertEquals(IPCUtil.LogEvents.UPDATE_THING_SHADOW.code, acceptRequestCaptor.getAllValues().get(0).getPublishOperation().getLogEventType());
        assertEquals(Operation.UPDATE_SHADOW, acceptRequestCaptor.getAllValues().get(0).getPublishOperation());

        assertEquals(shadowName, acceptRequestCaptor.getAllValues().get(1).getShadowName());
        assertThat(deltaJson.get(), Matchers.is(expectedDeltaJson.get()));
        assertEquals(THING_NAME, acceptRequestCaptor.getAllValues().get(1).getThingName());
        assertEquals(IPCUtil.LogEvents.UPDATE_THING_SHADOW.code, acceptRequestCaptor.getAllValues().get(1).getPublishOperation().getLogEventType());
        assertEquals(Operation.UPDATE_SHADOW, acceptRequestCaptor.getAllValues().get(1).getPublishOperation());

        assertEquals(shadowName, acceptRequestCaptor.getAllValues().get(2).getShadowName());
        assertThat(documentsJson.get(), Matchers.is(expectedDocumentsJson.get()));
        assertEquals(THING_NAME, acceptRequestCaptor.getAllValues().get(2).getThingName());
        assertEquals(IPCUtil.LogEvents.UPDATE_THING_SHADOW.code, acceptRequestCaptor.getAllValues().get(2).getPublishOperation().getLogEventType());
        assertEquals(Operation.UPDATE_SHADOW, acceptRequestCaptor.getAllValues().get(2).getPublishOperation());
    }

    @ParameterizedTest
    @ValueSource(strings = {SHADOW_NAME, ""})
    void GIVEN_update_thing_shadow_request_with_non_existent_shadow_WHEN_handle_request_THEN_update_thing_shadow(String shadowName) throws IOException, URISyntaxException {
        byte[] updateRequest = getJsonFromResource(RESOURCE_DIRECTORY_NAME + GOOD_UPDATE_DOCUMENT_WITH_DESIRED_REQUEST_FILE_NAME);
        JsonNode payloadJson = JsonUtil.getPayloadJson(updateRequest).get();
        ((ObjectNode)payloadJson).remove(SHADOW_DOCUMENT_VERSION);
        updateRequest = JsonUtil.getPayloadBytes(payloadJson);
        byte[] updateDocument = getJsonFromResource(RESOURCE_DIRECTORY_NAME + GOOD_UPDATED_DOCUMENT_FILE_NAME);
        byte[] documentsPayload = getJsonFromResource(RESOURCE_DIRECTORY_NAME + GOOD_DOCUMENTS_PAYLOAD_WITH_NO_PREVIOUS_FILE_NAME);
        byte[] deltaPayload = getJsonFromResource(RESOURCE_DIRECTORY_NAME + GOOD_DELTA_FILE_NAME);
        UpdateThingShadowRequest request = new UpdateThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(shadowName);
        request.setPayload(updateRequest);

        UpdateThingShadowResponse expectedResponse = new UpdateThingShadowResponse();
        expectedResponse.setPayload(updateDocument);

        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubClientWrapper);
        when(mockDao.getShadowThing(any(), any())).thenReturn(Optional.empty());
        when(mockDao.updateShadowThing(any(), any(), any())).thenReturn(Optional.of(updateDocument));

        UpdateThingShadowResponse actualResponse = updateThingShadowIPCHandler.handleRequest(request);
        Optional<JsonNode> updatedDocumentJson = JsonUtil.getPayloadJson(actualResponse.getPayload());
        assertTrue(updatedDocumentJson.isPresent());
        assertTrue(updatedDocumentJson.get().has(SHADOW_DOCUMENT_TIMESTAMP));
        ((ObjectNode) updatedDocumentJson.get()).remove(SHADOW_DOCUMENT_TIMESTAMP);
        Optional<JsonNode> expectedAcceptedJson = JsonUtil.getPayloadJson(updateRequest);
        assertTrue(expectedAcceptedJson.isPresent());
        ((ObjectNode) expectedAcceptedJson.get()).set(SHADOW_DOCUMENT_VERSION, new IntNode(0));

        assertThat(updatedDocumentJson.get(), Matchers.is(expectedAcceptedJson.get()));

        verify(mockPubSubClientWrapper, times(1)).accept(acceptRequestCaptor.capture());
        verify(mockPubSubClientWrapper, times(1)).delta(acceptRequestCaptor.capture());
        verify(mockPubSubClientWrapper, times(1)).documents(acceptRequestCaptor.capture());
        assertEquals(3, acceptRequestCaptor.getAllValues().size());

        Optional<JsonNode> expectedDeltaJson = JsonUtil.getPayloadJson(deltaPayload);
        assertTrue(expectedDeltaJson.isPresent());
        ((ObjectNode) expectedDeltaJson.get().get(SHADOW_DOCUMENT_STATE).get("color")).set("r", new IntNode(255));
        ((ObjectNode) expectedDeltaJson.get()).set(SHADOW_DOCUMENT_VERSION, new IntNode(0));
        Optional<JsonNode> expectedDocumentsJson = JsonUtil.getPayloadJson(documentsPayload);
        assertTrue(expectedDocumentsJson.isPresent());

        Optional<JsonNode> acceptedJson = JsonUtil.getPayloadJson(acceptRequestCaptor.getAllValues().get(0).getPayload());
        assertTrue(acceptedJson.isPresent());
        assertTrue(acceptedJson.get().has(SHADOW_DOCUMENT_TIMESTAMP));
        ((ObjectNode) acceptedJson.get()).remove(SHADOW_DOCUMENT_TIMESTAMP);
        Optional<JsonNode> deltaJson = JsonUtil.getPayloadJson(acceptRequestCaptor.getAllValues().get(1).getPayload());
        assertTrue(deltaJson.isPresent());
        assertTrue(deltaJson.get().has(SHADOW_DOCUMENT_TIMESTAMP));
        ((ObjectNode) deltaJson.get()).remove(SHADOW_DOCUMENT_TIMESTAMP);
        Optional<JsonNode> documentsJson = JsonUtil.getPayloadJson(acceptRequestCaptor.getAllValues().get(2).getPayload());
        assertTrue(documentsJson.isPresent());
        assertTrue(documentsJson.get().has(SHADOW_DOCUMENT_TIMESTAMP));
        ((ObjectNode) documentsJson.get()).remove(SHADOW_DOCUMENT_TIMESTAMP);

        assertEquals(shadowName, acceptRequestCaptor.getAllValues().get(0).getShadowName());
        assertThat(acceptedJson.get(), Matchers.is(expectedAcceptedJson.get()));
        assertEquals(THING_NAME, acceptRequestCaptor.getAllValues().get(0).getThingName());
        assertEquals(IPCUtil.LogEvents.UPDATE_THING_SHADOW.code, acceptRequestCaptor.getAllValues().get(0).getPublishOperation().getLogEventType());
        assertEquals(Operation.UPDATE_SHADOW, acceptRequestCaptor.getAllValues().get(0).getPublishOperation());

        assertEquals(shadowName, acceptRequestCaptor.getAllValues().get(1).getShadowName());
        assertThat(deltaJson.get(), Matchers.is(expectedDeltaJson.get()));
        assertEquals(THING_NAME, acceptRequestCaptor.getAllValues().get(1).getThingName());
        assertEquals(IPCUtil.LogEvents.UPDATE_THING_SHADOW.code, acceptRequestCaptor.getAllValues().get(1).getPublishOperation().getLogEventType());
        assertEquals(Operation.UPDATE_SHADOW, acceptRequestCaptor.getAllValues().get(1).getPublishOperation());

        assertEquals(shadowName, acceptRequestCaptor.getAllValues().get(2).getShadowName());
        assertThat(documentsJson.get(), Matchers.is(expectedDocumentsJson.get()));
        assertEquals(THING_NAME, acceptRequestCaptor.getAllValues().get(2).getThingName());
        assertEquals(IPCUtil.LogEvents.UPDATE_THING_SHADOW.code, acceptRequestCaptor.getAllValues().get(2).getPublishOperation().getLogEventType());
        assertEquals(Operation.UPDATE_SHADOW, acceptRequestCaptor.getAllValues().get(2).getPublishOperation());
    }

    @Test
    void GIVEN_update_thing_shadow_WHEN_dao_update_sends_data_exception_THEN_update_thing_shadow_fails(ExtensionContext context) throws IOException, URISyntaxException {
        byte[] initialDocument = getJsonFromResource(RESOURCE_DIRECTORY_NAME + GOOD_INITIAL_DOCUMENT_FILE_NAME);
        byte[] updateRequest = getJsonFromResource(RESOURCE_DIRECTORY_NAME + GOOD_UPDATE_DOCUMENT_WITH_DESIRED_REQUEST_FILE_NAME);
        ignoreExceptionOfType(context, ShadowManagerDataException.class);
        UpdateThingShadowRequest request = new UpdateThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);
        request.setPayload(updateRequest);

        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubClientWrapper);
        when(mockDao.getShadowThing(any(), any())).thenReturn(Optional.of(initialDocument));
        doThrow(ShadowManagerDataException.class).when(mockDao).updateShadowThing(any(), any(), any());

        assertThrows(ServiceError.class, () -> updateThingShadowIPCHandler.handleRequest(request));

        verify(mockPubSubClientWrapper, times(1)).reject(rejectRequestCaptor.capture());

        assertNotNull(rejectRequestCaptor.getValue());

        assertEquals(SHADOW_NAME, rejectRequestCaptor.getValue().getShadowName());
        ErrorMessage errorMessage = rejectRequestCaptor.getValue().getErrorMessage();
        assertNotEquals(Instant.EPOCH.toEpochMilli(), errorMessage.getTimestamp());
        assertEquals(Operation.UPDATE_SHADOW, rejectRequestCaptor.getValue().getPublishOperation());
        assertEquals(500, errorMessage.getErrorCode());
        assertEquals("Internal service failure", errorMessage.getMessage());
        assertEquals(IPCUtil.LogEvents.UPDATE_THING_SHADOW.code, rejectRequestCaptor.getAllValues().get(0).getPublishOperation().getLogEventType());
    }

    @Test
    void GIVEN_update_thing_shadow_WHEN_dao_get_sends_data_exception_THEN_update_thing_shadow_fails(ExtensionContext context) throws IOException, URISyntaxException {
        byte[] updateRequest = getJsonFromResource(RESOURCE_DIRECTORY_NAME + GOOD_UPDATE_DOCUMENT_WITH_DESIRED_REQUEST_FILE_NAME);
        ignoreExceptionOfType(context, ShadowManagerDataException.class);
        UpdateThingShadowRequest request = new UpdateThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);
        request.setPayload(updateRequest);

        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubClientWrapper);
        doThrow(ShadowManagerDataException.class).when(mockDao).getShadowThing(any(), any());

        assertThrows(ServiceError.class, () -> updateThingShadowIPCHandler.handleRequest(request));

        verify(mockPubSubClientWrapper, times(1)).reject(rejectRequestCaptor.capture());

        assertNotNull(rejectRequestCaptor.getValue());

        assertEquals(SHADOW_NAME, rejectRequestCaptor.getValue().getShadowName());
        ErrorMessage errorMessage = rejectRequestCaptor.getValue().getErrorMessage();
        assertNotEquals(Instant.EPOCH.toEpochMilli(), errorMessage.getTimestamp());
        assertEquals(Operation.UPDATE_SHADOW, rejectRequestCaptor.getValue().getPublishOperation());
        assertEquals(500, errorMessage.getErrorCode());
        assertEquals("Internal service failure", errorMessage.getMessage());
        assertEquals(IPCUtil.LogEvents.UPDATE_THING_SHADOW.code, rejectRequestCaptor.getAllValues().get(0).getPublishOperation().getLogEventType());
    }

    @Test
    void GIVEN_update_thing_shadow_request_WHEN_missing_payload_THEN_update_thing_shadow(ExtensionContext context) {
        ignoreExceptionOfType(context, InvalidRequestParametersException.class);
        UpdateThingShadowRequest request = new UpdateThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);
        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubClientWrapper);

        InvalidArgumentsError thrown = assertThrows(InvalidArgumentsError.class, () -> updateThingShadowIPCHandler.handleRequest(request));
        assertThat(thrown.getMessage(), Matchers.is("Missing update payload"));
        verify(mockPubSubClientWrapper, times(1))
                .reject(rejectRequestCaptor.capture());

        assertNotNull(rejectRequestCaptor.getValue());

        assertEquals(SHADOW_NAME, rejectRequestCaptor.getValue().getShadowName());
        ErrorMessage errorMessage = rejectRequestCaptor.getValue().getErrorMessage();
        assertEquals(Operation.UPDATE_SHADOW, rejectRequestCaptor.getValue().getPublishOperation());
        assertEquals(403, errorMessage.getErrorCode());
        assertEquals("Forbidden", errorMessage.getMessage());
        assertEquals(IPCUtil.LogEvents.UPDATE_THING_SHADOW.code, rejectRequestCaptor.getAllValues().get(0).getPublishOperation().getLogEventType());
    }

    @Test
    void GIVEN_update_thing_shadow_request_WHEN_ipc_request_unauthorized_THEN_throw_unauthorized_exception(ExtensionContext context) throws Exception {
        byte[] updateRequest = getJsonFromResource(RESOURCE_DIRECTORY_NAME + GOOD_UPDATE_DOCUMENT_WITH_DESIRED_REQUEST_FILE_NAME);
        ignoreExceptionOfType(context, AuthorizationException.class);
        UpdateThingShadowRequest request = new UpdateThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);
        request.setPayload(updateRequest);
        when(mockAuthorizationHandler.isAuthorized(any(), any(Permission.class))).thenThrow(AuthorizationException.class);

        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubClientWrapper);
        assertThrows(UnauthorizedError.class, () -> updateThingShadowIPCHandler.handleRequest(request));
        verify(mockPubSubClientWrapper, times(1)).reject(rejectRequestCaptor.capture());

        assertNotNull(rejectRequestCaptor.getValue());

        assertEquals(SHADOW_NAME, rejectRequestCaptor.getValue().getShadowName());
        ErrorMessage errorMessage = rejectRequestCaptor.getValue().getErrorMessage();
        assertEquals(Operation.UPDATE_SHADOW, rejectRequestCaptor.getValue().getPublishOperation());
        assertEquals(401, errorMessage.getErrorCode());
        assertEquals("Unauthorized", errorMessage.getMessage());
        assertEquals(IPCUtil.LogEvents.UPDATE_THING_SHADOW.code, rejectRequestCaptor.getAllValues().get(0).getPublishOperation().getLogEventType());
    }

    @ParameterizedTest
    @NullAndEmptySource
    void GIVEN_missing_thing_name_WHEN_handle_request_THEN_throw_invalid_arguments_error(String thingName, ExtensionContext context) throws IOException, URISyntaxException {
        byte[] updateRequest = getJsonFromResource(RESOURCE_DIRECTORY_NAME + GOOD_UPDATE_DOCUMENT_WITH_DESIRED_REQUEST_FILE_NAME);
        ignoreExceptionOfType(context, InvalidRequestParametersException.class);
        UpdateThingShadowRequest request = new UpdateThingShadowRequest();
        request.setThingName(thingName);
        request.setShadowName(SHADOW_NAME);
        request.setPayload(updateRequest);

        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubClientWrapper);
        assertThrows(InvalidArgumentsError.class, () -> updateThingShadowIPCHandler.handleRequest(request));
        verify(mockPubSubClientWrapper, times(1)).reject(rejectRequestCaptor.capture());

        assertNotNull(rejectRequestCaptor.getValue());

        assertEquals(SHADOW_NAME, rejectRequestCaptor.getValue().getShadowName());
        ErrorMessage errorMessage = rejectRequestCaptor.getValue().getErrorMessage();
        assertEquals(Operation.UPDATE_SHADOW, rejectRequestCaptor.getValue().getPublishOperation());
        assertEquals(404, errorMessage.getErrorCode());
        assertEquals("Thing not found", errorMessage.getMessage());
        assertEquals(IPCUtil.LogEvents.UPDATE_THING_SHADOW.code, rejectRequestCaptor.getAllValues().get(0).getPublishOperation().getLogEventType());
    }

    @Test
    void GIVEN_update_thing_shadow_request_WHEN_missing_payload_THEN_throw_invalid_arguments_exception(ExtensionContext context) {
        ignoreExceptionOfType(context, InvalidRequestParametersException.class);
        UpdateThingShadowRequest request = new UpdateThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);
        request.setPayload(new byte[0]);

        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubClientWrapper);
        assertThrows(InvalidArgumentsError.class, () -> updateThingShadowIPCHandler.handleRequest(request));

        verify(mockPubSubClientWrapper, times(1)).reject(rejectRequestCaptor.capture());

        assertNotNull(rejectRequestCaptor.getValue());

        assertEquals(SHADOW_NAME, rejectRequestCaptor.getValue().getShadowName());
        ErrorMessage errorMessage = rejectRequestCaptor.getValue().getErrorMessage();
        assertEquals(Operation.UPDATE_SHADOW, rejectRequestCaptor.getValue().getPublishOperation());
        assertEquals(403, errorMessage.getErrorCode());
        assertEquals("Forbidden", errorMessage.getMessage());
        assertEquals(IPCUtil.LogEvents.UPDATE_THING_SHADOW.code, rejectRequestCaptor.getAllValues().get(0).getPublishOperation().getLogEventType());
    }

    @Test
    void GIVEN_update_thing_shadow_request_WHEN_unexpected_empty_return_during_update_THEN_throw_service_error_exception(ExtensionContext context) throws IOException, URISyntaxException {
        byte[] initialDocument = getJsonFromResource(RESOURCE_DIRECTORY_NAME + GOOD_INITIAL_DOCUMENT_FILE_NAME);
        byte[] updateRequest = getJsonFromResource(RESOURCE_DIRECTORY_NAME + GOOD_UPDATE_DOCUMENT_WITH_DESIRED_REQUEST_FILE_NAME);
        ignoreExceptionOfType(context, ServiceError.class);
        UpdateThingShadowRequest request = new UpdateThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);
        request.setPayload(updateRequest);

        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubClientWrapper);
        when(mockDao.getShadowThing(any(), any())).thenReturn(Optional.of(initialDocument));
        when(mockDao.updateShadowThing(any(), any(), any())).thenReturn(Optional.empty());

        ServiceError thrown = assertThrows(ServiceError.class, () -> updateThingShadowIPCHandler.handleRequest(request));
        assertTrue(thrown.getMessage().contains("Unexpected error"));

        verify(mockPubSubClientWrapper, times(1))
                .reject(rejectRequestCaptor.capture());

        assertNotNull(rejectRequestCaptor.getValue());

        assertEquals(SHADOW_NAME, rejectRequestCaptor.getValue().getShadowName());
        ErrorMessage errorMessage = rejectRequestCaptor.getValue().getErrorMessage();
        assertNotEquals(Instant.EPOCH.toEpochMilli(), errorMessage.getTimestamp());
        assertEquals(Operation.UPDATE_SHADOW, rejectRequestCaptor.getValue().getPublishOperation());
        assertEquals(500, errorMessage.getErrorCode());
        assertEquals("Internal service failure", errorMessage.getMessage());
        assertEquals(IPCUtil.LogEvents.UPDATE_THING_SHADOW.code, rejectRequestCaptor.getAllValues().get(0).getPublishOperation().getLogEventType());
    }

    @Test
    void GIVEN_update_thing_shadow_request_WHEN_given_bad_payload_THEN_sends_message_on_rejected_topic(ExtensionContext context) throws IOException, URISyntaxException {
        byte[] badUpdateRequest = getJsonFromResource(RESOURCE_DIRECTORY_NAME + BAD_UPDATE_DOCUMENT_WITHOUT_STATE_NODE_FILE_NAME);
        ignoreExceptionOfType(context, InvalidRequestParametersException.class);
        UpdateThingShadowRequest request = new UpdateThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);
        request.setPayload(badUpdateRequest);

        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubClientWrapper);

        InvalidArgumentsError thrown = assertThrows(InvalidArgumentsError.class, () -> updateThingShadowIPCHandler.handleRequest(request));
        assertEquals("Invalid JSON\nobject has missing required properties ([\"state\"])", thrown.getMessage().trim());

        verify(mockPubSubClientWrapper, times(1))
                .reject(rejectRequestCaptor.capture());

        assertNotNull(rejectRequestCaptor.getValue());

        assertEquals(SHADOW_NAME, rejectRequestCaptor.getValue().getShadowName());
        ErrorMessage errorMessage = rejectRequestCaptor.getValue().getErrorMessage();
        assertEquals(Operation.UPDATE_SHADOW, rejectRequestCaptor.getValue().getPublishOperation());
        assertEquals(400, errorMessage.getErrorCode());
        assertEquals("Invalid JSON\nobject has missing required properties ([\"state\"])", errorMessage.getMessage());
        assertEquals(IPCUtil.LogEvents.UPDATE_THING_SHADOW.code, rejectRequestCaptor.getAllValues().get(0).getPublishOperation().getLogEventType());
    }

    @Test
    void GIVEN_update_thing_shadow_request_with_nonInt_version_and_existing_shadow_WHEN_validation_throws_ConflictError_THEN_sends_message_on_rejected_topic(ExtensionContext context)
            throws IOException, URISyntaxException {
        byte[] initialDocument = getJsonFromResource(RESOURCE_DIRECTORY_NAME + GOOD_INITIAL_DOCUMENT_FILE_NAME);
        byte[] badUpdateRequest = getJsonFromResource(RESOURCE_DIRECTORY_NAME + "bad_version_update_document.json");
        ignoreExceptionOfType(context, InvalidRequestParametersException.class);
        UpdateThingShadowRequest request = new UpdateThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);
        request.setPayload(badUpdateRequest);

        when(mockDao.getShadowThing(any(), any())).thenReturn(Optional.of(initialDocument));

        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubClientWrapper);

        InvalidArgumentsError thrown = assertThrows(InvalidArgumentsError.class, () -> updateThingShadowIPCHandler.handleRequest(request));
        assertEquals("Invalid JSON\nInvalid version. instance type (string) does not match any allowed primitive type (allowed: [\"integer\",\"number\"])", thrown.getMessage().trim());

        verify(mockPubSubClientWrapper, times(1))
                .reject(rejectRequestCaptor.capture());

        assertNotNull(rejectRequestCaptor.getValue());

        assertEquals(SHADOW_NAME, rejectRequestCaptor.getValue().getShadowName());
        ErrorMessage errorMessage = rejectRequestCaptor.getValue().getErrorMessage();
        assertEquals(Operation.UPDATE_SHADOW, rejectRequestCaptor.getValue().getPublishOperation());
        assertEquals(400, errorMessage.getErrorCode());
        assertEquals("Invalid JSON\nInvalid version. instance type (string) does not match any allowed primitive type (allowed: [\"integer\",\"number\"])", errorMessage.getMessage());
        assertEquals(IPCUtil.LogEvents.UPDATE_THING_SHADOW.code, rejectRequestCaptor.getAllValues().get(0).getPublishOperation().getLogEventType());
    }

    @Test
    void GIVEN_update_thing_shadow_request_with_higher_version_and_existing_shadow_WHEN_validation_throws_ConflictError_THEN_sends_message_on_rejected_topic(ExtensionContext context)
            throws IOException, URISyntaxException {
        byte[] initialDocument = getJsonFromResource(RESOURCE_DIRECTORY_NAME + GOOD_INITIAL_DOCUMENT_FILE_NAME);
        byte[] badUpdateRequest = getJsonFromResource(RESOURCE_DIRECTORY_NAME + "bad_version_update_document_with_higher_version_number.json");
        ignoreExceptionOfType(context, ConflictError.class);
        UpdateThingShadowRequest request = new UpdateThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);
        request.setPayload(badUpdateRequest);

        when(mockDao.getShadowThing(any(), any())).thenReturn(Optional.of(initialDocument));

        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubClientWrapper);

        ConflictError thrown = assertThrows(ConflictError.class, () -> updateThingShadowIPCHandler.handleRequest(request));
        assertEquals("Invalid version", thrown.getMessage().trim());

        verify(mockPubSubClientWrapper, times(1))
                .reject(rejectRequestCaptor.capture());

        assertNotNull(rejectRequestCaptor.getValue());

        assertEquals(SHADOW_NAME, rejectRequestCaptor.getValue().getShadowName());
        ErrorMessage errorMessage = rejectRequestCaptor.getValue().getErrorMessage();
        assertEquals(Operation.UPDATE_SHADOW, rejectRequestCaptor.getValue().getPublishOperation());
        assertEquals(409, errorMessage.getErrorCode());
        assertEquals("Version conflict", errorMessage.getMessage());
        assertEquals(IPCUtil.LogEvents.UPDATE_THING_SHADOW.code, rejectRequestCaptor.getAllValues().get(0).getPublishOperation().getLogEventType());
    }

    @Test
    void GIVEN_update_thing_shadow_request_with_state_node_as_value_node_WHEN_validation_throws_ConflictError_THEN_sends_message_on_rejected_topic(ExtensionContext context)
            throws IOException, URISyntaxException {
        byte[] initialDocument = getJsonFromResource(RESOURCE_DIRECTORY_NAME + GOOD_INITIAL_DOCUMENT_FILE_NAME);
        byte[] badUpdateRequest = getJsonFromResource(RESOURCE_DIRECTORY_NAME + "bad_update_document_with_state_node_as_value_node.json");
        ignoreExceptionOfType(context, InvalidRequestParametersException.class);
        UpdateThingShadowRequest request = new UpdateThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);
        request.setPayload(badUpdateRequest);

        when(mockDao.getShadowThing(any(), any())).thenReturn(Optional.of(initialDocument));

        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubClientWrapper);

        InvalidArgumentsError thrown = assertThrows(InvalidArgumentsError.class, () -> updateThingShadowIPCHandler.handleRequest(request));
        assertEquals("Invalid JSON\nInvalid state. instance type (string) does not match any allowed primitive type (allowed: [\"object\"])", thrown.getMessage().trim());

        verify(mockPubSubClientWrapper, times(1))
                .reject(rejectRequestCaptor.capture());

        assertNotNull(rejectRequestCaptor.getValue());

        assertEquals(SHADOW_NAME, rejectRequestCaptor.getValue().getShadowName());
        ErrorMessage errorMessage = rejectRequestCaptor.getValue().getErrorMessage();
        assertEquals(Operation.UPDATE_SHADOW, rejectRequestCaptor.getValue().getPublishOperation());
        assertEquals(400, errorMessage.getErrorCode());
        assertEquals("Invalid JSON\nInvalid state. instance type (string) does not match any allowed primitive type (allowed: [\"object\"])", errorMessage.getMessage());
        assertEquals(IPCUtil.LogEvents.UPDATE_THING_SHADOW.code, rejectRequestCaptor.getAllValues().get(0).getPublishOperation().getLogEventType());
    }


    @Test
    void GIVEN_update_thing_shadow_request_with_state_node_as_empty_node_WHEN_validation_throws_ConflictError_THEN_sends_message_on_rejected_topic(ExtensionContext context)
            throws IOException, URISyntaxException {
        byte[] initialDocument = getJsonFromResource(RESOURCE_DIRECTORY_NAME + GOOD_INITIAL_DOCUMENT_FILE_NAME);
        byte[] badUpdateRequest = getJsonFromResource(RESOURCE_DIRECTORY_NAME + "bad_update_document_with_state_node_as_empty_node.json");
        ignoreExceptionOfType(context, InvalidRequestParametersException.class);
        UpdateThingShadowRequest request = new UpdateThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);
        request.setPayload(badUpdateRequest);

        when(mockDao.getShadowThing(any(), any())).thenReturn(Optional.of(initialDocument));

        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubClientWrapper);

        InvalidArgumentsError thrown = assertThrows(InvalidArgumentsError.class, () -> updateThingShadowIPCHandler.handleRequest(request));
        assertEquals("Invalid JSON\nState node needs to have to either reported or desired node.", thrown.getMessage().trim());

        verify(mockPubSubClientWrapper, times(1))
                .reject(rejectRequestCaptor.capture());

        assertNotNull(rejectRequestCaptor.getValue());

        assertEquals(SHADOW_NAME, rejectRequestCaptor.getValue().getShadowName());
        ErrorMessage errorMessage = rejectRequestCaptor.getValue().getErrorMessage();
        assertEquals(Operation.UPDATE_SHADOW, rejectRequestCaptor.getValue().getPublishOperation());
        assertEquals(400, errorMessage.getErrorCode());
        assertEquals("Invalid JSON\nState node needs to have to either reported or desired node.", errorMessage.getMessage());
        assertEquals(IPCUtil.LogEvents.UPDATE_THING_SHADOW.code, rejectRequestCaptor.getAllValues().get(0).getPublishOperation().getLogEventType());
    }

    @Test
    void GIVEN_update_thing_shadow_request_with_state_depth_greater_than_max_WHEN_validation_throws_ConflictError_THEN_sends_message_on_rejected_topic(ExtensionContext context)
            throws IOException, URISyntaxException {
        byte[] initialDocument = getJsonFromResource(RESOURCE_DIRECTORY_NAME + GOOD_INITIAL_DOCUMENT_FILE_NAME);
        byte[] badUpdateRequest = getJsonFromResource(RESOURCE_DIRECTORY_NAME + "bad_update_document_with_depth.json");
        ignoreExceptionOfType(context, InvalidRequestParametersException.class);
        UpdateThingShadowRequest request = new UpdateThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);
        request.setPayload(badUpdateRequest);

        when(mockDao.getShadowThing(any(), any())).thenReturn(Optional.of(initialDocument));

        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubClientWrapper);

        InvalidArgumentsError thrown = assertThrows(InvalidArgumentsError.class, () -> updateThingShadowIPCHandler.handleRequest(request));
        assertEquals("JSON contains too many levels of nesting; maximum is 6", thrown.getMessage().trim());

        verify(mockPubSubClientWrapper, times(1)).reject(rejectRequestCaptor.capture());

        assertNotNull(rejectRequestCaptor.getValue());

        assertEquals(SHADOW_NAME, rejectRequestCaptor.getValue().getShadowName());
        ErrorMessage errorMessage = rejectRequestCaptor.getValue().getErrorMessage();
        assertEquals(Operation.UPDATE_SHADOW, rejectRequestCaptor.getValue().getPublishOperation());
        assertEquals(400, errorMessage.getErrorCode());
        assertEquals("JSON contains too many levels of nesting; maximum is 6", errorMessage.getMessage());
        assertEquals(IPCUtil.LogEvents.UPDATE_THING_SHADOW.code, rejectRequestCaptor.getAllValues().get(0).getPublishOperation().getLogEventType());
    }

    @Test
    void GIVEN_update_thing_shadow_request_with_payload_size_greater_than_max_WHEN_validation_throws_ConflictError_THEN_sends_message_on_rejected_topic(ExtensionContext context)
            throws IOException, URISyntaxException {
        byte[] initialDocument = getJsonFromResource(RESOURCE_DIRECTORY_NAME + GOOD_INITIAL_DOCUMENT_FILE_NAME);
        byte[] badUpdateRequest = new byte[DEFAULT_DOCUMENT_SIZE + 1];
        ignoreExceptionOfType(context, InvalidRequestParametersException.class);
        UpdateThingShadowRequest request = new UpdateThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);
        request.setPayload(badUpdateRequest);

        when(mockDao.getShadowThing(any(), any())).thenReturn(Optional.of(initialDocument));

        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubClientWrapper);

        InvalidArgumentsError thrown = assertThrows(InvalidArgumentsError.class, () -> updateThingShadowIPCHandler.handleRequest(request));
        assertEquals("The payload exceeds the maximum size allowed", thrown.getMessage().trim());

        verify(mockPubSubClientWrapper, times(1)).reject(rejectRequestCaptor.capture());

        assertNotNull(rejectRequestCaptor.getValue());

        assertEquals(SHADOW_NAME, rejectRequestCaptor.getValue().getShadowName());
        ErrorMessage errorMessage = rejectRequestCaptor.getValue().getErrorMessage();
        assertEquals(Operation.UPDATE_SHADOW, rejectRequestCaptor.getValue().getPublishOperation());
        assertEquals(413, errorMessage.getErrorCode());
        assertEquals("The payload exceeds the maximum size allowed", errorMessage.getMessage());
        assertEquals(IPCUtil.LogEvents.UPDATE_THING_SHADOW.code, rejectRequestCaptor.getAllValues().get(0).getPublishOperation().getLogEventType());
    }

    @Test
    void GIVEN_update_thing_shadow_ipc_handler_WHEN_handle_stream_event_THEN_nothing_happens() {
        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubClientWrapper);
        assertDoesNotThrow(() -> updateThingShadowIPCHandler.handleStreamEvent(mock(EventStreamJsonMessage.class)));
    }

    @Test
    void GIVEN_update_thing_shadow_ipc_handler_WHEN_stream_closes_THEN_nothing_happens() {
        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubClientWrapper);
        assertDoesNotThrow(updateThingShadowIPCHandler::onStreamClosed);
    }
}
