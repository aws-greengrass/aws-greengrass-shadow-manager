/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.ipc;

import com.aws.greengrass.authorization.exceptions.AuthorizationException;
import com.aws.greengrass.shadowmanager.util.JsonUtil;
import com.aws.greengrass.shadowmanager.AuthorizationHandlerWrapper;
import com.aws.greengrass.shadowmanager.ShadowManagerDAO;
import com.aws.greengrass.shadowmanager.exception.InvalidRequestParametersException;
import com.aws.greengrass.shadowmanager.exception.ShadowManagerDataException;
import com.aws.greengrass.shadowmanager.ipc.model.AcceptRequest;
import com.aws.greengrass.shadowmanager.ipc.model.Operation;
import com.aws.greengrass.shadowmanager.ipc.model.RejectRequest;
import com.aws.greengrass.shadowmanager.model.ErrorMessage;
import com.aws.greengrass.shadowmanager.model.LogEvents;
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
import org.junit.jupiter.params.provider.EmptySource;
import org.junit.jupiter.params.provider.MethodSource;
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

import static com.aws.greengrass.shadowmanager.TestUtils.*;
import static com.aws.greengrass.shadowmanager.model.Constants.DEFAULT_DOCUMENT_SIZE;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_STATE;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_TIMESTAMP;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_VERSION;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
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
    AuthorizationHandlerWrapper mockAuthorizationHandlerWrapper;

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
    @EmptySource
    @ValueSource(strings = {SHADOW_NAME})
    void GIVEN_update_thing_shadow_request_with_desired_and_existing_shadow_WHEN_handle_request_THEN_update_thing_shadow(String shadowName) throws IOException, URISyntaxException {
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

        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandlerWrapper, mockPubSubClientWrapper);
        when(mockDao.getShadowThing(any(), any())).thenReturn(Optional.of(initialDocument));
        when(mockDao.updateShadowThing(any(), any(), any())).thenReturn(Optional.of(updateDocument));

        UpdateThingShadowResponse actualResponse = updateThingShadowIPCHandler.handleRequest(request);
        Optional<JsonNode> updatedDocumentJson = JsonUtil.getPayloadJson(actualResponse.getPayload());
        assertThat("Retrieved updateDocumentJson", updatedDocumentJson.isPresent(), is(true));
        assertThat("updateDocumentJson has timestamp", updatedDocumentJson.get().has(SHADOW_DOCUMENT_TIMESTAMP), is(true));
        ((ObjectNode) updatedDocumentJson.get()).remove(SHADOW_DOCUMENT_TIMESTAMP);
        Optional<JsonNode> expectedAcceptedJson = JsonUtil.getPayloadJson(updateRequest);
        assertThat("Retrieved expectedAcceptedJson", expectedAcceptedJson.isPresent(), is(true));
        ((ObjectNode) expectedAcceptedJson.get()).set(SHADOW_DOCUMENT_VERSION, new IntNode(1));

        assertThat(updatedDocumentJson.get(), is(equalTo(expectedAcceptedJson.get())));

        verify(mockPubSubClientWrapper, times(1)).accept(acceptRequestCaptor.capture());
        verify(mockPubSubClientWrapper, times(1)).delta(acceptRequestCaptor.capture());
        verify(mockPubSubClientWrapper, times(1)).documents(acceptRequestCaptor.capture());

        assertThat(acceptRequestCaptor.getAllValues().size(), is(equalTo(3)));

        Optional<JsonNode> expectedDeltaJson = JsonUtil.getPayloadJson(deltaPayload);
        assertThat("Found expectedDeltaJson", expectedDeltaJson.isPresent(), is(true));
        Optional<JsonNode> expectedDocumentsJson = JsonUtil.getPayloadJson(documentsPayload);
        assertThat("Found expectedDocumentsJson", expectedDocumentsJson.isPresent(), is(true));

        Optional<JsonNode> acceptedJson = JsonUtil.getPayloadJson(acceptRequestCaptor.getAllValues().get(0).getPayload());
        assertThat("Retrieved acceptedJson", acceptedJson.isPresent(), is(true));
        assertThat("acceptedJson has timestamp", acceptedJson.get().has(SHADOW_DOCUMENT_TIMESTAMP), is(true));
        ((ObjectNode) acceptedJson.get()).remove(SHADOW_DOCUMENT_TIMESTAMP);
        Optional<JsonNode> deltaJson = JsonUtil.getPayloadJson(acceptRequestCaptor.getAllValues().get(1).getPayload());
        assertThat("Retrieved deltaJson", deltaJson.isPresent(), is(true));
        assertThat("deltaJson has timestamp", deltaJson.get().has(SHADOW_DOCUMENT_TIMESTAMP), is(true));
        ((ObjectNode) deltaJson.get()).remove(SHADOW_DOCUMENT_TIMESTAMP);
        Optional<JsonNode> documentsJson = JsonUtil.getPayloadJson(acceptRequestCaptor.getAllValues().get(2).getPayload());
        assertThat("Retrieved documentsJson", documentsJson.isPresent(), is(true));
        assertThat("documentsJson has timestamp", documentsJson.get().has(SHADOW_DOCUMENT_TIMESTAMP), is(true));
        ((ObjectNode) documentsJson.get()).remove(SHADOW_DOCUMENT_TIMESTAMP);

        // verify pubsub payloads match expected output
        assertThat(acceptedJson.get(), is(equalTo(expectedAcceptedJson.get())));
        assertThat(deltaJson.get(), is(equalTo(expectedDeltaJson.get())));
        assertThat(documentsJson.get(), is(equalTo(expectedDocumentsJson.get())));

        // verify each pubsub call (accept, delta, documents) had expected values
        for (int i = 0; i < acceptRequestCaptor.getAllValues().size(); i++) {
            assertThat(acceptRequestCaptor.getAllValues().get(i).getShadowName(), is(equalTo(shadowName)));
            assertThat(acceptRequestCaptor.getAllValues().get(i).getThingName(), is(equalTo(THING_NAME)));
            assertThat("Expected operation", acceptRequestCaptor.getAllValues().get(i).getPublishOperation(), is(Operation.UPDATE_SHADOW));
            assertThat("Expected log code", acceptRequestCaptor.getAllValues().get(i).getPublishOperation().getLogEventType(), is(LogEvents.UPDATE_THING_SHADOW.code()));
        }
    }

    @ParameterizedTest
    @EmptySource
    @ValueSource(strings = {SHADOW_NAME})
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

        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandlerWrapper, mockPubSubClientWrapper);
        when(mockDao.getShadowThing(any(), any())).thenReturn(Optional.of(initialDocument));
        when(mockDao.updateShadowThing(any(), any(), any())).thenReturn(Optional.of(updateDocument));

        UpdateThingShadowResponse actualResponse = updateThingShadowIPCHandler.handleRequest(request);
        Optional<JsonNode> updatedDocumentJson = JsonUtil.getPayloadJson(actualResponse.getPayload());
        assertThat("Retrieved updateDocumentJson", updatedDocumentJson.isPresent(), is(true));
        assertThat("updateDocumentJson has timestamp", updatedDocumentJson.get().has(SHADOW_DOCUMENT_TIMESTAMP), is(true));
        ((ObjectNode) updatedDocumentJson.get()).remove(SHADOW_DOCUMENT_TIMESTAMP);
        Optional<JsonNode> expectedAcceptedJson = JsonUtil.getPayloadJson(updateRequest);
        assertThat("Retrieved expectedAcceptedJson", expectedAcceptedJson.isPresent(), is(true));
        ((ObjectNode) expectedAcceptedJson.get()).set(SHADOW_DOCUMENT_VERSION, new IntNode(1));

        assertThat(updatedDocumentJson.get(), is(equalTo(expectedAcceptedJson.get())));

        verify(mockPubSubClientWrapper, times(1)).accept(acceptRequestCaptor.capture());
        verify(mockPubSubClientWrapper, times(0)).delta(acceptRequestCaptor.capture());
        verify(mockPubSubClientWrapper, times(1)).documents(acceptRequestCaptor.capture());

        assertThat(acceptRequestCaptor.getAllValues().size(), is(equalTo(2)));

        Optional<JsonNode> expectedDeltaJson = JsonUtil.getPayloadJson(deltaPayload);
        assertThat("Found expectedDeltaJson", expectedDeltaJson.isPresent(), is(true));
        Optional<JsonNode> expectedDocumentsJson = JsonUtil.getPayloadJson(documentsPayload);
        assertThat("Found expectedDocumentsJson", expectedDocumentsJson.isPresent(), is(true));

        Optional<JsonNode> acceptedJson = JsonUtil.getPayloadJson(acceptRequestCaptor.getAllValues().get(0).getPayload());
        assertThat("Retrieved acceptedJson", acceptedJson.isPresent(), is(true));
        assertThat("acceptedJson has timestamp", acceptedJson.get().has(SHADOW_DOCUMENT_TIMESTAMP), is(true));
        ((ObjectNode) acceptedJson.get()).remove(SHADOW_DOCUMENT_TIMESTAMP);
        Optional<JsonNode> documentsJson = JsonUtil.getPayloadJson(acceptRequestCaptor.getAllValues().get(1).getPayload());
        assertThat("Retrieved documentsJson", documentsJson.isPresent(), is(true));
        assertThat("documentsJson has timestamp", documentsJson.get().has(SHADOW_DOCUMENT_TIMESTAMP), is(true));
        ((ObjectNode) documentsJson.get()).remove(SHADOW_DOCUMENT_TIMESTAMP);

        // verify pubsub payloads match expected output
        assertThat(acceptedJson.get(), is(equalTo(expectedAcceptedJson.get())));

        // TODO: broken until partial update PR added, uncomment once added
        // assertThat(documentsJson.get(), is(equalTo(expectedDocumentsJson.get())));

        // verify each pubsub call (accept, documents) had expected values
        for (int i = 0; i < acceptRequestCaptor.getAllValues().size(); i++) {
            assertThat(acceptRequestCaptor.getAllValues().get(i).getShadowName(), is(equalTo(shadowName)));
            assertThat(acceptRequestCaptor.getAllValues().get(i).getThingName(), is(equalTo(THING_NAME)));
            assertThat("Expected operation", acceptRequestCaptor.getAllValues().get(i).getPublishOperation(), is(Operation.UPDATE_SHADOW));
            assertThat("Expected log code", acceptRequestCaptor.getAllValues().get(i).getPublishOperation().getLogEventType(), is(LogEvents.UPDATE_THING_SHADOW.code()));
        }
    }

    @ParameterizedTest
    @EmptySource
    @ValueSource(strings = {SHADOW_NAME})
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

        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandlerWrapper, mockPubSubClientWrapper);
        when(mockDao.getShadowThing(any(), any())).thenReturn(Optional.of(initialDocument));
        when(mockDao.updateShadowThing(any(), any(), any())).thenReturn(Optional.of(updateDocument));

        UpdateThingShadowResponse actualResponse = updateThingShadowIPCHandler.handleRequest(request);
        Optional<JsonNode> updatedDocumentJson = JsonUtil.getPayloadJson(actualResponse.getPayload());
        assertThat("Retrieved updateDocumentJson", updatedDocumentJson.isPresent(), is(true));
        assertThat("updateDocumentJson has timestamp", updatedDocumentJson.get().has(SHADOW_DOCUMENT_TIMESTAMP), is(true));
        ((ObjectNode) updatedDocumentJson.get()).remove(SHADOW_DOCUMENT_TIMESTAMP);
        Optional<JsonNode> expectedAcceptedJson = JsonUtil.getPayloadJson(updateRequest);
        assertThat("Retrieved expectedAcceptedJson", expectedAcceptedJson.isPresent(), is(true));
        ((ObjectNode) expectedAcceptedJson.get()).set(SHADOW_DOCUMENT_VERSION, new IntNode(1));

        assertThat(updatedDocumentJson.get(), is(equalTo(expectedAcceptedJson.get())));

        verify(mockPubSubClientWrapper, times(1)).accept(acceptRequestCaptor.capture());
        verify(mockPubSubClientWrapper, times(0)).delta(acceptRequestCaptor.capture());
        verify(mockPubSubClientWrapper, times(1)).documents(acceptRequestCaptor.capture());

        assertThat(acceptRequestCaptor.getAllValues().size(), is(equalTo(2)));

        Optional<JsonNode> expectedDeltaJson = JsonUtil.getPayloadJson(deltaPayload);
        assertThat("Found expectedDeltaJson", expectedDeltaJson.isPresent(), is(true));
        Optional<JsonNode> expectedDocumentsJson = JsonUtil.getPayloadJson(documentsPayload);
        assertThat("Found expectedDocumentsJson", expectedDocumentsJson.isPresent(), is(true));

        Optional<JsonNode> acceptedJson = JsonUtil.getPayloadJson(acceptRequestCaptor.getAllValues().get(0).getPayload());
        assertThat("Retrieved acceptedJson", acceptedJson.isPresent(), is(true));
        assertThat("acceptedJson has timestamp", acceptedJson.get().has(SHADOW_DOCUMENT_TIMESTAMP), is(true));
        ((ObjectNode) acceptedJson.get()).remove(SHADOW_DOCUMENT_TIMESTAMP);
        Optional<JsonNode> documentsJson = JsonUtil.getPayloadJson(acceptRequestCaptor.getAllValues().get(1).getPayload());
        assertThat("Retrieved documentsJson", documentsJson.isPresent(), is(true));
        assertThat("documentsJson has timestamp", documentsJson.get().has(SHADOW_DOCUMENT_TIMESTAMP), is(true));
        ((ObjectNode) documentsJson.get()).remove(SHADOW_DOCUMENT_TIMESTAMP);

        // verify pubsub payloads match expected output
        assertThat(acceptedJson.get(), is(equalTo(expectedAcceptedJson.get())));
        assertThat(documentsJson.get(), is(equalTo(expectedDocumentsJson.get())));

        // verify each pubsub call (accept, documents) had expected values
        for (int i = 0; i < acceptRequestCaptor.getAllValues().size(); i++) {
            assertThat(acceptRequestCaptor.getAllValues().get(i).getShadowName(), is(equalTo(shadowName)));
            assertThat(acceptRequestCaptor.getAllValues().get(i).getThingName(), is(equalTo(THING_NAME)));
            assertThat("Expected operation", acceptRequestCaptor.getAllValues().get(i).getPublishOperation(), is(Operation.UPDATE_SHADOW));
            assertThat("Expected log code", acceptRequestCaptor.getAllValues().get(i).getPublishOperation().getLogEventType(), is(LogEvents.UPDATE_THING_SHADOW.code()));
        }
    }

    @ParameterizedTest
    @EmptySource
    @ValueSource(strings = {SHADOW_NAME})
    void GIVEN_update_thing_shadow_request_with_add_and_remove_nodes_in_desired_and_existing_shadow_WHEN_handle_request_THEN_update_thing_shadow(String shadowName) throws IOException, URISyntaxException {
        byte[] initialDocument = getJsonFromResource(RESOURCE_DIRECTORY_NAME + GOOD_INITIAL_DOCUMENT_FILE_NAME);
        byte[] updateRequest = getJsonFromResource(RESOURCE_DIRECTORY_NAME + GOOD_UPDATE_DOCUMENT_WITH_DESIRED_AND_ADD_REMOVE_NODE_REQUEST_FILE_NAME);
        byte[] updateDocument = getJsonFromResource(RESOURCE_DIRECTORY_NAME + "good_new_document_with_add_delete_update.json");
        byte[] documentsPayload = getJsonFromResource(RESOURCE_DIRECTORY_NAME + "good_documents_payload_after_new_node_desired.json");
        byte[] deltaPayload = getJsonFromResource(RESOURCE_DIRECTORY_NAME + "good_delta_node_with_new_node.json");

        Optional<JsonNode> expectedAcceptedJson = JsonUtil.getPayloadJson(updateRequest);
        assertThat("Found expectedAcceptedJson", expectedAcceptedJson.isPresent(), is(true));
        ((ObjectNode) expectedAcceptedJson.get()).set(SHADOW_DOCUMENT_VERSION, new IntNode(1));
        Optional<JsonNode> expectedDeltaJson = JsonUtil.getPayloadJson(deltaPayload);
        assertThat("Found expectedDeltaJson", expectedDeltaJson.isPresent(), is(true));
        Optional<JsonNode> expectedDocumentsJson = JsonUtil.getPayloadJson(documentsPayload);
        assertThat("Found expectedDocumentsJson", expectedDocumentsJson.isPresent(), is(true));

        UpdateThingShadowRequest request = new UpdateThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(shadowName);
        request.setPayload(updateRequest);

        UpdateThingShadowResponse expectedResponse = new UpdateThingShadowResponse();
        expectedResponse.setPayload(updateDocument);

        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandlerWrapper, mockPubSubClientWrapper);
        when(mockDao.getShadowThing(any(), any())).thenReturn(Optional.of(initialDocument));
        when(mockDao.updateShadowThing(any(), any(), any())).thenReturn(Optional.of(updateDocument));

        UpdateThingShadowResponse actualResponse = updateThingShadowIPCHandler.handleRequest(request);
        Optional<JsonNode> updatedDocumentJson = JsonUtil.getPayloadJson(actualResponse.getPayload());
        assertThat("Retrieved updatedDocumentJson", updatedDocumentJson.isPresent(), is(true));
        assertThat("updatedDocumentJson has timestamp", updatedDocumentJson.get().has(SHADOW_DOCUMENT_TIMESTAMP), is(true));
        ((ObjectNode) updatedDocumentJson.get()).remove(SHADOW_DOCUMENT_TIMESTAMP);

        assertThat(updatedDocumentJson.get(), is(equalTo(expectedAcceptedJson.get())));

        verify(mockPubSubClientWrapper, times(1)).accept(acceptRequestCaptor.capture());
        verify(mockPubSubClientWrapper, times(1)).delta(acceptRequestCaptor.capture());
        verify(mockPubSubClientWrapper, times(1)).documents(acceptRequestCaptor.capture());
        assertThat(acceptRequestCaptor.getAllValues().size(), is(equalTo(3)));

        Optional<JsonNode> acceptedJson = JsonUtil.getPayloadJson(acceptRequestCaptor.getAllValues().get(0).getPayload());
        assertThat("Retrieved acceptedJson", acceptedJson.isPresent(), is(true));
        assertThat("acceptedJson has timestamp", acceptedJson.get().has(SHADOW_DOCUMENT_TIMESTAMP), is(true));
        ((ObjectNode) acceptedJson.get()).remove(SHADOW_DOCUMENT_TIMESTAMP);
        Optional<JsonNode> deltaJson = JsonUtil.getPayloadJson(acceptRequestCaptor.getAllValues().get(1).getPayload());
        assertThat("Retrieved deltaJson", deltaJson.isPresent(), is(true));
        assertThat("deltaJson has timestamp", deltaJson.get().has(SHADOW_DOCUMENT_TIMESTAMP), is(true));
        ((ObjectNode) deltaJson.get()).remove(SHADOW_DOCUMENT_TIMESTAMP);
        Optional<JsonNode> documentsJson = JsonUtil.getPayloadJson(acceptRequestCaptor.getAllValues().get(2).getPayload());
        assertThat("Retrieved documentsJson", documentsJson.isPresent(), is(true));
        assertThat("documentsJson has timestamp", documentsJson.get().has(SHADOW_DOCUMENT_TIMESTAMP), is(true));
        ((ObjectNode) documentsJson.get()).remove(SHADOW_DOCUMENT_TIMESTAMP);

        // verify pubsub payloads match expected output
        assertThat(acceptedJson.get(), is(equalTo(expectedAcceptedJson.get())));
        assertThat(deltaJson.get(), is(equalTo(expectedDeltaJson.get())));
        assertThat(documentsJson.get(), is(equalTo(expectedDocumentsJson.get())));

        // verify each pubsub call (accept, delta, documents) had expected values
        for (int i = 0; i < acceptRequestCaptor.getAllValues().size(); i++) {
            assertThat(acceptRequestCaptor.getAllValues().get(i).getShadowName(), is(equalTo(shadowName)));
            assertThat(acceptRequestCaptor.getAllValues().get(i).getThingName(), is(equalTo(THING_NAME)));
            assertThat("Expected operation", acceptRequestCaptor.getAllValues().get(i).getPublishOperation(), is(Operation.UPDATE_SHADOW));
            assertThat("Expected log code", acceptRequestCaptor.getAllValues().get(i).getPublishOperation().getLogEventType(), is(LogEvents.UPDATE_THING_SHADOW.code()));
        }
    }

    @ParameterizedTest
    @EmptySource
    @ValueSource(strings = {SHADOW_NAME})
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

        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandlerWrapper, mockPubSubClientWrapper);
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

        assertThat(updatedDocumentJson.get(), is(expectedAcceptedJson.get()));

        verify(mockPubSubClientWrapper, times(1)).accept(acceptRequestCaptor.capture());
        verify(mockPubSubClientWrapper, times(1)).delta(acceptRequestCaptor.capture());
        verify(mockPubSubClientWrapper, times(1)).documents(acceptRequestCaptor.capture());
        assertThat(acceptRequestCaptor.getAllValues().size(), is(equalTo(3)));

        Optional<JsonNode> expectedDeltaJson = JsonUtil.getPayloadJson(deltaPayload);
        assertThat("Found expectedDeltaJson", expectedDeltaJson.isPresent(), is(true));
        ((ObjectNode) expectedDeltaJson.get().get(SHADOW_DOCUMENT_STATE).get("color")).set("r", new IntNode(255));
        ((ObjectNode) expectedDeltaJson.get()).set(SHADOW_DOCUMENT_VERSION, new IntNode(0));
        Optional<JsonNode> expectedDocumentsJson = JsonUtil.getPayloadJson(documentsPayload);
        assertThat("Found expectedDocumentsJson", expectedDocumentsJson.isPresent(), is(true));

        Optional<JsonNode> acceptedJson = JsonUtil.getPayloadJson(acceptRequestCaptor.getAllValues().get(0).getPayload());
        assertThat("Retrieved acceptedJson", acceptedJson.isPresent(), is(true));
        assertThat("acceptedJson has timestamp", acceptedJson.get().has(SHADOW_DOCUMENT_TIMESTAMP), is(true));
        ((ObjectNode) acceptedJson.get()).remove(SHADOW_DOCUMENT_TIMESTAMP);
        Optional<JsonNode> deltaJson = JsonUtil.getPayloadJson(acceptRequestCaptor.getAllValues().get(1).getPayload());
        assertThat("Retrieved deltaJson", deltaJson.isPresent(), is(true));
        assertThat("deltaJson has timestamp", deltaJson.get().has(SHADOW_DOCUMENT_TIMESTAMP), is(true));
        ((ObjectNode) deltaJson.get()).remove(SHADOW_DOCUMENT_TIMESTAMP);
        Optional<JsonNode> documentsJson = JsonUtil.getPayloadJson(acceptRequestCaptor.getAllValues().get(2).getPayload());
        assertThat("Retrieved documentsJson", documentsJson.isPresent(), is(true));
        assertThat("documentsJson has timestamp", documentsJson.get().has(SHADOW_DOCUMENT_TIMESTAMP), is(true));
        ((ObjectNode) documentsJson.get()).remove(SHADOW_DOCUMENT_TIMESTAMP);

        // verify pubsub payloads match expected output
        assertThat(acceptedJson.get(), is(equalTo(expectedAcceptedJson.get())));
        assertThat(deltaJson.get(), is(equalTo(expectedDeltaJson.get())));
        assertThat(documentsJson.get(), is(equalTo(expectedDocumentsJson.get())));

        // verify each pubsub call (accept, delta, documents) had expected values
        for (int i = 0; i < acceptRequestCaptor.getAllValues().size(); i++) {
            assertThat(acceptRequestCaptor.getAllValues().get(i).getShadowName(), is(equalTo(shadowName)));
            assertThat(acceptRequestCaptor.getAllValues().get(i).getThingName(), is(equalTo(THING_NAME)));
            assertThat("Expected operation", acceptRequestCaptor.getAllValues().get(i).getPublishOperation(), is(Operation.UPDATE_SHADOW));
            assertThat("Expected log code", acceptRequestCaptor.getAllValues().get(i).getPublishOperation().getLogEventType(), is(LogEvents.UPDATE_THING_SHADOW.code()));
        }
    }

    @Test
    void GIVEN_shadow_manager_data_exception_from_update_query_WHEN_handle_request_THEN_throw_service_error(ExtensionContext context) throws IOException, URISyntaxException {
        byte[] initialDocument = getJsonFromResource(RESOURCE_DIRECTORY_NAME + GOOD_INITIAL_DOCUMENT_FILE_NAME);
        byte[] updateRequest = getJsonFromResource(RESOURCE_DIRECTORY_NAME + GOOD_UPDATE_DOCUMENT_WITH_DESIRED_REQUEST_FILE_NAME);
        ignoreExceptionOfType(context, ShadowManagerDataException.class);
        UpdateThingShadowRequest request = new UpdateThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);
        request.setPayload(updateRequest);

        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandlerWrapper, mockPubSubClientWrapper);
        when(mockDao.getShadowThing(any(), any())).thenReturn(Optional.of(initialDocument));
        doThrow(new ShadowManagerDataException(new Exception(SAMPLE_EXCEPTION_MESSAGE))).when(mockDao).updateShadowThing(any(), any(), any());
        ServiceError thrown = assertThrows(ServiceError.class, () -> updateThingShadowIPCHandler.handleRequest(request));
        assertThat(thrown.getMessage(), containsString(SAMPLE_EXCEPTION_MESSAGE));

        verify(mockPubSubClientWrapper, times(1)).reject(rejectRequestCaptor.capture());

        assertThat(rejectRequestCaptor.getValue(), is(notNullValue()));
        assertThat(rejectRequestCaptor.getValue().getShadowName(), is(equalTo(SHADOW_NAME)));
        assertThat("Expected operation", rejectRequestCaptor.getValue().getPublishOperation(), is(Operation.UPDATE_SHADOW));
        assertThat("Expected log code", rejectRequestCaptor.getValue().getPublishOperation().getLogEventType(), is(LogEvents.UPDATE_THING_SHADOW.code()));

        ErrorMessage errorMessage = rejectRequestCaptor.getValue().getErrorMessage();
        assertThat(errorMessage.getTimestamp(), is(not(equalTo(Instant.EPOCH.toEpochMilli()))));
        assertThat(errorMessage.getErrorCode(), is(500));
        assertThat(errorMessage.getMessage(), startsWith("Internal service failure"));
    }

    @Test
    void GIVEN_shadow_manager_data_exception_from_get_query_WHEN_handle_request_THEN_throw_service_error(ExtensionContext context) throws IOException, URISyntaxException {
        byte[] updateRequest = getJsonFromResource(RESOURCE_DIRECTORY_NAME + GOOD_UPDATE_DOCUMENT_WITH_DESIRED_REQUEST_FILE_NAME);
        ignoreExceptionOfType(context, ShadowManagerDataException.class);
        UpdateThingShadowRequest request = new UpdateThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);
        request.setPayload(updateRequest);

        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandlerWrapper, mockPubSubClientWrapper);
        doThrow(new ShadowManagerDataException(new Exception(SAMPLE_EXCEPTION_MESSAGE))).when(mockDao).getShadowThing(any(), any());
        ServiceError thrown = assertThrows(ServiceError.class, () -> updateThingShadowIPCHandler.handleRequest(request));
        assertThat(thrown.getMessage(), containsString(SAMPLE_EXCEPTION_MESSAGE));

        verify(mockPubSubClientWrapper, times(1)).reject(rejectRequestCaptor.capture());

        assertThat(rejectRequestCaptor.getValue(), is(notNullValue()));
        assertThat(rejectRequestCaptor.getValue().getShadowName(), is(equalTo(SHADOW_NAME)));
        assertThat("Expected operation", rejectRequestCaptor.getValue().getPublishOperation(), is(Operation.UPDATE_SHADOW));
        assertThat("Expected log code", rejectRequestCaptor.getValue().getPublishOperation().getLogEventType(), is(LogEvents.UPDATE_THING_SHADOW.code()));

        ErrorMessage errorMessage = rejectRequestCaptor.getValue().getErrorMessage();
        assertThat(errorMessage.getTimestamp(), is(not(equalTo(Instant.EPOCH.toEpochMilli()))));
        assertThat(errorMessage.getErrorCode(), is(500));
        assertThat(errorMessage.getMessage(), startsWith("Internal service failure"));
    }

    @Test
    void GIVEN_unauthorized_service_WHEN_handle_request_THEN_throw_unauthorized_error(ExtensionContext context) throws Exception {
        byte[] updateRequest = getJsonFromResource(RESOURCE_DIRECTORY_NAME + GOOD_UPDATE_DOCUMENT_WITH_DESIRED_REQUEST_FILE_NAME);
        ignoreExceptionOfType(context, AuthorizationException.class);
        UpdateThingShadowRequest request = new UpdateThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);
        request.setPayload(updateRequest);
        doThrow(new AuthorizationException(SAMPLE_EXCEPTION_MESSAGE)).when(mockAuthorizationHandlerWrapper).doAuthorization(any(), any(), any(), any());

        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandlerWrapper, mockPubSubClientWrapper);
        UnauthorizedError thrown = assertThrows(UnauthorizedError.class, () -> updateThingShadowIPCHandler.handleRequest(request));
        assertThat(thrown.getMessage(), is(equalTo(SAMPLE_EXCEPTION_MESSAGE)));

        verify(mockPubSubClientWrapper, times(1)).reject(rejectRequestCaptor.capture());

        assertThat(rejectRequestCaptor.getValue(), is(notNullValue()));
        assertThat(rejectRequestCaptor.getValue().getShadowName(), is(equalTo(SHADOW_NAME)));
        assertThat("Expected operation", rejectRequestCaptor.getValue().getPublishOperation(), is(Operation.UPDATE_SHADOW));
        assertThat("Expected log code", rejectRequestCaptor.getValue().getPublishOperation().getLogEventType(), is(LogEvents.UPDATE_THING_SHADOW.code()));

        ErrorMessage errorMessage = rejectRequestCaptor.getValue().getErrorMessage();
        assertThat(errorMessage.getErrorCode(), is(401));
        assertThat(errorMessage.getMessage(), Matchers.startsWith("Unauthorized"));
    }

    @ParameterizedTest
    @MethodSource("com.aws.greengrass.shadowmanager.TestUtils#invalidThingAndShadowName")
    void GIVEN_invalid_thing_or_shadow_name_WHEN_handle_request_THEN_throw_invalid_arguments_error(String thingName, String shadowName,ExtensionContext context) throws IOException, URISyntaxException {
        byte[] updateRequest = getJsonFromResource(RESOURCE_DIRECTORY_NAME + GOOD_UPDATE_DOCUMENT_WITH_DESIRED_REQUEST_FILE_NAME);
        ignoreExceptionOfType(context, InvalidRequestParametersException.class);
        UpdateThingShadowRequest request = new UpdateThingShadowRequest();
        request.setThingName(thingName);
        request.setShadowName(shadowName);
        request.setPayload(updateRequest);

        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandlerWrapper, mockPubSubClientWrapper);
        InvalidArgumentsError thrown = assertThrows(InvalidArgumentsError.class, () -> updateThingShadowIPCHandler.handleRequest(request));
        assertThat(thrown.getMessage(),either(startsWith("ShadowName")).or(startsWith("ThingName")));

        verify(mockPubSubClientWrapper, times(1)).reject(rejectRequestCaptor.capture());

        assertThat(rejectRequestCaptor.getValue(), is(notNullValue()));
        assertThat(rejectRequestCaptor.getValue().getShadowName(), is(equalTo(shadowName)));
        assertThat("Expected operation", rejectRequestCaptor.getValue().getPublishOperation(), is(Operation.UPDATE_SHADOW));
        assertThat("Expected log code", rejectRequestCaptor.getValue().getPublishOperation().getLogEventType(), is(LogEvents.UPDATE_THING_SHADOW.code()));

        ErrorMessage errorMessage = rejectRequestCaptor.getValue().getErrorMessage();
        assertThat(errorMessage.getTimestamp(), is(not(equalTo(Instant.EPOCH.toEpochMilli()))));
        assertThat(errorMessage.getErrorCode(), is(400));
        assertThat(errorMessage.getMessage(),either(startsWith("ShadowName")).or(startsWith("ThingName")));
    }

    @Test
    void GIVEN_unexpected_empty_return_during_update_query_WHEN_handle_request_THEN_throw_service_error(ExtensionContext context) throws IOException, URISyntaxException {
        byte[] initialDocument = getJsonFromResource(RESOURCE_DIRECTORY_NAME + GOOD_INITIAL_DOCUMENT_FILE_NAME);
        byte[] updateRequest = getJsonFromResource(RESOURCE_DIRECTORY_NAME + GOOD_UPDATE_DOCUMENT_WITH_DESIRED_REQUEST_FILE_NAME);
        ignoreExceptionOfType(context, ServiceError.class);
        UpdateThingShadowRequest request = new UpdateThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);
        request.setPayload(updateRequest);

        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandlerWrapper, mockPubSubClientWrapper);
        when(mockDao.getShadowThing(any(), any())).thenReturn(Optional.of(initialDocument));
        when(mockDao.updateShadowThing(any(), any(), any())).thenReturn(Optional.empty());

        ServiceError thrown = assertThrows(ServiceError.class, () -> updateThingShadowIPCHandler.handleRequest(request));
        assertThat(thrown.getMessage(), startsWith("Unexpected error"));

        verify(mockPubSubClientWrapper, times(1))
                .reject(rejectRequestCaptor.capture());

        assertThat(rejectRequestCaptor.getValue(), is(notNullValue()));
        assertThat(rejectRequestCaptor.getValue().getShadowName(), is(equalTo(SHADOW_NAME)));
        assertThat("Expected operation", rejectRequestCaptor.getValue().getPublishOperation(), is(Operation.UPDATE_SHADOW));
        assertThat("Expected log code", rejectRequestCaptor.getValue().getPublishOperation().getLogEventType(), is(LogEvents.UPDATE_THING_SHADOW.code()));

        ErrorMessage errorMessage = rejectRequestCaptor.getValue().getErrorMessage();
        assertThat(errorMessage.getTimestamp(), is(not(equalTo(Instant.EPOCH.toEpochMilli()))));
        assertThat(errorMessage.getErrorCode(), is(500));
        assertThat(errorMessage.getMessage(), startsWith("Internal service failure"));
    }

    // reusable function to verify InvalidArgumentsError from faulty update requests
    private void assertInvalidArgumentsErrorFromPayloadUpdate(byte[] initialDocument, byte[] badUpdateRequest, String expectedErrorMessage, int expectedErrorCode, ExtensionContext context) {
        ignoreExceptionOfType(context, InvalidRequestParametersException.class);
        UpdateThingShadowRequest request = new UpdateThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);
        request.setPayload(badUpdateRequest);

        if (initialDocument != null) {
            when(mockDao.getShadowThing(any(), any())).thenReturn(Optional.of(initialDocument));
        }

        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandlerWrapper, mockPubSubClientWrapper);

        InvalidArgumentsError thrown = assertThrows(InvalidArgumentsError.class, () -> updateThingShadowIPCHandler.handleRequest(request));
        assertThat(thrown.getMessage().trim(), is(equalTo(expectedErrorMessage)));

        verify(mockPubSubClientWrapper, times(1))
                .reject(rejectRequestCaptor.capture());

        assertThat(rejectRequestCaptor.getValue(), is(notNullValue()));
        assertThat(rejectRequestCaptor.getValue().getShadowName(), is(equalTo(SHADOW_NAME)));
        assertThat("Expected operation", rejectRequestCaptor.getValue().getPublishOperation(), is(Operation.UPDATE_SHADOW));
        assertThat("Expected log code", rejectRequestCaptor.getValue().getPublishOperation().getLogEventType(), is(LogEvents.UPDATE_THING_SHADOW.code()));

        ErrorMessage errorMessage = rejectRequestCaptor.getValue().getErrorMessage();
        assertThat(errorMessage.getErrorCode(), is(expectedErrorCode));
        assertThat(errorMessage.getMessage(), is(equalTo(expectedErrorMessage)));
    }

    @ParameterizedTest
    @NullAndEmptySource
    void GIVEN_missing_payload_WHEN_handle_request_THEN_throw_invalid_arguments_error(byte[] updatePayload, ExtensionContext context) {
        String expectedErrorString = "Missing update payload";
        int expectedErrorCode = 400;
        assertInvalidArgumentsErrorFromPayloadUpdate(null, updatePayload, expectedErrorString, expectedErrorCode, context);
    }

    @Test
    void GIVEN_bad_update_document_with_no_state_node_WHEN_handle_request_THEN_throw_invalid_argument_error_and_send_message_on_rejected_topic(ExtensionContext context) throws IOException, URISyntaxException {
        byte[] badUpdateRequest = getJsonFromResource(RESOURCE_DIRECTORY_NAME + BAD_UPDATE_DOCUMENT_WITHOUT_STATE_NODE_FILE_NAME);
        String expectedErrorString = "Invalid JSON\nobject has missing required properties ([\"state\"])";
        int expectedErrorCode = 400;
        assertInvalidArgumentsErrorFromPayloadUpdate(null, badUpdateRequest, expectedErrorString, expectedErrorCode, context);
    }

    @Test
    void GIVEN_bad_update_with_non_int_version_WHEN_handle_request_THEN_throw_invalid_argument_error_and_send_message_on_rejected_topic(ExtensionContext context)
            throws IOException, URISyntaxException {
        byte[] badUpdateRequest = getJsonFromResource(RESOURCE_DIRECTORY_NAME + "bad_version_update_document.json");
        String expectedErrorString = "Invalid JSON\nInvalid version. instance type (string) does not match any allowed primitive type (allowed: [\"integer\",\"number\"])";
        int expectedErrorCode = 400;
        assertInvalidArgumentsErrorFromPayloadUpdate(null, badUpdateRequest, expectedErrorString, expectedErrorCode, context);
    }

    @Test
    void GIVEN_bad_update_with_much_higher_version_than_existing_shadow_WHEN_handle_request_THEN_throw_conflict_error_and_send_message_on_rejected_topic(ExtensionContext context)
            throws IOException, URISyntaxException {
        byte[] initialDocument = getJsonFromResource(RESOURCE_DIRECTORY_NAME + GOOD_INITIAL_DOCUMENT_FILE_NAME);
        byte[] badUpdateRequest = getJsonFromResource(RESOURCE_DIRECTORY_NAME + "bad_version_update_document_with_higher_version_number.json");
        ignoreExceptionOfType(context, ConflictError.class);
        UpdateThingShadowRequest request = new UpdateThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);
        request.setPayload(badUpdateRequest);

        when(mockDao.getShadowThing(any(), any())).thenReturn(Optional.of(initialDocument));

        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandlerWrapper, mockPubSubClientWrapper);

        ConflictError thrown = assertThrows(ConflictError.class, () -> updateThingShadowIPCHandler.handleRequest(request));
        assertThat(thrown.getMessage().trim(), is(equalTo("Invalid version")));

        verify(mockPubSubClientWrapper, times(1))
                .reject(rejectRequestCaptor.capture());

        assertThat(rejectRequestCaptor.getValue(), is(notNullValue()));
        assertThat(rejectRequestCaptor.getValue().getShadowName(), is(equalTo(SHADOW_NAME)));
        assertThat("Expected operation", rejectRequestCaptor.getValue().getPublishOperation(), is(Operation.UPDATE_SHADOW));
        assertThat("Expected log code", rejectRequestCaptor.getValue().getPublishOperation().getLogEventType(), is(LogEvents.UPDATE_THING_SHADOW.code()));

        ErrorMessage errorMessage = rejectRequestCaptor.getValue().getErrorMessage();
        assertThat(errorMessage.getErrorCode(), is(409));
        assertThat(errorMessage.getMessage(), is(equalTo("Version conflict")));
    }

    @Test
    void GIVEN_bad_update_with_state_node_as_value_node_WHEN_handle_request_THEN_throw_invalid_argument_error_and_send_message_on_rejected_topic(ExtensionContext context)
            throws IOException, URISyntaxException {
        byte[] initialDocument = getJsonFromResource(RESOURCE_DIRECTORY_NAME + GOOD_INITIAL_DOCUMENT_FILE_NAME);
        byte[] badUpdateRequest = getJsonFromResource(RESOURCE_DIRECTORY_NAME + "bad_update_document_with_state_node_as_value_node.json");
        String expectedErrorString = "Invalid JSON\nInvalid state. instance type (string) does not match any allowed primitive type (allowed: [\"object\"])";
        int expectedErrorCode = 400;
        assertInvalidArgumentsErrorFromPayloadUpdate(initialDocument, badUpdateRequest, expectedErrorString, expectedErrorCode, context);
    }

    @Test
    void GIVEN_bad_update_with_state_node_as_empty_node_WHEN_handle_request_THEN_throw_invalid_argument_error_and_send_message_on_rejected_topic(ExtensionContext context)
            throws IOException, URISyntaxException {
        byte[] initialDocument = getJsonFromResource(RESOURCE_DIRECTORY_NAME + GOOD_INITIAL_DOCUMENT_FILE_NAME);
        byte[] badUpdateRequest = getJsonFromResource(RESOURCE_DIRECTORY_NAME + "bad_update_document_with_state_node_as_empty_node.json");
        String expectedErrorString = "Invalid JSON\nState node needs to have to either reported or desired node.";
        int expectedErrorCode = 400;
        assertInvalidArgumentsErrorFromPayloadUpdate(initialDocument, badUpdateRequest, expectedErrorString, expectedErrorCode, context);
    }

    @Test
    void GIVEN_bad_update_with_state_depth_greater_than_max_depth_WHEN_handle_request_THEN_throw_invalid_argument_error_and_send_message_on_rejected_topic(ExtensionContext context)
            throws IOException, URISyntaxException {
        byte[] initialDocument = getJsonFromResource(RESOURCE_DIRECTORY_NAME + GOOD_INITIAL_DOCUMENT_FILE_NAME);
        byte[] badUpdateRequest = getJsonFromResource(RESOURCE_DIRECTORY_NAME + "bad_update_document_with_depth.json");
        String expectedErrorString = "JSON contains too many levels of nesting; maximum is 6";
        int expectedErrorCode = 400;
        assertInvalidArgumentsErrorFromPayloadUpdate(initialDocument, badUpdateRequest, expectedErrorString, expectedErrorCode, context);
    }

    @Test
    void GIVEN_bad_update_with_payload_size_greater_than_max_size_WHEN_handle_request_THEN_throw_invalid_argument_error_and_send_message_on_rejected_topic(ExtensionContext context)
            throws IOException, URISyntaxException {
        byte[] initialDocument = getJsonFromResource(RESOURCE_DIRECTORY_NAME + GOOD_INITIAL_DOCUMENT_FILE_NAME);
        byte[] badUpdateRequest = new byte[DEFAULT_DOCUMENT_SIZE + 1];
        String expectedErrorString = "The payload exceeds the maximum size allowed";
        int expectedErrorCode = 413;
        assertInvalidArgumentsErrorFromPayloadUpdate(initialDocument, badUpdateRequest, expectedErrorString, expectedErrorCode, context);
    }

    @Test
    void GIVEN_update_thing_shadow_ipc_handler_WHEN_handle_stream_event_THEN_nothing_happens() {
        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandlerWrapper, mockPubSubClientWrapper);
        assertDoesNotThrow(() -> updateThingShadowIPCHandler.handleStreamEvent(mock(EventStreamJsonMessage.class)));
    }

    @Test
    void GIVEN_update_thing_shadow_ipc_handler_WHEN_stream_closes_THEN_nothing_happens() {
        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandlerWrapper, mockPubSubClientWrapper);
        assertDoesNotThrow(updateThingShadowIPCHandler::onStreamClosed);
    }
}
