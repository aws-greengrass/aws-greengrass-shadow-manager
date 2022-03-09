/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager;

import com.aws.greengrass.builtin.services.pubsub.PublishEvent;
import com.aws.greengrass.shadowmanager.exception.InvalidRequestParametersException;
import com.aws.greengrass.shadowmanager.ipc.DeleteThingShadowRequestHandler;
import com.aws.greengrass.shadowmanager.ipc.GetThingShadowRequestHandler;
import com.aws.greengrass.shadowmanager.ipc.PubSubClientWrapper;
import com.aws.greengrass.shadowmanager.ipc.UpdateThingShadowRequestHandler;
import com.aws.greengrass.shadowmanager.model.UpdateThingShadowHandlerResponse;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.aws.greengrass.model.ConflictError;
import software.amazon.awssdk.aws.greengrass.model.DeleteThingShadowRequest;
import software.amazon.awssdk.aws.greengrass.model.DeleteThingShadowResponse;
import software.amazon.awssdk.aws.greengrass.model.GetThingShadowRequest;
import software.amazon.awssdk.aws.greengrass.model.GetThingShadowResponse;
import software.amazon.awssdk.aws.greengrass.model.InvalidArgumentsError;
import software.amazon.awssdk.aws.greengrass.model.ResourceNotFoundError;
import software.amazon.awssdk.aws.greengrass.model.ServiceError;
import software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest;

import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.aws.greengrass.shadowmanager.TestUtils.SHADOW_NAME;
import static com.aws.greengrass.shadowmanager.model.Constants.CLASSIC_SHADOW_IDENTIFIER;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_MANAGER_NAME;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class PubSubIntegratorTest {
    private static final byte[] PAYLOAD = "{\"version\": 10, \"state\": {\"reported\": {\"name\": \"The Beach Boys\", \"NewField\": 100}, \"desired\": {\"name\": \"Pink Floyd\", \"SomethingNew\": true}}}".getBytes();
    private static final String MOCK_THING = "thing1";
    private static final String MOCK_SHADOW = "shadow1";

    @Mock
    private DeleteThingShadowRequestHandler mockDeleteThingShadowRequestHandler;
    @Mock
    private UpdateThingShadowRequestHandler mockUpdateThingShadowRequestHandler;
    @Mock
    private GetThingShadowRequestHandler mockGetThingShadowRequestHandler;
    @Mock
    private PubSubClientWrapper mockPubSubClientWrapper;

    @Captor
    private ArgumentCaptor<Consumer<PublishEvent>> publishEventCaptor;
    @Captor
    private ArgumentCaptor<UpdateThingShadowRequest> updateThingShadowRequestCaptor;
    @Captor
    private ArgumentCaptor<DeleteThingShadowRequest> deleteThingShadowRequestCaptor;
    @Captor
    private ArgumentCaptor<GetThingShadowRequest> getThingShadowRequestCaptor;

    @SuppressWarnings("PMD.UnusedPrivateMethod")
    private static Stream<Arguments> classicAndNamedShadow() {
        return Stream.of(
                arguments(SHADOW_NAME, "update"),
                arguments(SHADOW_NAME, "delete"),
                arguments(SHADOW_NAME, "get"),
                arguments(SHADOW_NAME, "badOp"),
                arguments(CLASSIC_SHADOW_IDENTIFIER, "update"),
                arguments(CLASSIC_SHADOW_IDENTIFIER, "delete"),
                arguments(CLASSIC_SHADOW_IDENTIFIER, "get"),
                arguments(CLASSIC_SHADOW_IDENTIFIER, "badOp")
        );
    }


    @BeforeEach
    void setup() {
        lenient().when(mockUpdateThingShadowRequestHandler.handleRequest(updateThingShadowRequestCaptor.capture(), eq(SHADOW_MANAGER_NAME))).thenReturn(mock(UpdateThingShadowHandlerResponse.class));
        lenient().when(mockDeleteThingShadowRequestHandler.handleRequest(deleteThingShadowRequestCaptor.capture(), eq(SHADOW_MANAGER_NAME))).thenReturn(mock(DeleteThingShadowResponse.class));
        lenient().when(mockGetThingShadowRequestHandler.handleRequest(getThingShadowRequestCaptor.capture(), eq(SHADOW_MANAGER_NAME))).thenReturn(mock(GetThingShadowResponse.class));
        lenient().doNothing().when(mockPubSubClientWrapper).subscribe(publishEventCaptor.capture());
    }

    @Test
    void GIVEN_pubsubIntegrator_WHEN_multiple_subscribes_and_unsubscribes_THEN_only_subscribes_and_unsubscribes_once() {
        PubSubIntegrator integrator = new PubSubIntegrator(mockPubSubClientWrapper, mockDeleteThingShadowRequestHandler,
                mockUpdateThingShadowRequestHandler, mockGetThingShadowRequestHandler);

        integrator.subscribe();
        verify(mockPubSubClientWrapper, atMostOnce()).subscribe(any());

        integrator.subscribe();
        verify(mockPubSubClientWrapper, atMostOnce()).subscribe(any());

        integrator.unsubscribe();
        verify(mockPubSubClientWrapper, atMostOnce()).unsubscribe(any());

        integrator.unsubscribe();
        verify(mockPubSubClientWrapper, atMostOnce()).unsubscribe(any());
    }

    @ParameterizedTest
    @MethodSource("classicAndNamedShadow")
    void GIVEN_classic_shadow_op_invocation_WHEN_accept_THEN_calls_the_correct_handler(String shadowName, String op, ExtensionContext extensionContext) {
        ignoreExceptionOfType(extensionContext, IllegalArgumentException.class);
        PubSubIntegrator integrator = new PubSubIntegrator(mockPubSubClientWrapper, mockDeleteThingShadowRequestHandler,
                mockUpdateThingShadowRequestHandler, mockGetThingShadowRequestHandler);
        integrator.subscribe();
        if (CLASSIC_SHADOW_IDENTIFIER.equals(shadowName)) {
            publishEventCaptor.getValue().accept(PublishEvent.builder().topic("$aws/things/" + MOCK_THING + "/shadow/" + op).payload(PAYLOAD).build());
        } else {
            publishEventCaptor.getValue().accept(PublishEvent.builder().topic("$aws/things/" + MOCK_THING + "/shadow/name/" + shadowName + "/" + op).payload(PAYLOAD).build());
        }

        switch (op) {
            case "update":
                verify(mockUpdateThingShadowRequestHandler, atMostOnce()).handleRequest(any(UpdateThingShadowRequest.class), eq(SHADOW_MANAGER_NAME));
                assertThat(updateThingShadowRequestCaptor.getAllValues().size(), is(1));
                UpdateThingShadowRequest updateRequest = updateThingShadowRequestCaptor.getValue();
                assertThat(updateRequest.getThingName(), is(MOCK_THING));
                assertThat(updateRequest.getShadowName(), is(shadowName));
                assertThat(updateRequest.getPayload(), is(notNullValue()));
                assertThat(updateRequest.getPayload(), is(PAYLOAD));
                break;
            case "delete":
                verify(mockDeleteThingShadowRequestHandler, atMostOnce()).handleRequest(any(DeleteThingShadowRequest.class), eq(SHADOW_MANAGER_NAME));
                assertThat(deleteThingShadowRequestCaptor.getAllValues().size(), is(1));
                DeleteThingShadowRequest deleteRequest = deleteThingShadowRequestCaptor.getValue();
                assertThat(deleteRequest.getThingName(), is(MOCK_THING));
                assertThat(deleteRequest.getShadowName(), is(shadowName));
                break;
            case "get":
                verify(mockGetThingShadowRequestHandler, atMostOnce()).handleRequest(any(GetThingShadowRequest.class), eq(SHADOW_MANAGER_NAME));
                assertThat(getThingShadowRequestCaptor.getAllValues().size(), is(1));
                GetThingShadowRequest getRequest = getThingShadowRequestCaptor.getValue();
                assertThat(getRequest.getThingName(), is(MOCK_THING));
                assertThat(getRequest.getShadowName(), is(shadowName));
                break;
            default:
                verify(mockUpdateThingShadowRequestHandler, never()).handleRequest(any(UpdateThingShadowRequest.class), eq(SHADOW_MANAGER_NAME));
                verify(mockDeleteThingShadowRequestHandler, never()).handleRequest(any(DeleteThingShadowRequest.class), eq(SHADOW_MANAGER_NAME));
                verify(mockGetThingShadowRequestHandler, never()).handleRequest(any(GetThingShadowRequest.class), eq(SHADOW_MANAGER_NAME));
                break;
        }
    }

    @Test
    void GIVEN_bad_topic_WHEN_accept_THEN_throws_IllegalArgumentException(ExtensionContext extensionContext) {
        ignoreExceptionOfType(extensionContext, IllegalArgumentException.class);
        PubSubIntegrator integrator = new PubSubIntegrator(mockPubSubClientWrapper, mockDeleteThingShadowRequestHandler,
                mockUpdateThingShadowRequestHandler, mockGetThingShadowRequestHandler);
        integrator.subscribe();
        // No shadow name or op
        publishEventCaptor.getValue().accept(PublishEvent.builder().topic("$aws/things/" + MOCK_THING + "/shadow").payload(PAYLOAD).build());

        verify(mockUpdateThingShadowRequestHandler, never()).handleRequest(any(UpdateThingShadowRequest.class), eq(SHADOW_MANAGER_NAME));
        verify(mockDeleteThingShadowRequestHandler, never()).handleRequest(any(DeleteThingShadowRequest.class), eq(SHADOW_MANAGER_NAME));
        verify(mockGetThingShadowRequestHandler, never()).handleRequest(any(GetThingShadowRequest.class), eq(SHADOW_MANAGER_NAME));

        // No op
        publishEventCaptor.getValue().accept(PublishEvent.builder().topic("$aws/things/" + MOCK_THING + "/shadow/name/" + SHADOW_NAME).payload(PAYLOAD).build());

        verify(mockUpdateThingShadowRequestHandler, never()).handleRequest(any(UpdateThingShadowRequest.class), eq(SHADOW_MANAGER_NAME));
        verify(mockDeleteThingShadowRequestHandler, never()).handleRequest(any(DeleteThingShadowRequest.class), eq(SHADOW_MANAGER_NAME));
        verify(mockGetThingShadowRequestHandler, never()).handleRequest(any(GetThingShadowRequest.class), eq(SHADOW_MANAGER_NAME));
    }

    @Test
    void GIVEN_response_topic_WHEN_accept_THEN_does_not_perform_shadow_op(ExtensionContext extensionContext) {
        ignoreExceptionOfType(extensionContext, IllegalArgumentException.class);
        PubSubIntegrator integrator = new PubSubIntegrator(mockPubSubClientWrapper, mockDeleteThingShadowRequestHandler,
                mockUpdateThingShadowRequestHandler, mockGetThingShadowRequestHandler);
        integrator.subscribe();
        // No shadow name or op
        publishEventCaptor.getValue().accept(PublishEvent.builder().topic("$aws/things/" + MOCK_THING + "/shadow/update/accepted").payload(PAYLOAD).build());

        verify(mockUpdateThingShadowRequestHandler, never()).handleRequest(any(UpdateThingShadowRequest.class), eq(SHADOW_MANAGER_NAME));
        verify(mockDeleteThingShadowRequestHandler, never()).handleRequest(any(DeleteThingShadowRequest.class), eq(SHADOW_MANAGER_NAME));
        verify(mockGetThingShadowRequestHandler, never()).handleRequest(any(GetThingShadowRequest.class), eq(SHADOW_MANAGER_NAME));

        // No op
        publishEventCaptor.getValue().accept(PublishEvent.builder().topic("$aws/things/" + MOCK_THING + "/shadow/name/shadow1/update/accepted" + SHADOW_NAME).payload(PAYLOAD).build());

        verify(mockUpdateThingShadowRequestHandler, never()).handleRequest(any(UpdateThingShadowRequest.class), eq(SHADOW_MANAGER_NAME));
        verify(mockDeleteThingShadowRequestHandler, never()).handleRequest(any(DeleteThingShadowRequest.class), eq(SHADOW_MANAGER_NAME));
        verify(mockGetThingShadowRequestHandler, never()).handleRequest(any(GetThingShadowRequest.class), eq(SHADOW_MANAGER_NAME));
    }

    @Test
    void GIVEN_update_command_WHEN_update_throws_errors_THEN_pubsub_integrator_does_not_rethrow(ExtensionContext extensionContext) {
        ignoreExceptionOfType(extensionContext, ConflictError.class);
        ignoreExceptionOfType(extensionContext, ServiceError.class);
        ignoreExceptionOfType(extensionContext, InvalidArgumentsError.class);
        ignoreExceptionOfType(extensionContext, InvalidRequestParametersException.class);

        when(mockUpdateThingShadowRequestHandler.handleRequest(any(), eq(SHADOW_MANAGER_NAME))).thenThrow(new ConflictError());
        PubSubIntegrator integrator = new PubSubIntegrator(mockPubSubClientWrapper, mockDeleteThingShadowRequestHandler,
                mockUpdateThingShadowRequestHandler, mockGetThingShadowRequestHandler);
        integrator.subscribe();
        assertDoesNotThrow(() -> publishEventCaptor.getValue().accept(PublishEvent.builder().topic("$aws/things/" + MOCK_THING + "/shadow/update").payload(PAYLOAD).build()));
        verify(mockUpdateThingShadowRequestHandler, atMostOnce()).handleRequest(any(UpdateThingShadowRequest.class), eq(SHADOW_MANAGER_NAME));

        reset(mockUpdateThingShadowRequestHandler);
        when(mockUpdateThingShadowRequestHandler.handleRequest(any(), eq(SHADOW_MANAGER_NAME))).thenThrow(new InvalidArgumentsError());
        assertDoesNotThrow(() -> publishEventCaptor.getValue().accept(PublishEvent.builder().topic("$aws/things/" + MOCK_THING + "/shadow/update").payload(PAYLOAD).build()));
        verify(mockUpdateThingShadowRequestHandler, atMostOnce()).handleRequest(any(UpdateThingShadowRequest.class), eq(SHADOW_MANAGER_NAME));

        reset(mockUpdateThingShadowRequestHandler);
        when(mockUpdateThingShadowRequestHandler.handleRequest(any(), eq(SHADOW_MANAGER_NAME))).thenThrow(new ServiceError());
        assertDoesNotThrow(() -> publishEventCaptor.getValue().accept(PublishEvent.builder().topic("$aws/things/" + MOCK_THING + "/shadow/update").payload(PAYLOAD).build()));
        verify(mockUpdateThingShadowRequestHandler, atMostOnce()).handleRequest(any(UpdateThingShadowRequest.class), eq(SHADOW_MANAGER_NAME));

        reset(mockUpdateThingShadowRequestHandler);
        when(mockUpdateThingShadowRequestHandler.handleRequest(any(), eq(SHADOW_MANAGER_NAME))).thenThrow(InvalidRequestParametersException.class);
        assertDoesNotThrow(() -> publishEventCaptor.getValue().accept(PublishEvent.builder().topic("$aws/things/" + MOCK_THING + "/shadow/update").payload(PAYLOAD).build()));
        verify(mockUpdateThingShadowRequestHandler, atMostOnce()).handleRequest(any(UpdateThingShadowRequest.class), eq(SHADOW_MANAGER_NAME));
    }

    @Test
    void GIVEN_delete_command_WHEN_delete_throws_errors_THEN_pubsub_integrator_does_not_rethrow(ExtensionContext extensionContext) {
        ignoreExceptionOfType(extensionContext, ResourceNotFoundError.class);
        ignoreExceptionOfType(extensionContext, ServiceError.class);
        ignoreExceptionOfType(extensionContext, InvalidArgumentsError.class);

        when(mockDeleteThingShadowRequestHandler.handleRequest(any(), eq(SHADOW_MANAGER_NAME))).thenThrow(new ResourceNotFoundError());
        PubSubIntegrator integrator = new PubSubIntegrator(mockPubSubClientWrapper, mockDeleteThingShadowRequestHandler,
                mockUpdateThingShadowRequestHandler, mockGetThingShadowRequestHandler);
        integrator.subscribe();
        assertDoesNotThrow(() -> publishEventCaptor.getValue().accept(PublishEvent.builder().topic("$aws/things/" + MOCK_THING + "/shadow/delete").payload(PAYLOAD).build()));
        verify(mockDeleteThingShadowRequestHandler, atMostOnce()).handleRequest(any(DeleteThingShadowRequest.class), eq(SHADOW_MANAGER_NAME));

        reset(mockDeleteThingShadowRequestHandler);
        when(mockDeleteThingShadowRequestHandler.handleRequest(any(), eq(SHADOW_MANAGER_NAME))).thenThrow(new InvalidArgumentsError());
        assertDoesNotThrow(() -> publishEventCaptor.getValue().accept(PublishEvent.builder().topic("$aws/things/" + MOCK_THING + "/shadow/delete").payload(PAYLOAD).build()));
        verify(mockDeleteThingShadowRequestHandler, atMostOnce()).handleRequest(any(DeleteThingShadowRequest.class), eq(SHADOW_MANAGER_NAME));

        reset(mockDeleteThingShadowRequestHandler);
        when(mockDeleteThingShadowRequestHandler.handleRequest(any(), eq(SHADOW_MANAGER_NAME))).thenThrow(new ServiceError());
        assertDoesNotThrow(() -> publishEventCaptor.getValue().accept(PublishEvent.builder().topic("$aws/things/" + MOCK_THING + "/shadow/delete").payload(PAYLOAD).build()));
        verify(mockDeleteThingShadowRequestHandler, atMostOnce()).handleRequest(any(DeleteThingShadowRequest.class), eq(SHADOW_MANAGER_NAME));
    }

    @Test
    void GIVEN_get_command_WHEN_get_throws_errors_THEN_pubsub_integrator_does_not_rethrow(ExtensionContext extensionContext) {
        ignoreExceptionOfType(extensionContext, ResourceNotFoundError.class);
        ignoreExceptionOfType(extensionContext, ServiceError.class);
        ignoreExceptionOfType(extensionContext, InvalidArgumentsError.class);

        when(mockGetThingShadowRequestHandler.handleRequest(any(), eq(SHADOW_MANAGER_NAME))).thenThrow(new ResourceNotFoundError());
        PubSubIntegrator integrator = new PubSubIntegrator(mockPubSubClientWrapper, mockDeleteThingShadowRequestHandler,
                mockUpdateThingShadowRequestHandler, mockGetThingShadowRequestHandler);
        integrator.subscribe();
        assertDoesNotThrow(() -> publishEventCaptor.getValue().accept(PublishEvent.builder().topic("$aws/things/" + MOCK_THING + "/shadow/get").payload(PAYLOAD).build()));
        verify(mockGetThingShadowRequestHandler, atMostOnce()).handleRequest(any(GetThingShadowRequest.class), eq(SHADOW_MANAGER_NAME));

        reset(mockGetThingShadowRequestHandler);
        when(mockGetThingShadowRequestHandler.handleRequest(any(), eq(SHADOW_MANAGER_NAME))).thenThrow(new InvalidArgumentsError());
        assertDoesNotThrow(() -> publishEventCaptor.getValue().accept(PublishEvent.builder().topic("$aws/things/" + MOCK_THING + "/shadow/get").payload(PAYLOAD).build()));
        verify(mockGetThingShadowRequestHandler, atMostOnce()).handleRequest(any(GetThingShadowRequest.class), eq(SHADOW_MANAGER_NAME));

        reset(mockGetThingShadowRequestHandler);
        when(mockGetThingShadowRequestHandler.handleRequest(any(), eq(SHADOW_MANAGER_NAME))).thenThrow(new ServiceError());
        assertDoesNotThrow(() -> publishEventCaptor.getValue().accept(PublishEvent.builder().topic("$aws/things/" + MOCK_THING + "/shadow/get").payload(PAYLOAD).build()));
        verify(mockGetThingShadowRequestHandler, atMostOnce()).handleRequest(any(GetThingShadowRequest.class), eq(SHADOW_MANAGER_NAME));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", MOCK_SHADOW})
    void GIVEN_response_topic_WHEN_isResponseMessage_THEN_returns_true(String shadowName) {
        PubSubIntegrator integrator = new PubSubIntegrator(mockPubSubClientWrapper, mockDeleteThingShadowRequestHandler,
                mockUpdateThingShadowRequestHandler, mockGetThingShadowRequestHandler);

        String shadowPrefix = "";
        if (!"".equals(shadowName)) {
            shadowPrefix = "/name/" + shadowName;
        }
        assertTrue(integrator.isResponseMessage("$aws/things/" + MOCK_THING + "/shadow" + shadowPrefix + "/get/accepted"));
        assertTrue(integrator.isResponseMessage("$aws/things/" + MOCK_THING + "/shadow" + shadowPrefix + "/get/rejected"));
        assertTrue(integrator.isResponseMessage("$aws/things/" + MOCK_THING + "/shadow" + shadowPrefix + "/delete/accepted"));
        assertTrue(integrator.isResponseMessage("$aws/things/" + MOCK_THING + "/shadow" + shadowPrefix + "/delete/rejected"));
        assertTrue(integrator.isResponseMessage("$aws/things/" + MOCK_THING + "/shadow" + shadowPrefix + "/update/accepted"));
        assertTrue(integrator.isResponseMessage("$aws/things/" + MOCK_THING + "/shadow" + shadowPrefix + "/update/rejected"));
        assertTrue(integrator.isResponseMessage("$aws/things/" + MOCK_THING + "/shadow" + shadowPrefix + "/update/delta"));
        assertTrue(integrator.isResponseMessage("$aws/things/" + MOCK_THING + "/shadow" + shadowPrefix + "/update/documents"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", MOCK_SHADOW})
    void GIVEN_not_response_topic_WHEN_isResponseMessage_THEN_returns_false(String shadowName) {
        PubSubIntegrator integrator = new PubSubIntegrator(mockPubSubClientWrapper, mockDeleteThingShadowRequestHandler,
                mockUpdateThingShadowRequestHandler, mockGetThingShadowRequestHandler);

        String shadowPrefix = "";
        if (!"".equals(shadowName)) {
            shadowPrefix = "/name/" + shadowName;
        }
        assertFalse(integrator.isResponseMessage("$aws/things/" + MOCK_THING + "/shadow" + shadowPrefix + "/get"));
        assertFalse(integrator.isResponseMessage("$aws/things/" + MOCK_THING + "/shadow" + shadowPrefix + "/delete"));
        assertFalse(integrator.isResponseMessage("$aws/things/" + MOCK_THING + "/shadow" + shadowPrefix + "/update"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", MOCK_SHADOW})
    void GIVEN_response_topic_WHEN_extractShadowFromTopic_THEN_throws_IllegalArgumentException(String shadowName) {
        PubSubIntegrator integrator = new PubSubIntegrator(mockPubSubClientWrapper, mockDeleteThingShadowRequestHandler,
                mockUpdateThingShadowRequestHandler, mockGetThingShadowRequestHandler);

        StringBuilder shadowPrefix = new StringBuilder();
        if (!"".equals(shadowName)) {
            shadowPrefix.append("/name/").append(shadowName);
        }
        assertThrows(IllegalArgumentException.class, () -> integrator.extractShadowFromTopic("$aws/things/" + MOCK_THING + "/shadow" + shadowPrefix + "/get/accepted"));
        assertThrows(IllegalArgumentException.class, () -> integrator.extractShadowFromTopic("$aws/things/" + MOCK_THING + "/shadow" + shadowPrefix + "/get/rejected"));
        assertThrows(IllegalArgumentException.class, () -> integrator.extractShadowFromTopic("$aws/things/" + MOCK_THING + "/shadow" + shadowPrefix + "/delete/accepted"));
        assertThrows(IllegalArgumentException.class, () -> integrator.extractShadowFromTopic("$aws/things/" + MOCK_THING + "/shadow" + shadowPrefix + "/delete/rejected"));
        assertThrows(IllegalArgumentException.class, () -> integrator.extractShadowFromTopic("$aws/things/" + MOCK_THING + "/shadow" + shadowPrefix + "/update/accepted"));
        assertThrows(IllegalArgumentException.class, () -> integrator.extractShadowFromTopic("$aws/things/" + MOCK_THING + "/shadow" + shadowPrefix + "/update/rejected"));
        assertThrows(IllegalArgumentException.class, () -> integrator.extractShadowFromTopic("$aws/things/" + MOCK_THING + "/shadow" + shadowPrefix + "/update/delta"));
        assertThrows(IllegalArgumentException.class, () -> integrator.extractShadowFromTopic("$aws/things/" + MOCK_THING + "/shadow" + shadowPrefix + "/update/documents"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", MOCK_SHADOW})
    void GIVEN_not_response_topic_WHEN_extractShadowFromTopic_THEN_returns_(String shadowName) {
        PubSubIntegrator integrator = new PubSubIntegrator(mockPubSubClientWrapper, mockDeleteThingShadowRequestHandler,
                mockUpdateThingShadowRequestHandler, mockGetThingShadowRequestHandler);

        StringBuilder shadowPrefix = new StringBuilder();
        if (!"".equals(shadowName)) {
            shadowPrefix.append("/name/").append(shadowName);
        }
        assertDoesNotThrow(() -> integrator.extractShadowFromTopic("$aws/things/" + MOCK_THING + "/shadow" + shadowPrefix + "/get"));
        assertDoesNotThrow(() -> integrator.extractShadowFromTopic("$aws/things/" + MOCK_THING + "/shadow" + shadowPrefix + "/delete"));
        assertDoesNotThrow(() -> integrator.extractShadowFromTopic("$aws/things/" + MOCK_THING + "/shadow" + shadowPrefix + "/update"));
    }
}
