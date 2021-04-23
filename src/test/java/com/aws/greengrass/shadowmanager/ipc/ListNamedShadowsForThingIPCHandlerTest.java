/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.ipc;

import com.aws.greengrass.authorization.exceptions.AuthorizationException;
import com.aws.greengrass.shadowmanager.AuthorizationHandlerWrapper;
import com.aws.greengrass.shadowmanager.ShadowManagerDAO;
import com.aws.greengrass.shadowmanager.exception.InvalidRequestParametersException;
import com.aws.greengrass.shadowmanager.exception.ShadowManagerDataException;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
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

import javax.crypto.BadPaddingException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static com.aws.greengrass.shadowmanager.TestUtils.*;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class ListNamedShadowsForThingIPCHandlerTest {
    private static final List<String> NAMED_SHADOW_LIST = Arrays.asList("one", "two", "three");
    private static final String EXPECTED_TOKEN_WITH_OFFSET = "o8Zz1puGZZ/aNy+OKKAN8A==";
    private static final int DECODED_OFFSET_VALUE = 3;
    private static final int PAGE_SIZE_MATCHING_SHADOW_LIST = 3;
    private static final int DEFAULT_OFFSET = 0;
    private static final int DEFAULT_PAGE_SIZE = 25;

    @Mock
    OperationContinuationHandlerContext mockContext;

    @Mock
    AuthenticationData mockAuthenticationData;

    @Mock
    AuthorizationHandlerWrapper mockAuthorizationHandlerWrapper;

    @Mock
    ShadowManagerDAO mockDao;

    @Captor
    ArgumentCaptor<Integer> offsetCaptor;
    @Captor
    ArgumentCaptor<Integer> pageSizeCaptor;

    @BeforeEach
    void setup() {
        when(mockContext.getContinuation()).thenReturn(mock(ServerConnectionContinuation.class));
        when(mockContext.getAuthenticationData()).thenReturn(mockAuthenticationData);
        when(mockAuthenticationData.getIdentityLabel()).thenReturn(TEST_SERVICE);
    }

    @ParameterizedTest
    @NullAndEmptySource
    void GIVEN_valid_request_WHEN_handle_request_THEN_return_valid_response(String nextToken) {
        ListNamedShadowsForThingRequest request = new ListNamedShadowsForThingRequest();
        request.setThingName(THING_NAME);
        request.setNextToken(nextToken);

        // only tests for null pageSize
        if (nextToken == null) {
            request.setPageSize(null);
        }

        try (ListNamedShadowsForThingIPCHandler listNamedShadowsForThingIPCHandler = new ListNamedShadowsForThingIPCHandler(mockContext, mockDao, mockAuthorizationHandlerWrapper)) {
            when(mockDao.listNamedShadowsForThing(any(), anyInt(), anyInt())).thenReturn(NAMED_SHADOW_LIST);

            ListNamedShadowsForThingResponse actualResponse = listNamedShadowsForThingIPCHandler.handleRequest(request);
            assertThat(actualResponse.getResults(), is(equalTo(NAMED_SHADOW_LIST)));
            assertThat("nextToken not expected", actualResponse.getNextToken(), is(nullValue()));
            assertThat(actualResponse.getTimestamp(), is(notNullValue()));

            verify(mockDao, times(1)).listNamedShadowsForThing(any(),
                    offsetCaptor.capture(), pageSizeCaptor.capture());
            assertThat(offsetCaptor.getValue(), is(equalTo(DEFAULT_OFFSET)));
            assertThat(pageSizeCaptor.getValue(), is(equalTo(DEFAULT_PAGE_SIZE)));
        }
    }

    @Test
    void GIVEN_page_sized_reached_WHEN_handle_request_THEN_return_valid_response_with_token() {
        ListNamedShadowsForThingRequest request = new ListNamedShadowsForThingRequest();
        request.setThingName(THING_NAME);
        request.setPageSize(PAGE_SIZE_MATCHING_SHADOW_LIST);

        try (ListNamedShadowsForThingIPCHandler listNamedShadowsForThingIPCHandler = new ListNamedShadowsForThingIPCHandler(mockContext, mockDao, mockAuthorizationHandlerWrapper)) {
            when(mockDao.listNamedShadowsForThing(any(), anyInt(), anyInt())).thenReturn(NAMED_SHADOW_LIST);
            ListNamedShadowsForThingResponse actualResponse = listNamedShadowsForThingIPCHandler.handleRequest(request);

            assertThat(actualResponse.getResults(), is(equalTo(NAMED_SHADOW_LIST)));
            assertThat(actualResponse.getNextToken(), is(notNullValue()));
            assertThat(actualResponse.getNextToken(), is(equalTo(EXPECTED_TOKEN_WITH_OFFSET)));
            assertThat(actualResponse.getTimestamp(), is(notNullValue()));

            verify(mockDao, times(1)).listNamedShadowsForThing(any(),
                    offsetCaptor.capture(), pageSizeCaptor.capture());
            assertThat(offsetCaptor.getValue(), is(equalTo(DEFAULT_OFFSET)));
            assertThat(pageSizeCaptor.getValue(), is(equalTo(PAGE_SIZE_MATCHING_SHADOW_LIST)));
        }
    }

    @Test
    void GIVEN_next_token_WHEN_handle_request_THEN_return_valid_response() {
        ListNamedShadowsForThingRequest request = new ListNamedShadowsForThingRequest();
        request.setThingName(THING_NAME);
        request.setNextToken(EXPECTED_TOKEN_WITH_OFFSET);

        try (ListNamedShadowsForThingIPCHandler listNamedShadowsForThingIPCHandler = new ListNamedShadowsForThingIPCHandler(mockContext, mockDao, mockAuthorizationHandlerWrapper)) {
            when(mockDao.listNamedShadowsForThing(any(), anyInt(), anyInt())).thenReturn(NAMED_SHADOW_LIST);
            ListNamedShadowsForThingResponse actualResponse = listNamedShadowsForThingIPCHandler.handleRequest(request);

            assertThat(actualResponse.getResults(), is(equalTo(NAMED_SHADOW_LIST)));
            assertThat("nextToken not expected", actualResponse.getNextToken(), is(nullValue()));
            assertThat(actualResponse.getTimestamp(), is(notNullValue()));

            verify(mockDao, times(1)).listNamedShadowsForThing(any(),
                    offsetCaptor.capture(), pageSizeCaptor.capture());
            assertThat(offsetCaptor.getValue(), is(equalTo(DECODED_OFFSET_VALUE)));
            assertThat(pageSizeCaptor.getValue(), is(equalTo(DEFAULT_PAGE_SIZE)));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 0, 101})
    void GIVEN_invalid_page_size_WHEN_handle_request_THEN_throw_invalid_arguments_error(int pageSize, ExtensionContext context) {
        ignoreExceptionOfType(context, IllegalArgumentException.class);
        ListNamedShadowsForThingRequest request = new ListNamedShadowsForThingRequest();
        request.setThingName(THING_NAME);
        request.setPageSize(pageSize);

        try (ListNamedShadowsForThingIPCHandler listNamedShadowsForThingIPCHandler = new ListNamedShadowsForThingIPCHandler(mockContext, mockDao, mockAuthorizationHandlerWrapper)) {
            InvalidArgumentsError thrown = assertThrows(InvalidArgumentsError.class, () -> listNamedShadowsForThingIPCHandler.handleRequest(request));
            assertThat(thrown.getMessage(), startsWith("pageSize argument must"));

            verify(mockDao, times(0)).listNamedShadowsForThing(any(),
                    offsetCaptor.capture(), pageSizeCaptor.capture());
        }
    }

    @Test
    void GIVEN_next_token_used_from_different_thing_WHEN_handle_request_THEN_throw_invalid_arguments_error(ExtensionContext context) {
        ignoreExceptionOfType(context, IllegalArgumentException.class);
        ignoreExceptionOfType(context, BadPaddingException.class);
        ListNamedShadowsForThingRequest request = new ListNamedShadowsForThingRequest();
        request.setThingName("DifferentThingName");
        request.setNextToken(EXPECTED_TOKEN_WITH_OFFSET);

        try (ListNamedShadowsForThingIPCHandler listNamedShadowsForThingIPCHandler = new ListNamedShadowsForThingIPCHandler(mockContext, mockDao, mockAuthorizationHandlerWrapper)) {
            InvalidArgumentsError thrown = assertThrows(InvalidArgumentsError.class, () -> listNamedShadowsForThingIPCHandler.handleRequest(request));
            assertThat(thrown.getMessage(), startsWith("Invalid nextToken"));

            verify(mockDao, times(0)).listNamedShadowsForThing(any(),
                    offsetCaptor.capture(), pageSizeCaptor.capture());
        }
    }

    @ParameterizedTest
    @NullAndEmptySource
    void GIVEN_missing_thing_name_WHEN_handle_request_THEN_throw_invalid_arguments_error(String thingName, ExtensionContext context) {
        ignoreExceptionOfType(context, InvalidRequestParametersException.class);
        ListNamedShadowsForThingRequest request = new ListNamedShadowsForThingRequest();
        request.setThingName(thingName);

        try (ListNamedShadowsForThingIPCHandler listNamedShadowsForThingIPCHandler = new ListNamedShadowsForThingIPCHandler(mockContext, mockDao, mockAuthorizationHandlerWrapper)) {
            InvalidArgumentsError thrown = assertThrows(InvalidArgumentsError.class, () -> listNamedShadowsForThingIPCHandler.handleRequest(request));
            assertThat(thrown.getMessage(), startsWith("ThingName"));

            verify(mockDao, times(0)).listNamedShadowsForThing(any(),
                    offsetCaptor.capture(), pageSizeCaptor.capture());
        }
    }

    @Test
    void GIVEN_unauthorized_service_WHEN_handle_request_THEN_throw_unauthorized_error(ExtensionContext context) throws Exception {
        ignoreExceptionOfType(context, AuthorizationException.class);
        ListNamedShadowsForThingRequest request = new ListNamedShadowsForThingRequest();
        request.setThingName(THING_NAME);

        doThrow(new AuthorizationException(SAMPLE_EXCEPTION_MESSAGE)).when(mockAuthorizationHandlerWrapper).doAuthorization(any(), any(), anyString());

        try (ListNamedShadowsForThingIPCHandler listNamedShadowsForThingIPCHandler = new ListNamedShadowsForThingIPCHandler(mockContext, mockDao, mockAuthorizationHandlerWrapper)) {
            UnauthorizedError thrown = assertThrows(UnauthorizedError.class, () -> listNamedShadowsForThingIPCHandler.handleRequest(request));
            assertThat(thrown.getMessage(), is(equalTo(SAMPLE_EXCEPTION_MESSAGE)));

            verify(mockDao, times(0)).listNamedShadowsForThing(any(),
                    offsetCaptor.capture(), pageSizeCaptor.capture());
        }
    }

    @Test
    void GIVEN_shadow_manager_data_exception_from_query_WHEN_handle_request_THEN_throw_service_error(ExtensionContext context) throws IOException {
        ignoreExceptionOfType(context, ShadowManagerDataException.class);
        ListNamedShadowsForThingRequest request = new ListNamedShadowsForThingRequest();
        request.setThingName(THING_NAME);

        try (ListNamedShadowsForThingIPCHandler listNamedShadowsForThingIPCHandler = new ListNamedShadowsForThingIPCHandler(mockContext, mockDao, mockAuthorizationHandlerWrapper)) {
            doThrow(new ShadowManagerDataException(new Exception(SAMPLE_EXCEPTION_MESSAGE))).when(mockDao).listNamedShadowsForThing(any(), anyInt(), anyInt());
            ServiceError thrown = assertThrows(ServiceError.class, () -> listNamedShadowsForThingIPCHandler.handleRequest(request));
            assertThat(thrown.getMessage(), containsString(SAMPLE_EXCEPTION_MESSAGE));

            verify(mockDao, times(1)).listNamedShadowsForThing(any(),
                    offsetCaptor.capture(), pageSizeCaptor.capture());
            assertThat(offsetCaptor.getValue(), is(equalTo(DEFAULT_OFFSET)));
            assertThat(pageSizeCaptor.getValue(), is(equalTo(DEFAULT_PAGE_SIZE)));
        }
    }

    @Test
    void GIVEN_dao_results_are_greater_than_page_size_WHEN_handle_request_THEN_throw_service_error(ExtensionContext context) throws IOException {
        ignoreExceptionOfType(context, ServiceError.class);
        int pageSize = 1;

        ListNamedShadowsForThingRequest request = new ListNamedShadowsForThingRequest();
        request.setThingName(THING_NAME);
        request.setPageSize(pageSize);

        try (ListNamedShadowsForThingIPCHandler listNamedShadowsForThingIPCHandler = new ListNamedShadowsForThingIPCHandler(mockContext, mockDao, mockAuthorizationHandlerWrapper)) {
            when(mockDao.listNamedShadowsForThing(any(), anyInt(), anyInt())).thenReturn(NAMED_SHADOW_LIST);
            ServiceError thrown = assertThrows(ServiceError.class, () -> listNamedShadowsForThingIPCHandler.handleRequest(request));
            assertThat(thrown.getMessage(), containsString("internal service error"));

            verify(mockDao, times(1)).listNamedShadowsForThing(any(),
                    offsetCaptor.capture(), pageSizeCaptor.capture());
            assertThat(offsetCaptor.getValue(), is(equalTo(DEFAULT_OFFSET)));
            assertThat(pageSizeCaptor.getValue(), is(equalTo(pageSize)));
        }
    }

    @Test
    void GIVEN_list_named_shadows_for_thing_ipc_handler_WHEN_handle_stream_event_THEN_nothing_happens() {
        try (ListNamedShadowsForThingIPCHandler listNamedShadowsForThingIPCHandler = new ListNamedShadowsForThingIPCHandler(mockContext, mockDao, mockAuthorizationHandlerWrapper)) {
            assertDoesNotThrow(() -> listNamedShadowsForThingIPCHandler.handleStreamEvent(mock(EventStreamJsonMessage.class)));
        }
    }

    @Test
    void GIVEN_list_named_shadows_for_thing_ipc_handler_WHEN_stream_closes_event_THEN_nothing_happens() {
        try (ListNamedShadowsForThingIPCHandler listNamedShadowsForThingIPCHandler = new ListNamedShadowsForThingIPCHandler(mockContext, mockDao, mockAuthorizationHandlerWrapper)) {
            assertDoesNotThrow(listNamedShadowsForThingIPCHandler::onStreamClosed);
        }
    }
}