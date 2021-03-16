/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.ipc;

import com.aws.greengrass.authorization.AuthorizationHandler;
import com.aws.greengrass.authorization.Permission;
import com.aws.greengrass.authorization.exceptions.AuthorizationException;
import com.aws.greengrass.shadowmanager.ShadowManagerDAO;
import com.aws.greengrass.shadowmanager.exception.ShadowManagerDataException;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.aws.greengrass.model.*;
import software.amazon.awssdk.crt.eventstream.ServerConnectionContinuation;
import software.amazon.awssdk.eventstreamrpc.AuthenticationData;
import software.amazon.awssdk.eventstreamrpc.OperationContinuationHandlerContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith({MockitoExtension.class, GGExtension.class})
public class ListNamedShadowsForThingIPCHandlerTest {
    private static final String TEST_SERVICE = "TestService";
    private static final String THING_NAME = "testThingName";
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
    AuthorizationHandler mockAuthorizationHandler;

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

    @Test
    void GIVEN_valid_request_WHEN_handle_request_THEN_return_list_of_named_shadows() {
        ListNamedShadowsForThingRequest request = new ListNamedShadowsForThingRequest();
        request.setThingName(THING_NAME);

        ListNamedShadowsForThingIPCHandler listNamedShadowsForThingIPCHandler = new ListNamedShadowsForThingIPCHandler(mockContext, mockDao, mockAuthorizationHandler);
        when(mockDao.listNamedShadowsForThing(any(), anyInt(), anyInt())).thenReturn(Optional.of(NAMED_SHADOW_LIST));

        ListNamedShadowsForThingResponse actualResponse = listNamedShadowsForThingIPCHandler.handleRequest(request);
        assertThat(actualResponse.getResults(), is(equalTo(NAMED_SHADOW_LIST)));
        assertThat("nextToken not expected", actualResponse.getNextToken(), is(nullValue()));
        assertThat(actualResponse.getTimestamp(), is(notNullValue()));

        verify(mockDao, times(1)).listNamedShadowsForThing(any(),
                offsetCaptor.capture(), pageSizeCaptor.capture());
        assertThat(offsetCaptor.getValue(), is(equalTo(DEFAULT_OFFSET)));
        assertThat(pageSizeCaptor.getValue(), is(equalTo(DEFAULT_PAGE_SIZE)));
    }

    @Test
    void GIVEN_page_sized_reached_WHEN_handle_request_THEN_return_valid_response_with_token() {
        ListNamedShadowsForThingRequest request = new ListNamedShadowsForThingRequest();
        request.setThingName(THING_NAME);
        request.setPageSize(PAGE_SIZE_MATCHING_SHADOW_LIST);

        ListNamedShadowsForThingIPCHandler listNamedShadowsForThingIPCHandler = new ListNamedShadowsForThingIPCHandler(mockContext, mockDao, mockAuthorizationHandler);
        when(mockDao.listNamedShadowsForThing(any(), anyInt(), anyInt())).thenReturn(Optional.of(NAMED_SHADOW_LIST));
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

    @Test
    void GIVEN_next_token_WHEN_handle_request_THEN_return_valid_response() {
        ListNamedShadowsForThingRequest request = new ListNamedShadowsForThingRequest();
        request.setThingName(THING_NAME);
        request.setNextToken(EXPECTED_TOKEN_WITH_OFFSET);

        ListNamedShadowsForThingIPCHandler listNamedShadowsForThingIPCHandler = new ListNamedShadowsForThingIPCHandler(mockContext, mockDao, mockAuthorizationHandler);
        when(mockDao.listNamedShadowsForThing(any(), anyInt(), anyInt())).thenReturn(Optional.of(NAMED_SHADOW_LIST));
        ListNamedShadowsForThingResponse actualResponse = listNamedShadowsForThingIPCHandler.handleRequest(request);

        assertThat(actualResponse.getResults(), is(equalTo(NAMED_SHADOW_LIST)));
        assertThat("nextToken not expected", actualResponse.getNextToken(), is(nullValue()));
        assertThat(actualResponse.getTimestamp(), is(notNullValue()));

        verify(mockDao, times(1)).listNamedShadowsForThing(any(),
                offsetCaptor.capture(), pageSizeCaptor.capture());
        assertEquals(DECODED_OFFSET_VALUE, offsetCaptor.getValue());
        assertEquals(DEFAULT_PAGE_SIZE, pageSizeCaptor.getValue());
    }

    @Test
    void GIVEN_invalid_page_size_WHEN_handle_request_THEN_throw_invalid_arguments_error(ExtensionContext context) {
        ignoreExceptionOfType(context, InvalidArgumentsError.class);
        ListNamedShadowsForThingRequest request = new ListNamedShadowsForThingRequest();
        request.setThingName(THING_NAME);

        // test value at 0
        request.setPageSize(0);
        ListNamedShadowsForThingIPCHandler listNamedShadowsForThingIPCHandler = new ListNamedShadowsForThingIPCHandler(mockContext, mockDao, mockAuthorizationHandler);
        InvalidArgumentsError thrown = assertThrows(InvalidArgumentsError.class, () -> listNamedShadowsForThingIPCHandler.handleRequest(request));
        assertThat(thrown.getMessage(), startsWith("pageSize argument must"));

        // test value at 101
        request.setPageSize(101);
        thrown = assertThrows(InvalidArgumentsError.class, () -> listNamedShadowsForThingIPCHandler.handleRequest(request));
        assertThat(thrown.getMessage(), startsWith("pageSize argument must"));

        verify(mockDao, times(0)).listNamedShadowsForThing(any(),
                offsetCaptor.capture(), pageSizeCaptor.capture());
    }

    @Test
    void GIVEN_next_token_used_from_different_thing_WHEN_handle_request_THEN_throw_invalid_arguments_error(ExtensionContext context) {
        ignoreExceptionOfType(context, InvalidArgumentsError.class);
        ListNamedShadowsForThingRequest request = new ListNamedShadowsForThingRequest();
        request.setThingName("DifferentThingName");
        request.setNextToken(EXPECTED_TOKEN_WITH_OFFSET);

        ListNamedShadowsForThingIPCHandler listNamedShadowsForThingIPCHandler = new ListNamedShadowsForThingIPCHandler(mockContext, mockDao, mockAuthorizationHandler);
        InvalidArgumentsError thrown = assertThrows(InvalidArgumentsError.class, () -> listNamedShadowsForThingIPCHandler.handleRequest(request));
        assertThat(thrown.getMessage(), startsWith("Unable to decode"));

        verify(mockDao, times(0)).listNamedShadowsForThing(any(),
                offsetCaptor.capture(), pageSizeCaptor.capture());
    }

    @Test
    void GIVEN_missing_thing_name_WHEN_handle_request_THEN_throw_invalid_arguments_error(ExtensionContext context) {
        ignoreExceptionOfType(context, InvalidArgumentsError.class);

        // check if thingName omitted from request
        ListNamedShadowsForThingRequest request = new ListNamedShadowsForThingRequest();

        ListNamedShadowsForThingIPCHandler listNamedShadowsForThingIPCHandler = new ListNamedShadowsForThingIPCHandler(mockContext, mockDao, mockAuthorizationHandler);
        InvalidArgumentsError thrown = assertThrows(InvalidArgumentsError.class, () -> listNamedShadowsForThingIPCHandler.handleRequest(request));
        assertThat(thrown.getMessage(), startsWith("ThingName absent"));

        // check if thingName was empty string
        request.setThingName("");
        thrown = assertThrows(InvalidArgumentsError.class, () -> listNamedShadowsForThingIPCHandler.handleRequest(request));
        assertThat(thrown.getMessage(), startsWith("ThingName absent"));

        verify(mockDao, times(0)).listNamedShadowsForThing(any(),
                offsetCaptor.capture(), pageSizeCaptor.capture());
    }

    @Test
    void GIVEN_empty_return_from_query_WHEN_handle_request_THEN_throw_service_error(ExtensionContext context) {
        ignoreExceptionOfType(context, ServiceError.class);
        ListNamedShadowsForThingRequest request = new ListNamedShadowsForThingRequest();
        request.setThingName(THING_NAME);

        ListNamedShadowsForThingIPCHandler listNamedShadowsForThingIPCHandler = new ListNamedShadowsForThingIPCHandler(mockContext, mockDao, mockAuthorizationHandler);
        when(mockDao.listNamedShadowsForThing(any(), anyInt(), anyInt())).thenReturn(Optional.empty());

        ServiceError thrown = assertThrows(ServiceError.class, () -> listNamedShadowsForThingIPCHandler.handleRequest(request));
        assertThat(thrown.getMessage(), startsWith("Unexpected error"));

        verify(mockDao, times(1)).listNamedShadowsForThing(any(),
                offsetCaptor.capture(), pageSizeCaptor.capture());
    }

    @Test
    void GIVEN_unauthorized_service_WHEN_handle_request_THEN_throw_unauthorized_error(ExtensionContext context) throws Exception {
        ignoreExceptionOfType(context, AuthorizationException.class);
        ListNamedShadowsForThingRequest request = new ListNamedShadowsForThingRequest();
        request.setThingName(THING_NAME);

        when(mockAuthorizationHandler.isAuthorized(any(), any(Permission.class)))
                .thenThrow(new AuthorizationException("sample authorization error"));

        ListNamedShadowsForThingIPCHandler listNamedShadowsForThingIPCHandler = new ListNamedShadowsForThingIPCHandler(mockContext, mockDao, mockAuthorizationHandler);
        UnauthorizedError thrown = assertThrows(UnauthorizedError.class, () -> listNamedShadowsForThingIPCHandler.handleRequest(request));
        assertThat(thrown.getMessage(), startsWith("sample"));

        verify(mockDao, times(0)).listNamedShadowsForThing(any(),
                offsetCaptor.capture(), pageSizeCaptor.capture());
    }

    @Test
    void GIVEN_shadow_manager_data_exception_from_query_WHEN_handle_request_THEN_throw_service_error(ExtensionContext context) throws IOException {
        ignoreExceptionOfType(context, ShadowManagerDataException.class);
        ListNamedShadowsForThingRequest request = new ListNamedShadowsForThingRequest();
        request.setThingName(THING_NAME);

        ListNamedShadowsForThingIPCHandler listNamedShadowsForThingIPCHandler = new ListNamedShadowsForThingIPCHandler(mockContext, mockDao, mockAuthorizationHandler);
        doThrow(new ShadowManagerDataException(new Exception("sample exception message"))).when(mockDao).listNamedShadowsForThing(any(), anyInt(), anyInt());
        ServiceError thrown = assertThrows(ServiceError.class, () -> listNamedShadowsForThingIPCHandler.handleRequest(request));
        assertThat(thrown.getMessage(), containsString("sample"));

        verify(mockDao, times(1)).listNamedShadowsForThing(any(),
                offsetCaptor.capture(), pageSizeCaptor.capture());
    }
}