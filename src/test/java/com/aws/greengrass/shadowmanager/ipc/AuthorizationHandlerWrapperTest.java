/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.ipc;

import com.aws.greengrass.authorization.AuthorizationHandler;
import com.aws.greengrass.authorization.Permission;
import com.aws.greengrass.authorization.exceptions.AuthorizationException;
import com.aws.greengrass.shadowmanager.AuthorizationHandlerWrapper;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static com.aws.greengrass.shadowmanager.TestUtils.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith({MockitoExtension.class, GGExtension.class})
public class AuthorizationHandlerWrapperTest {
    private final static String OP_CODE = "testOpCode";
    private final static Set<String> TEST_OPERATIONS_SET = new HashSet<>(Arrays.asList("a", "b", "c"));


    @Mock
    AuthorizationHandler mockAuthorizationHandler;

    @Test
    void GIVEN_valid_authorization_without_shadow_name_WHEN_do_authorization_THEN_throw_authorization_exception() throws AuthorizationException {
        when(mockAuthorizationHandler.isAuthorized(any(), any(Permission.class))).thenReturn(true);
        AuthorizationHandlerWrapper authorizationHandlerWrapper = new AuthorizationHandlerWrapper(mockAuthorizationHandler);

        assertDoesNotThrow(() -> authorizationHandlerWrapper.doAuthorization(OP_CODE, TEST_SERVICE, THING_NAME));
    }

    @Test
    void GIVEN_invalid_authorization_without_shadow_name_WHEN_do_authorization_THEN_throw_authorization_exception() throws AuthorizationException {
        when(mockAuthorizationHandler.isAuthorized(any(), any(Permission.class)))
                .thenThrow(new AuthorizationException(SAMPLE_EXCEPTION_MESSAGE));
        AuthorizationHandlerWrapper authorizationHandlerWrapper = new AuthorizationHandlerWrapper(mockAuthorizationHandler);
        AuthorizationException thrown = assertThrows(AuthorizationException.class,
                () -> authorizationHandlerWrapper.doAuthorization(OP_CODE, TEST_SERVICE, THING_NAME));

        assertThat(thrown.getMessage(), is(equalTo(SAMPLE_EXCEPTION_MESSAGE)));
    }

    @Test
    void GIVEN_valid_authorization_with_shadow_name_WHEN_do_authorization_THEN_do_nothing() throws AuthorizationException {
        when(mockAuthorizationHandler.isAuthorized(any(), any(Permission.class))).thenReturn(true);
        AuthorizationHandlerWrapper authorizationHandlerWrapper = new AuthorizationHandlerWrapper(mockAuthorizationHandler);

        assertDoesNotThrow(() -> authorizationHandlerWrapper.doAuthorization(OP_CODE, TEST_SERVICE, THING_NAME, SHADOW_NAME));
    }

    @Test
    void GIVEN_invalid_authorization_with_shadow_name_WHEN_do_authorization_THEN_throw_authorization_exception() throws AuthorizationException {
        when(mockAuthorizationHandler.isAuthorized(any(), any(Permission.class)))
                .thenThrow(new AuthorizationException(SAMPLE_EXCEPTION_MESSAGE));
        AuthorizationHandlerWrapper authorizationHandlerWrapper = new AuthorizationHandlerWrapper(mockAuthorizationHandler);
        AuthorizationException thrown = assertThrows(AuthorizationException.class,
                () -> authorizationHandlerWrapper.doAuthorization(OP_CODE, TEST_SERVICE, THING_NAME, SHADOW_NAME));

        assertThat(thrown.getMessage(), is(equalTo(SAMPLE_EXCEPTION_MESSAGE)));
    }

    @Test
    void GIVEN_valid_component_registration_WHEN_register_component_THEN_throw_do_nothing() throws AuthorizationException {
        doNothing().when(mockAuthorizationHandler).registerComponent(any(), any());
        AuthorizationHandlerWrapper authorizationHandlerWrapper = new AuthorizationHandlerWrapper(mockAuthorizationHandler);

        assertDoesNotThrow(() -> authorizationHandlerWrapper.registerComponent(TEST_SERVICE, TEST_OPERATIONS_SET));
    }

    @Test
    void GIVEN_invalid_component_registration_WHEN_register_component_THEN_throw_authorization_exception() throws AuthorizationException {
        doThrow(new AuthorizationException(SAMPLE_EXCEPTION_MESSAGE)).when(mockAuthorizationHandler).registerComponent(any(), any());
        AuthorizationHandlerWrapper authorizationHandlerWrapper = new AuthorizationHandlerWrapper(mockAuthorizationHandler);
        AuthorizationException thrown = assertThrows(AuthorizationException.class,
                () -> authorizationHandlerWrapper.registerComponent(TEST_SERVICE, TEST_OPERATIONS_SET));

        assertThat(thrown.getMessage(), is(equalTo(SAMPLE_EXCEPTION_MESSAGE)));
    }
}
