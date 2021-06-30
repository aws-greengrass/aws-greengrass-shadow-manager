/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.ipc;

import com.aws.greengrass.authorization.AuthorizationHandler;
import com.aws.greengrass.authorization.Permission;
import com.aws.greengrass.authorization.exceptions.AuthorizationException;
import com.aws.greengrass.shadowmanager.AuthorizationHandlerWrapper;
import com.aws.greengrass.shadowmanager.ShadowManager;
import com.aws.greengrass.shadowmanager.model.ShadowRequest;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static com.aws.greengrass.shadowmanager.TestUtils.SAMPLE_EXCEPTION_MESSAGE;
import static com.aws.greengrass.shadowmanager.TestUtils.SHADOW_NAME;
import static com.aws.greengrass.shadowmanager.TestUtils.TEST_SERVICE;
import static com.aws.greengrass.shadowmanager.TestUtils.THING_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, GGExtension.class})
public class AuthorizationHandlerWrapperTest {
    private final static String OP_CODE = "testOpCode";
    private final static Set<String> TEST_OPERATIONS_SET = new HashSet<>(Arrays.asList("a", "b", "c"));


    @Mock
    AuthorizationHandler mockAuthorizationHandler;

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {SHADOW_NAME})
    void GIVEN_valid_authorization_WHEN_do_authorization_THEN_do_nothing(String shadowName) throws AuthorizationException {
        when(mockAuthorizationHandler.isAuthorized(any(), any(Permission.class))).thenReturn(true);
        AuthorizationHandlerWrapper authorizationHandlerWrapper = new AuthorizationHandlerWrapper(mockAuthorizationHandler);
        ShadowRequest shadowRequest = new ShadowRequest(THING_NAME, shadowName);

        assertDoesNotThrow(() -> authorizationHandlerWrapper.doAuthorization(OP_CODE, TEST_SERVICE, shadowRequest));
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {SHADOW_NAME})
    void GIVEN_invalid_authorization_WHEN_do_authorization_THEN_throw_authorization_exception(String shadowName) throws AuthorizationException {
        when(mockAuthorizationHandler.isAuthorized(any(), any(Permission.class)))
                .thenThrow(new AuthorizationException(SAMPLE_EXCEPTION_MESSAGE));
        AuthorizationHandlerWrapper authorizationHandlerWrapper = new AuthorizationHandlerWrapper(mockAuthorizationHandler);
        ShadowRequest shadowRequest = new ShadowRequest(THING_NAME, shadowName);

        AuthorizationException thrown = assertThrows(AuthorizationException.class,
                () -> authorizationHandlerWrapper.doAuthorization(OP_CODE, TEST_SERVICE, shadowRequest));

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

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {SHADOW_NAME})
    void GIVEN_shadow_manager_WHEN_perform_action_THEN_authorized(String shadowName)  {
        AuthorizationHandlerWrapper authorizationHandlerWrapper = new AuthorizationHandlerWrapper(mockAuthorizationHandler);
        ShadowRequest shadowRequest = new ShadowRequest(THING_NAME, shadowName);
        assertDoesNotThrow(() -> authorizationHandlerWrapper.doAuthorization(OP_CODE, ShadowManager.SERVICE_NAME,
                shadowRequest ));
    }
}
