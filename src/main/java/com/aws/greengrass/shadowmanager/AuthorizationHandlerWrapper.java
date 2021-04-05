/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager;

import com.aws.greengrass.authorization.AuthorizationHandler;
import com.aws.greengrass.authorization.Permission;
import com.aws.greengrass.authorization.exceptions.AuthorizationException;
import com.aws.greengrass.shadowmanager.model.ShadowRequest;

import java.util.Set;
import javax.inject.Inject;

import static com.aws.greengrass.shadowmanager.model.Constants.CLASSIC_SHADOW_IDENTIFIER;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_MANAGER_NAME;

/**
 * Class to check if ipc requests are authorized.
 */
public class AuthorizationHandlerWrapper {
    private final AuthorizationHandler authorizationHandler;

    /**
     * Constructor.
     *
     * @param authorizationHandler PubSub event stream agent
     */
    @Inject
    public AuthorizationHandlerWrapper(AuthorizationHandler authorizationHandler) {
        this.authorizationHandler = authorizationHandler;
    }

    /**
     * Registers component and set of permissible operations with the Authorization module.
     *
     * @param componentName Name of component to be initialized with the Authorization module
     * @param operations    Set of operations that component will be authorized to execute
     * @throws AuthorizationException When invalid arguments were passed into the registerComponent function call
     */
    public void registerComponent(String componentName, Set<String> operations)
            throws AuthorizationException {
        authorizationHandler.registerComponent(componentName, operations);
    }

    /**
     * Checks if service is authorized to run the operation on the target shadow resource.
     *
     * @param opCode        The operation to be executed
     * @param serviceName   The service trying to run the operation
     * @param shadowRequest The shadow request object containing the thingName and shadowName
     * @throws AuthorizationException When the service is unauthorized to execute the operation on shadow resource
     */
    public void doAuthorization(String opCode, String serviceName, ShadowRequest shadowRequest)
            throws AuthorizationException {
        authorizationHandler.isAuthorized(
                SHADOW_MANAGER_NAME,
                Permission.builder()
                        .principal(serviceName)
                        .operation(opCode)
                        .resource(shadowRequest.getShadowTopicPrefix())
                        .build());
    }
}
