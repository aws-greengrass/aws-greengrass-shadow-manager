/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager;

import com.aws.greengrass.authorization.AuthorizationHandler;
import com.aws.greengrass.authorization.Permission;
import com.aws.greengrass.authorization.exceptions.AuthorizationException;
import com.aws.greengrass.util.Utils;

import java.util.StringJoiner;
import javax.inject.Inject;

import static com.aws.greengrass.shadowmanager.model.Constants.CLASSIC_SHADOW_IDENTIFIER;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_MANAGER_NAME;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_RESOURCE_JOINER;

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

    public void registerComponent(String componentName, java.util.Set<String> operations)
            throws AuthorizationException {
        authorizationHandler.registerComponent(componentName, operations);
    }

    /**
     * Checks if service is authorized to run the operation on the target classic shadow resource.
     *
     * @param opCode      The operation to be executed
     * @param serviceName The service trying to run the operation
     * @param thingName   The thing name to be formed into the shadow resource
     * @throws AuthorizationException When the service is unauthorized to execute the operation on shadow resource
     */
    public void doAuthorization(String opCode, String serviceName,
                                String thingName) throws AuthorizationException {
        doAuthorization(opCode, serviceName, thingName, CLASSIC_SHADOW_IDENTIFIER);
    }

    /**
     * Checks if service is authorized to run the operation on the target shadow resource.
     *
     * @param opCode      The operation to be executed
     * @param serviceName The service trying to run the operation
     * @param thingName   The thing name to be formed into the shadow resource
     * @param shadowName  The shadow name to be formed into the shadow resource
     * @throws AuthorizationException When the service is unauthorized to execute the operation on shadow resource
     */
    public void doAuthorization(String opCode, String serviceName,
                                String thingName, String shadowName) throws AuthorizationException {
        StringJoiner shadowResource = new StringJoiner("/");
        shadowResource.add(thingName);
        shadowResource.add(SHADOW_RESOURCE_JOINER);

        if (Utils.isNotEmpty(shadowName)) {
            shadowResource.add(shadowName);
        }

        authorizationHandler.isAuthorized(
                SHADOW_MANAGER_NAME,
                Permission.builder()
                        .principal(serviceName)
                        .operation(opCode)
                        .resource(shadowResource.toString())
                        .build());
    }
}
