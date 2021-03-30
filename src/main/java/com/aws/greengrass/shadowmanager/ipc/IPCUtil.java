/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.ipc;

import com.aws.greengrass.authorization.AuthorizationHandler;
import com.aws.greengrass.authorization.Permission;
import com.aws.greengrass.authorization.exceptions.AuthorizationException;
import com.aws.greengrass.shadowmanager.exception.InvalidRequestParametersException;
import com.aws.greengrass.shadowmanager.model.ErrorMessage;
import com.aws.greengrass.util.Utils;
import software.amazon.awssdk.aws.greengrass.model.InvalidArgumentsError;

import java.util.StringJoiner;

import static com.aws.greengrass.shadowmanager.model.Constants.CLASSIC_SHADOW_IDENTIFIER;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_MANAGER_NAME;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_RESOURCE_JOINER;

public final class IPCUtil {
    public enum LogEvents {
        GET_THING_SHADOW("handle-get-thing-shadow"),
        UPDATE_THING_SHADOW("handle-update-thing-shadow"),
        DELETE_THING_SHADOW("handle-delete-thing-shadow"),
        LIST_NAMED_SHADOWS("handle-list-named-shadows-for-thing");

        String code;

        LogEvents(String code) {
            this.code = code;
        }

        public String code() {
            return code;
        }
    }

    private IPCUtil() {
    }

    /**
     * Validate the thingName and checks whether the service is authorized to run the operation on the shadow.
     * TODO: Remove in oncoming PR to split this up
     *
     * @param authorizationHandler The pubsub agent for new IPC
     * @param opCode               The IPC defined operation code
     * @param serviceName          The service which will be running the operation
     * @param thingName            The thingName of the local shadow
     */
    static void validateThingNameAndDoAuthorization(AuthorizationHandler authorizationHandler, String opCode,
                                                    String serviceName, String thingName)
            throws AuthorizationException, InvalidArgumentsError {
        validateThingNameAndDoAuthorization(authorizationHandler, opCode, serviceName,
                thingName, CLASSIC_SHADOW_IDENTIFIER);
    }

    /**
     * Validate the thingName and checks whether the service is authorized to run the operation on the shadow.
     *
     * @param authorizationHandler The pubsub agent for new IPC
     * @param opCode               The IPC defined operation code
     * @param serviceName          The service which will be running the operation
     * @param thingName            The thingName of the local shadow
     * @param shadowName           the shadowName of the local shadow
     */
    static void validateThingNameAndDoAuthorization(AuthorizationHandler authorizationHandler, String opCode,
                                                    String serviceName, String thingName, String shadowName)
            throws AuthorizationException, InvalidRequestParametersException {

        if (Utils.isEmpty(thingName)) {
            throw new InvalidRequestParametersException(ErrorMessage.createThingNotFoundMessage());
        }

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
