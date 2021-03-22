/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.ipc;

import com.aws.greengrass.authorization.AuthorizationHandler;
import com.aws.greengrass.authorization.Permission;
import com.aws.greengrass.authorization.exceptions.AuthorizationException;
import com.aws.greengrass.util.Utils;
import software.amazon.awssdk.aws.greengrass.model.InvalidArgumentsError;

import java.util.StringJoiner;

public final class IPCUtil {

    static final String SHADOW_RESOURCE_TYPE = "shadow";
    static final String SHADOW_RESOURCE_JOINER = "shadow";
    static final String SHADOW_MANAGER_NAME = "aws.greengrass.ShadowManager";
    static final String SHADOW_PUBLISH_TOPIC_ACCEPTED_FORMAT = "$aws/things/%s/shadow%s/accepted";
    static final String SHADOW_PUBLISH_TOPIC_REJECTED_FORMAT = "$aws/things/%s/shadow%s/rejected";
    static final String SHADOW_PUBLISH_TOPIC_DELTA_FORMAT = "$aws/things/%s/shadow%s/delta";
    static final String SHADOW_PUBLISH_TOPIC_DOCUMENTS_FORMAT = "$aws/things/%s/shadow%s/documents";
    static final String NAMED_SHADOW_TOPIC_PREFIX = "/name/%s";
    static final String LOG_THING_NAME_KEY = "thing name";
    static final String LOG_SHADOW_NAME_KEY = "shadow name";
    static final String LOG_NEXT_TOKEN_KEY = "nextToken";
    static final String LOG_PAGE_SIZE_KEY = "pageSize";
    public static final String CLASSIC_SHADOW_IDENTIFIER = "";

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
            throws AuthorizationException, InvalidArgumentsError {

        if (Utils.isEmpty(thingName)) {
            throw new IllegalArgumentException("ThingName absent in request");
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
