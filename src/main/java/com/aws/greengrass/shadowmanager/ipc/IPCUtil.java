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

    public enum LogEvents {
        GET_THING_SHADOW("handle-get-thing-shadow"),
        UPDATE_THING_SHADOW("handle-update-thing-shadow"),
        DELETE_THING_SHADOW("handle-delete-thing-shadow");

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

    static void validateThingNameAndDoAuthorization(AuthorizationHandler authorizationHandler, String opCode,
                                                    String serviceName, String thingName, String shadowName)
            throws AuthorizationException, InvalidArgumentsError {

        if (Utils.isEmpty(thingName)) {
            throw new InvalidArgumentsError("ThingName absent in request");
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
