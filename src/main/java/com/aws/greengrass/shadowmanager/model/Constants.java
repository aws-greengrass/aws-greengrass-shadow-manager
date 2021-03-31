/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.model;

public class Constants {
    public static final String SHADOW_RESOURCE_TYPE = "shadow";
    public static final String SHADOW_RESOURCE_JOINER = "shadow";
    public static final String SHADOW_MANAGER_NAME = "aws.greengrass.ShadowManager";
    public static final String SHADOW_PUBLISH_TOPIC_ACCEPTED_FORMAT = "$aws/things/%s/shadow%s/accepted";
    public static final String SHADOW_PUBLISH_TOPIC_REJECTED_FORMAT = "$aws/things/%s/shadow%s/rejected";
    public static final String SHADOW_PUBLISH_TOPIC_DELTA_FORMAT = "$aws/things/%s/shadow%s/delta";
    public static final String SHADOW_PUBLISH_TOPIC_DOCUMENTS_FORMAT = "$aws/things/%s/shadow%s/documents";
    public static final String NAMED_SHADOW_TOPIC_PREFIX = "/name/%s";
    public static final String LOG_THING_NAME_KEY = "thing name";
    public static final String LOG_SHADOW_NAME_KEY = "shadow name";
    public static final String SHADOW_DOCUMENT_VERSION = "version";
    public static final String SHADOW_DOCUMENT_TIMESTAMP = "timestamp";
    public static final String SHADOW_DOCUMENT_CLIENT_TOKEN = "clientToken";
    public static final String SHADOW_DOCUMENT_STATE = "state";
    public static final String SHADOW_DOCUMENT_METADATA = "metadata";
    public static final String SHADOW_DOCUMENT_STATE_REPORTED = "reported";
    public static final String SHADOW_DOCUMENT_STATE_DESIRED = "desired";
    public static final String SHADOW_DOCUMENT_STATE_DELTA = "delta";
    public static final String SHADOW_DOCUMENT_STATE_PREVIOUS = "previous";
    public static final String SHADOW_DOCUMENT_STATE_CURRENT = "current";
    public static final String STATE_NODE_REQUIRED_PARAM_ERROR_MESSAGE = "State node needs to have to either reported "
            + "or desired node.";
    public static final int DEFAULT_DOCUMENT_STATE_DEPTH = 6;
    public static final int DEFAULT_DOCUMENT_SIZE = 8 * 1024;
    public static final int MAX_ALLOWED_DOCUMENT_SIZE_IN_BYTES = 30 * 1024;
    public static final String LOG_NEXT_TOKEN_KEY = "nextToken";
    public static final String LOG_PAGE_SIZE_KEY = "pageSize";
    public static final String CLASSIC_SHADOW_IDENTIFIER = "";
}
