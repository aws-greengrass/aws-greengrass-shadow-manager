/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.model;

public class Constants {
    public static final String SHADOW_RESOURCE_TYPE = "shadow";
    public static final String SHADOW_RESOURCE_JOINER = "shadow";
    public static final String SHADOW_MANAGER_NAME = "aws.greengrass.ShadowManager";
    public static final String SHADOW_PUBLISH_ACCEPTED_TOPIC = "/accepted";
    public static final String SHADOW_PUBLISH_REJECTED_TOPIC = "/rejected";
    public static final String SHADOW_PUBLISH_DELTA_TOPIC = "/delta";
    public static final String SHADOW_PUBLISH_DOCUMENTS_TOPIC = "/documents";
    public static final String NAMED_SHADOW_TOPIC_PREFIX = "$aws/things/%s/shadow/name/%s";
    public static final String CLASSIC_SHADOW_TOPIC_PREFIX = "$aws/things/%s/shadow";
    public static final String LOG_THING_NAME_KEY = "thing name";
    public static final String LOG_SHADOW_NAME_KEY = "shadow name";
    public static final String SHADOW_DOCUMENT_VERSION = "version";
    public static final String SHADOW_DOCUMENT_TIMESTAMP = "timestamp";
    public static final String SHADOW_DOCUMENT_CLIENT_TOKEN = "clientToken";
    public static final String SHADOW_DOCUMENT_STATE = "state";
    public static final String SHADOW_DOCUMENT_STATE_REPORTED = "reported";
    public static final String SHADOW_DOCUMENT_STATE_DESIRED = "desired";
    public static final String SHADOW_DOCUMENT_STATE_DELTA = "delta";
    public static final String SHADOW_DOCUMENT_STATE_PREVIOUS = "previous";
    public static final String SHADOW_DOCUMENT_STATE_CURRENT = "current";
    public static final String STATE_NODE_REQUIRED_PARAM_ERROR_MESSAGE = "State node needs to have to either reported "
            + "or desired node.";
    public static final int DEFAULT_DOCUMENT_STATE_DEPTH = 6;
    public static final int DEFAULT_DOCUMENT_SIZE = 8 * 1024;
    public static final String LOG_NEXT_TOKEN_KEY = "nextToken";
    public static final String LOG_PAGE_SIZE_KEY = "pageSize";
    public static final String CLASSIC_SHADOW_IDENTIFIER = "";
    public static final int MAX_THING_NAME_LENGTH = 128;
    public static final int MAX_SHADOW_NAME_LENGTH = 64;
    public static final String SHADOW_PATTERN = "[a-zA-Z0-9:_-]+";
    public static final int MIN_PAGE_SIZE = 1;
    public static final int MAX_PAGE_SIZE = 100;
    public static final int DEFAULT_PAGE_SIZE = 25;
    public static final int DEFAULT_OFFSET = 0;
    public static final String CIPHER_TRANSFORMATION = "AES/CBC/PKCS5Padding";
    public static final String ENCRYPTION_ALGORITHM = "AES";
    public static final String SECRET_KEY_ALGORITHM = "PBKDF2WithHmacSHA256";
    public static final int PBE_KEY_ITERATION_COUNT = 65536;
    public static final int PBE_KEY_LENGTH = 256;
}
