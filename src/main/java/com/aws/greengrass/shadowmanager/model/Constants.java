/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.model;

public final class Constants {
    public static final String SHADOW_RESOURCE_TYPE = "shadow";
    public static final String SHADOW_MANAGER_NAME = "aws.greengrass.ShadowManager";
    public static final String SHADOW_PUBLISH_ACCEPTED_TOPIC = "/accepted";
    public static final String SHADOW_PUBLISH_REJECTED_TOPIC = "/rejected";
    public static final String SHADOW_PUBLISH_DELTA_TOPIC = "/delta";
    public static final String SHADOW_PUBLISH_DOCUMENTS_TOPIC = "/documents";
    public static final String SHADOW_UPDATE_SUBSCRIPTION_TOPIC = "/update/accepted";
    public static final String SHADOW_DELETE_SUBSCRIPTION_TOPIC = "/delete/accepted";
    public static final String NAMED_SHADOW_TOPIC_PREFIX = "$aws/things/%s/shadow/name/%s";
    public static final String CLASSIC_SHADOW_TOPIC_PREFIX = "$aws/things/%s/shadow";
    public static final String LOG_THING_NAME_KEY = "thing name";
    public static final String LOG_SHADOW_NAME_KEY = "shadow name";
    public static final String LOG_TOPIC = "topic";
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
    public static final int DEFAULT_DOCUMENT_STATE_DEPTH = 6;
    public static final int DEFAULT_DOCUMENT_SIZE = 8 * 1024;
    public static final int MAX_SHADOW_DOCUMENT_SIZE = 30 * 1024;
    // https://docs.aws.amazon.com/general/latest/gr/iot-core.html#device-shadow-limits
    // 400 is max TPS for some regions (account level), others are 4000
    public static final int DEFAULT_MAX_OUTBOUND_SYNC_UPDATES_PS = 400;
    public static final boolean DEFAULT_PROVIDE_SYNC_STATUS = false;
    public static final int DEFAULT_DISK_UTILIZATION_SIZE_B = 16 * 1024 * 1024;
    public static final String LOG_NEXT_TOKEN_KEY = "nextToken";
    public static final String LOG_PAGE_SIZE_KEY = "pageSize";
    public static final String CLASSIC_SHADOW_IDENTIFIER = "";
    public static final String ERROR_CODE_FIELD_NAME = "code";
    public static final String ERROR_MESSAGE_FIELD_NAME = "message";
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
    public static final int PBE_KEY_ITERATION_COUNT = 65_536;
    public static final int PBE_KEY_LENGTH = 256;
    public static final String CONFIGURATION_SYNCHRONIZATION_TOPIC = "synchronize";
    public static final String CONFIGURATION_NUCLEUS_THING_TOPIC = "nucleusThing";
    public static final String CONFIGURATION_CLASSIC_SHADOW_TOPIC = "classic";
    public static final String CONFIGURATION_NAMED_SHADOWS_TOPIC = "namedShadows";
    public static final String CONFIGURATION_SHADOW_DOCUMENTS_TOPIC = "shadowDocuments";
    public static final String CONFIGURATION_THING_NAME_TOPIC = "thingName";
    public static final String CONFIGURATION_PROVIDE_SYNC_STATUS_TOPIC = "provideSyncStatus";
    public static final String CONFIGURATION_MAX_OUTBOUND_UPDATES_PS_TOPIC = "maxOutboundSyncUpdatesPerSecond";
    public static final String CONFIGURATION_MAX_DOC_SIZE_LIMIT_B_TOPIC = "shadowDocumentSizeLimitBytes";
    public static final String CONFIGURATION_MAX_DISK_UTILIZATION_MB_TOPIC = "maxDiskUtilizationMegaBytes";

    private Constants() {
    }
}
