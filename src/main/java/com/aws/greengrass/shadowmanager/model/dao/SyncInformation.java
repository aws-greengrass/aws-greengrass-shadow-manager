/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.model.dao;

import lombok.Builder;
import lombok.Data;
import lombok.Getter;

import java.time.Instant;

@Builder
@Getter
@Data
public class SyncInformation {
    private String thingName;
    private String shadowName;
    private byte[] cloudDocument;
    private long cloudVersion;
    private long cloudUpdateTime;
    @Builder.Default
    private long lastSyncTime = Instant.now().getEpochSecond();
    private boolean cloudDeleted;
}
