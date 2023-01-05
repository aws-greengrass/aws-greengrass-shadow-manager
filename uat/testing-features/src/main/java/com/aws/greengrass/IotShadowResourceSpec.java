/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass;

import com.aws.greengrass.testing.resources.iot.IotPolicySpec;
import com.aws.greengrass.testing.resources.iot.IotRoleAliasSpec;
import com.aws.greengrass.testing.resources.iot.IotThingGroupSpec;
import com.aws.greengrass.testing.resources.iot.IotThingSpec;
import com.aws.greengrass.testing.resources.s3.S3BucketSpec;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.List;


@Builder
@Data
public class IotShadowResourceSpec {
    @Builder.Default
    @NonNull
    List<IotThingGroupSpec> rootThingGroups = new ArrayList<>();
    @Builder.Default
    @NonNull
    List<IotThingSpec> things = new ArrayList<>();

    @Builder.Default
    @NonNull
    List<IoTShadowSpec> shadowSpecs = new ArrayList<>();

    @Builder.Default
    @NonNull
    List<IotRoleAliasSpec> iotRoleAliasSpecs= new ArrayList<>();

    @Builder.Default
    @NonNull
    List<IotPolicySpec> iotPolicySpecs = new ArrayList<>();

    @Builder.Default
    @NonNull
    List<S3BucketSpec> s3BucketSpecs = new ArrayList<>();
}
