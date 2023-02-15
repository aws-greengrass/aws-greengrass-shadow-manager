/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync;

import com.aws.greengrass.shadowmanager.sync.model.CloudDeleteSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.CloudUpdateSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.Direction;
import com.aws.greengrass.shadowmanager.sync.model.DirectionWrapper;
import com.aws.greengrass.shadowmanager.sync.model.FullShadowSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.LocalDeleteSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.LocalUpdateSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.OverwriteCloudShadowRequest;
import com.aws.greengrass.shadowmanager.sync.model.OverwriteLocalShadowRequest;
import com.aws.greengrass.shadowmanager.sync.model.SyncRequest;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class RequestMergerTest {

    RequestMerger merger;

    static FullShadowSyncRequest fullShadowSyncRequest = mock(FullShadowSyncRequest.class, "fullShadowSync");

    static OverwriteLocalShadowRequest overwriteLocalShadowRequest = mock(OverwriteLocalShadowRequest.class, "overwriteLocal");

    static OverwriteCloudShadowRequest overwriteCloudShadowRequest = mock(OverwriteCloudShadowRequest.class, "overwriteCloud");

    static CloudUpdateSyncRequest cloudUpdateSyncRequest = mock(CloudUpdateSyncRequest.class, "cloudUpdate");

    static CloudDeleteSyncRequest cloudDeleteSyncRequest = mock(CloudDeleteSyncRequest.class, "cloudDelete");

    static LocalUpdateSyncRequest localUpdateSyncRequest = mock(LocalUpdateSyncRequest.class, "localUpdate");

    static LocalDeleteSyncRequest localDeleteSyncRequest = mock(LocalDeleteSyncRequest.class, "localDelete");

    private final DirectionWrapper direction = new DirectionWrapper();

    @BeforeEach
    void setup() {
        merger = new RequestMerger(direction);
    }

    @ParameterizedTest
    @MethodSource("overridingRequests")
    void GIVEN_overriding_requests_WHEN_merge_THEN_return_overriding_request(SyncRequest old, SyncRequest value,
            SyncRequest expected, Direction direction) {
        this.direction.setDirection(direction);
        assertThat(merger.merge(old, value), is(instanceOf(expected.getClass())));
    }

    static Stream<Arguments> overridingRequests() {
        return Stream.of(
                arguments(fullShadowSyncRequest, cloudUpdateSyncRequest, fullShadowSyncRequest, Direction.BETWEEN_DEVICE_AND_CLOUD),
                arguments(cloudUpdateSyncRequest, fullShadowSyncRequest, fullShadowSyncRequest, Direction.BETWEEN_DEVICE_AND_CLOUD),
                arguments(overwriteLocalShadowRequest, cloudUpdateSyncRequest, overwriteLocalShadowRequest, Direction.CLOUD_TO_DEVICE),
                arguments(cloudUpdateSyncRequest, overwriteLocalShadowRequest, overwriteLocalShadowRequest, Direction.CLOUD_TO_DEVICE),
                arguments(overwriteCloudShadowRequest, cloudUpdateSyncRequest, overwriteCloudShadowRequest, Direction.DEVICE_TO_CLOUD),
                arguments(cloudUpdateSyncRequest, overwriteCloudShadowRequest, overwriteCloudShadowRequest, Direction.DEVICE_TO_CLOUD),
                arguments(cloudUpdateSyncRequest, cloudDeleteSyncRequest, cloudDeleteSyncRequest, Direction.BETWEEN_DEVICE_AND_CLOUD),
                arguments(localUpdateSyncRequest, localDeleteSyncRequest, localDeleteSyncRequest, Direction.BETWEEN_DEVICE_AND_CLOUD),
                arguments(cloudUpdateSyncRequest, localDeleteSyncRequest, localDeleteSyncRequest, Direction.BETWEEN_DEVICE_AND_CLOUD),
                arguments(localUpdateSyncRequest, cloudDeleteSyncRequest, cloudDeleteSyncRequest, Direction.BETWEEN_DEVICE_AND_CLOUD),
                arguments(cloudDeleteSyncRequest, localUpdateSyncRequest, cloudDeleteSyncRequest, Direction.BETWEEN_DEVICE_AND_CLOUD),
                arguments(localDeleteSyncRequest, cloudUpdateSyncRequest, localDeleteSyncRequest, Direction.BETWEEN_DEVICE_AND_CLOUD)
        );
    }

    @ParameterizedTest
    @MethodSource("nonMergingRequests")
    void GIVEN_non_mergable_request_WHEN_merge_THEN_return_full_shadow_sync(SyncRequest request1,
            SyncRequest request2, Direction direction) {
        this.direction.setDirection(direction);
        switch (direction) {
            case BETWEEN_DEVICE_AND_CLOUD:
                assertThat(merger.merge(request1, request2), is(instanceOf(FullShadowSyncRequest.class)));
                break;
            case DEVICE_TO_CLOUD:
                assertThat(merger.merge(request1, request2), is(instanceOf(OverwriteCloudShadowRequest.class)));
                break;
            case CLOUD_TO_DEVICE:
                assertThat(merger.merge(request1, request2), is(instanceOf(OverwriteLocalShadowRequest.class)));
                break;
        }
    }

    static Stream<Arguments> nonMergingRequests() {
        return Stream.of(
                arguments(localUpdateSyncRequest, cloudUpdateSyncRequest, Direction.BETWEEN_DEVICE_AND_CLOUD),
                arguments(cloudUpdateSyncRequest, localUpdateSyncRequest, Direction.BETWEEN_DEVICE_AND_CLOUD),
                arguments(localDeleteSyncRequest, cloudDeleteSyncRequest, Direction.BETWEEN_DEVICE_AND_CLOUD),
                arguments(localUpdateSyncRequest, cloudUpdateSyncRequest, Direction.CLOUD_TO_DEVICE),
                arguments(cloudUpdateSyncRequest, localUpdateSyncRequest, Direction.CLOUD_TO_DEVICE),
                arguments(localDeleteSyncRequest, cloudDeleteSyncRequest, Direction.CLOUD_TO_DEVICE),
                arguments(localUpdateSyncRequest, cloudUpdateSyncRequest, Direction.DEVICE_TO_CLOUD),
                arguments(cloudUpdateSyncRequest, localUpdateSyncRequest, Direction.DEVICE_TO_CLOUD),
                arguments(localDeleteSyncRequest, cloudDeleteSyncRequest, Direction.DEVICE_TO_CLOUD)
        );
    }

    @Test
    void GIVEN_update_mergeable_request_WHEN_merge_THEN_return_merged_shadow_sync() {
        LocalUpdateSyncRequest request1 = mock(LocalUpdateSyncRequest.class, "localUpdate1");
        LocalUpdateSyncRequest request2 = mock(LocalUpdateSyncRequest.class, "localUpdate2");
        assertThat(merger.merge(request1, request2), is(instanceOf(LocalUpdateSyncRequest.class)));
    }

}
