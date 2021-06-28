/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync;

import com.aws.greengrass.shadowmanager.sync.model.CloudDeleteSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.CloudUpdateSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.FullShadowSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.LocalDeleteSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.LocalUpdateSyncRequest;
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

    static CloudUpdateSyncRequest cloudUpdateSyncRequest = mock(CloudUpdateSyncRequest.class, "cloudUpdate");

    static CloudDeleteSyncRequest cloudDeleteSyncRequest = mock(CloudDeleteSyncRequest.class, "cloudDelete");

    static LocalUpdateSyncRequest localUpdateSyncRequest = mock(LocalUpdateSyncRequest.class, "localUpdate");

    static LocalDeleteSyncRequest localDeleteSyncRequest = mock(LocalDeleteSyncRequest.class, "localDelete");

    @BeforeEach
    void setup() {
        merger = new RequestMerger();
    }

    @ParameterizedTest
    @MethodSource("overridingRequests")
    void GIVEN_overriding_requests_WHEN_merge_THEN_return_overriding_request(SyncRequest old, SyncRequest value,
            SyncRequest expected) {
        assertThat(merger.merge(old, value), is(expected));
    }

    static Stream<Arguments> overridingRequests() {
        return Stream.of(
                arguments(fullShadowSyncRequest, cloudUpdateSyncRequest, fullShadowSyncRequest),
                arguments(cloudUpdateSyncRequest, fullShadowSyncRequest, fullShadowSyncRequest),
                arguments(cloudUpdateSyncRequest, cloudDeleteSyncRequest, cloudDeleteSyncRequest),
                arguments(localUpdateSyncRequest, localDeleteSyncRequest, localDeleteSyncRequest),
                arguments(cloudUpdateSyncRequest, localDeleteSyncRequest, localDeleteSyncRequest),
                arguments(localUpdateSyncRequest, cloudDeleteSyncRequest, cloudDeleteSyncRequest),
                arguments(cloudDeleteSyncRequest, localUpdateSyncRequest, cloudDeleteSyncRequest),
                arguments(localDeleteSyncRequest, cloudUpdateSyncRequest, localDeleteSyncRequest)
        );
    }

    @ParameterizedTest
    @MethodSource("nonMergingRequests")
    void GIVEN_non_mergable_request_WHEN_merge_THEN_return_full_shadow_sync(SyncRequest request1,
            SyncRequest request2) {
        assertThat(merger.merge(request1, request2), is(instanceOf(FullShadowSyncRequest.class)));
    }

    static Stream<Arguments> nonMergingRequests() {
        return Stream.of(
                arguments(localUpdateSyncRequest, cloudUpdateSyncRequest),
                arguments(cloudUpdateSyncRequest, localUpdateSyncRequest),
                arguments(localDeleteSyncRequest, cloudDeleteSyncRequest)
        );
    }

    @Test
    void GIVEN_update_mergeable_request_WHEN_merge_THEN_return_merged_shadow_sync() {
        LocalUpdateSyncRequest request1 = mock(LocalUpdateSyncRequest.class, "localUpdate1");
        LocalUpdateSyncRequest request2 = mock(LocalUpdateSyncRequest.class, "localUpdate2");
        assertThat(merger.merge(request1, request2), is(instanceOf(LocalUpdateSyncRequest.class)));
    }

}
