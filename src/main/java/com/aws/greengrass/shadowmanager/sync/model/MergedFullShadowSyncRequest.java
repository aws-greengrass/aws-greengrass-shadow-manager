/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.model;

import com.aws.greengrass.shadowmanager.exception.RetryableException;
import com.aws.greengrass.shadowmanager.exception.SkipSyncRequestException;
import com.aws.greengrass.shadowmanager.exception.UnknownShadowException;
import com.aws.greengrass.shadowmanager.sync.RequestMerger;
import lombok.Getter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A {@link FullShadowSyncRequest} that was created by merging other {@link SyncRequest}s
 * together. This class keeps track of all such merged requests.
 *
 * <p>When this sync request is executed, it will look through the merged requests
 * and determine if a full sync is truly needed, based on {@link SyncRequest#isUpdateNecessary(SyncContext)}.
 */
public class MergedFullShadowSyncRequest extends FullShadowSyncRequest {

    /**
     * List of individual {@link SyncRequest}s that were merged together to become a full sync request.
     */
    @Getter
    private final List<SyncRequest> mergedRequests = new ArrayList<>();
    private final RequestMerger merger;

    /**
     * Creates a new MergedFullShadowSyncRequest.
     *
     * @param thingName      thing name
     * @param shadowName     shadow name
     * @param merger         merger
     * @param mergedRequests requests that were merged to become a full sync request
     */
    public MergedFullShadowSyncRequest(String thingName, String shadowName,
                                       RequestMerger merger, SyncRequest... mergedRequests) {
        super(thingName, shadowName);
        this.merger = Objects.requireNonNull(merger);
        if (mergedRequests != null && mergedRequests.length > 0) {
            this.mergedRequests.addAll(flatten(Arrays.asList(mergedRequests)));
        }
    }

    @Override
    public void execute(SyncContext context)
            throws RetryableException, SkipSyncRequestException, InterruptedException, UnknownShadowException {
        super.setContext(context);

        List<SyncRequest> necessaryMergedUpdates = getNecessaryMergedRequests(context);
        if (!necessaryMergedUpdates.isEmpty()
                && (necessaryMergedUpdates.stream().allMatch(r -> r instanceof CloudUpdateSyncRequest)
                || necessaryMergedUpdates.stream().allMatch(r -> r instanceof LocalUpdateSyncRequest))) {
            SyncRequest consolidatedUpdateRequest = necessaryMergedUpdates.stream().reduce(merger::merge).get();
            consolidatedUpdateRequest.execute(context);
            return;
        }

        super.execute(context);
    }

    /**
     * Create a list of all the merged requests that require execution,
     * as deemed by {@link SyncRequest#isUpdateNecessary(SyncContext)}.
     *
     * @param context sync context
     * @return sync requests
     * @throws RetryableException       When error occurs in sync operation indicating a request needs to be retried
     * @throws SkipSyncRequestException When error occurs in sync operation indicating a request needs to be skipped.
     * @throws UnknownShadowException   When shadow not found in the sync table.
     */
    private List<SyncRequest> getNecessaryMergedRequests(SyncContext context)
            throws RetryableException, UnknownShadowException, SkipSyncRequestException {
        List<SyncRequest> necessaryUpdates = new ArrayList<>();
        for (SyncRequest request : mergedRequests) {
            if (request.isUpdateNecessary(context)) {
                necessaryUpdates.add(request);
            }
        }
        return necessaryUpdates;
    }

    /**
     * Returns a new list that matches the input, but all {@link MergedFullShadowSyncRequest}s
     * are replaced (flat-mapped) with their {@link MergedFullShadowSyncRequest#mergedRequests}.
     *
     * @param mergedRequests sync requests
     * @return sync requests
     */
    private static List<SyncRequest> flatten(List<SyncRequest> mergedRequests) {
        return mergedRequests.stream()
                .flatMap(r -> {
                    if (r instanceof MergedFullShadowSyncRequest) {
                        MergedFullShadowSyncRequest other = (MergedFullShadowSyncRequest) r;
                        if (other.getMergedRequests() == null) {
                            return Stream.empty();
                        } else {
                            return other.getMergedRequests().stream();
                        }
                    } else {
                        return Stream.of(r);
                    }
                })
                .collect(Collectors.toList());
    }
}
