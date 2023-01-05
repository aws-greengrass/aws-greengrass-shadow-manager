package com.aws.greengrass;

import com.aws.greengrass.testing.resources.ResourceSpec;
import com.aws.greengrass.testing.resources.iot.IotPolicySpec;
import com.aws.greengrass.testing.resources.iot.IotRoleAliasSpec;
import com.aws.greengrass.testing.resources.iot.IotThingGroupSpec;
import com.aws.greengrass.testing.resources.iot.IotThingSpec;
import com.aws.greengrass.testing.resources.s3.S3BucketSpec;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;

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
//    @Builder.Default
//    @NonNull
//    List<IAMRoleSpec> iamRoles = new ArrayList<>();
//    @Builder.Default
//    @NonNull
//    List<KinesisStreamSpec> kinesisStreams = new ArrayList<>();
//
//    @Builder.Default
//    @NonNull
//    List<IoTSiteWiseModelSpec> ioTSiteWiseModels = new ArrayList<>();
//
//    @Builder.Default
//    @NonNull
//    List<IoTAnalyticsChannelSpec> ioTSiteAnalyticsChannels = new ArrayList<>();
//
//    S3Directory s3Directory;
//
//    @Builder.Default
//    @NonNull
//    List<SecretSpec> secrets = new ArrayList<>();
//
//    @Builder.Default
//    @NonNull
//    List<SqsQueueSpec> sqs = new ArrayList<>();
//
//    @Builder.Default
//    @NonNull
//    List<EventBridgeSpec> eventBridge = new ArrayList<>();
//
//    @Builder.Default
//    @NonNull
//    List<EcrPrivateRepoSpec> ecrPrivateRepos = new ArrayList<>();

    @Builder.Default
    @NonNull
    List<IoTShadowSpec> shadowSpecs = new ArrayList<>();

    @Builder.Default
    @NonNull
    List<IotRoleAliasSpec> iotRoleAliasSpecs= new ArrayList<>();

    @Builder.Default
    @NonNull
    List<IotPolicySpec> iotPolicySpecs = new ArrayList<>();

//    @Builder.Default
//    @NonNull
//    List<SnsTopicSpec> snsTopicSpecs = new ArrayList<>();
//
//    @Builder.Default
//    @NonNull
//    List<FirehoseStreamSpec> firehoseStreamSpecs = new ArrayList<>();

    @Builder.Default
    @NonNull
    List<S3BucketSpec> s3BucketSpecs = new ArrayList<>();
}
