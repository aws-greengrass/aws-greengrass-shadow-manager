package com.aws.greengrass;

//import com.amazonaws.services.eventbridge.AmazonEventBridge;
//import com.amazonaws.services.greengrass.AWSGreengrass;
//import com.amazonaws.services.greengrassv2.AWSGreengrassV2;
//import com.amazonaws.services.greengrassv2.model.DeleteCoreDeviceRequest;
//import com.amazonaws.services.greengrassv2.model.ResourceNotFoundException;
//import com.amazonaws.services.greengrassv2.model.ValidationException;
//import com.amazonaws.services.iotanalytics.AWSIoTAnalytics;
//import com.amazonaws.services.iotsitewise.AWSIoTSiteWise;
//import com.amazonaws.services.kinesis.AmazonKinesis;
//import com.amazonaws.services.s3.AmazonS3;
//import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagement;
import com.aws.greengrass.testing.resources.iot.IotCertificate;
import com.aws.greengrass.testing.resources.iot.IotPolicy;
import com.aws.greengrass.testing.resources.iot.IotRoleAlias;
import com.aws.greengrass.testing.resources.iot.IotThing;
import com.aws.greengrass.testing.resources.iot.IotThingGroup;
import com.aws.greengrass.testing.resources.s3.S3Bucket;
import lombok.Data;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
//import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
//import software.amazon.awssdk.services.ecr.EcrClient;
//import software.amazon.awssdk.services.firehose.FirehoseClient;
import software.amazon.awssdk.services.iam.IamClient;
import software.amazon.awssdk.services.iam.model.Tag;
import software.amazon.awssdk.services.iot.IotClient;
//import software.amazon.awssdk.services.lambda.LambdaClient;
//import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
//import software.amazon.awssdk.services.sns.SnsClient;
//import software.amazon.awssdk.services.sqs.SqsClient;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import javax.inject.Inject;

@Getter
@Log4j2
public class IotShadowResources implements Closeable {
    protected static int MAX_RETRY_NUM = 5;
    protected static int INITIAL_RETRY_INTERVAL_SEC = 2;
    protected static int MAX_RETRY_INTERVAL_SEC = 30;

    private final IotShadowResourceSpec specs;
    private final Resources resources;
    private final IotClient iotClient;
    private final IamClient iamClient;
//    private final LambdaClient lambdaClient;
//    private final AWSGreengrassV2 greengrass;
//    private final AWSGreengrass greengrassV1;
//    private final AmazonKinesis kinesisClient;
//    private final AWSIoTSiteWise ioTSiteWiseClient;
//    private final AWSIoTAnalytics ioTAnalyticsClient;
//    private final CloudWatchLogsClient cloudWatchLogsClient;
//    private final AmazonS3 s3Client;
//    private final SecretsManagerClient secretsManagerClient;
//    private final AmazonEventBridge eventBridgeClient;
//    private final SqsClient sqsClient;
//    private final AWSSimpleSystemsManagement ssmClient;
//    private final EcrClient ecrClient;
//    private final IotDataPlaneClient iotDataPlaneClient;
//    private final SnsClient snsClient;
//    private final FirehoseClient firehoseClient;

    @Inject
    public IotShadowResources(IotClient iotClient, IamClient iamClient//,
//                              LambdaClient lambdaClient,
//                        AWSGreengrassV2 greengrass, AWSGreengrass greengrassV1, AmazonKinesis kinesisClient,
//                        CloudWatchLogsClient cloudWatchLogsClient, AmazonS3 s3Client,
//                        SecretsManagerClient secretsManagerClient, AWSIoTSiteWise ioTSiteWiseClient,
//                        AWSIoTAnalytics ioTAnalyticsClient, SqsClient sqsClient, AmazonEventBridge eventBridgeClient,
//                        AWSSimpleSystemsManagement ssmClient, EcrClient ecrClient,
//                        IotDataPlaneClient iotDataPlaneClient, SnsClient snsClient, FirehoseClient firehoseClient
                               ) {
        this.specs = IotShadowResourceSpec.builder().build();
        this.iamClient = iamClient;
        this.iotClient = iotClient;
//        this.lambdaClient = lambdaClient;
//        this.greengrass = greengrass;
//        this.greengrassV1 = greengrassV1;
//        this.kinesisClient = kinesisClient;
//        this.ioTSiteWiseClient = ioTSiteWiseClient;
//        this.cloudWatchLogsClient = cloudWatchLogsClient;
//        this.s3Client = s3Client;
//        this.secretsManagerClient = secretsManagerClient;
//        this.ioTAnalyticsClient = ioTAnalyticsClient;
//        this.eventBridgeClient = eventBridgeClient;
//        this.sqsClient = sqsClient;
//        this.ssmClient = ssmClient;
//        this.ecrClient = ecrClient;
//        this.iotDataPlaneClient = iotDataPlaneClient;
//        this.snsClient = snsClient;
//        this.firehoseClient = firehoseClient;
        this.resources = new Resources(this);
    }

    public void create() {
        log.atInfo().log("Creating all AWS resources");

//        specs.iamRoles.stream().filter((r) -> !r.isCreated()).forEach(r -> {
//            Result<Pair<IAMRole, IAMPolicy>> creation;
//            try {
//                creation = IAMRole.create(iamClient, r);
//            } catch (InterruptedException ie) {
//                log.atError().log("Interrupted in creating iam role");
//                return;
//            }
//            r.setCreated(true);
//
//            if (creation.getResult() != null) {
//                resources.iamRoles.add(creation.getResult().getL());
//                r.setResultingRole(creation.getResult().getL());
//                if (creation.getResult().getR() != null) {
//                    resources.iamPolicies.add(creation.getResult().getR());
//                }
//            }
//            if (!creation.ok()) {
//                throw creation.getError();
//            }
//        });
//        specs.rootThingGroups.stream().filter((t) -> !t.isCreated()).forEach(g -> {
//            // Child thing groups are added to resources in the creation method below
//            IotThingGroup rootThingGroup = IotThingGroup.create(resources, iotClient, g);
//            resources.rootThingGroups.add(rootThingGroup);
//        });
//
//        specs.things.stream().filter((t) -> !t.isCreated()).forEach(t -> {
//            IotThing thing = IotThing.create(iotClient, t);
//            t.setCreated(true);
//            t.setResultingThing(thing);
//            resources.getThings().add(thing);
//
//            // Add the newly created thing into each of the groups that it wanted.
//            // This is safe because we already created the groups in step 1.
//            if (t.thingGroups != null) {
//                thing.addToThingGroup(iotClient,
//                        resources.getThingGroups().stream().filter(x -> t.thingGroups.contains(x.getGroupName()))
//                                .findAny().get());
//            }
//
//            if (t.getRoleAliasSpec() != null) {
//                createIotRoleAlias(t.getRoleAliasSpec());
//            }
//
//            if (t.createCertificate) {
//                if (t.getRoleAliasSpec() != null && t.getRoleAliasSpec().isCreated()) {
//                    t.getPolicySpec().setRoleAliasArn(t.getRoleAliasSpec().getResultingRoleAlias().getRoleAliasArn());
//                }
//                Result<Pair<IotCertificate, IotPolicy>> possibleCert =
//                        IotCertificate.create(iotClient, t.getIotThingName(), t.policySpec);
//                // Even if one call failed, make sure to track the certificate and policy so that they can be cleaned up
//                if (possibleCert.getResult() != null) {
//                    if (possibleCert.getResult().getL() != null) {
//                        resources.getCertificates().add(possibleCert.getResult().getL());
//                        thing.setCertificate(possibleCert.getResult().getL());
//                    }
//                    if (possibleCert.getResult().getR() != null) {
//                        resources.getPolicies().add(possibleCert.getResult().getR());
//                    }
//                }
//                if (!possibleCert.ok()) {
//                    throw possibleCert.getError();
//                }
//            }
//        });
//
//        specs.kinesisStreams.stream().filter((t) -> !t.isCreated()).forEach(k -> {
//            KinesisStream kinesisStream = KinesisStream.create(kinesisClient, k);
//            k.setCreated(true);
//            k.setResultingStream(kinesisStream);
//            resources.getKinesisStreams().add(kinesisStream);
//        });
//
//        specs.secrets.stream().filter((t) -> !t.isCreated()).forEach(k -> {
//            Secret secret = Secret.create(secretsManagerClient, k);
//            k.setCreated(true);
//            k.setSecretId(secret.getSecretId());
//            resources.getSecrets().add(secret);
//        });
//
//        specs.ioTSiteWiseModels.stream().filter((t) -> !t.isCreated()).forEach(k -> {
//            IoTSiteWiseModel ioTSiteWiseModel = IoTSiteWiseModel.create(ioTSiteWiseClient, k);
//            k.setCreated(true);
//            k.setIoTSiteWiseModel(ioTSiteWiseModel);
//            resources.getIoTSiteWiseModels().add(ioTSiteWiseModel);
//        });
//
//        specs.ioTSiteAnalyticsChannels.stream().filter((t) -> !t.isCreated()).forEach(k -> {
//            IoTAnalyticsChannel ioTAnalyticsChannel = IoTAnalyticsChannel.create(ioTAnalyticsClient, k);
//            k.setCreated(true);
//            k.setIoTAnalytics(ioTAnalyticsChannel);
//            resources.getIoTAnalyticsChannels().add(ioTAnalyticsChannel);
//        });
//
//        specs.sqs.stream().filter((t) -> !t.isCreated()).forEach(k -> {
//            SqsQueue sqsQueue = SqsQueue.create(sqsClient, k);
//            k.setCreated(true);
//            k.setResultingQueue(sqsQueue);
//            resources.getSqsQueue().add(sqsQueue);
//        });
//
//        specs.eventBridge.stream().filter((t) -> !t.isCreated()).forEach(k -> {
//            EventBridge eventBridge = EventBridge.create(eventBridgeClient, k);
//            k.setCreated(true);
//            k.setResultingEventBridge(eventBridge);
//            resources.getEventBridge().add(eventBridge);
//        });
//
//        specs.ecrPrivateRepos.stream().filter((t) -> !t.isCreated()).forEach(k -> {
//            EcrPrivateRepo ecrPrivateRepo = EcrPrivateRepo.create(ecrClient, k);
//            k.setCreated(true);
//            k.setResultingRepo(ecrPrivateRepo);
//            resources.getEcrPrivateRepos().add(ecrPrivateRepo);
//        });
//
//        specs.shadowSpecs.stream().filter((t) -> !t.isCreated()).forEach(k -> {
//            IoTShadow ioTShadow = IoTShadow.create(iotDataPlaneClient, k);
//            k.setCreated(true);
//            k.setResultingIotShadow(ioTShadow);
//            resources.getIoTShadows().add(ioTShadow);
//        });
//
//        specs.snsTopicSpecs.stream().filter((t) -> !t.isCreated()).forEach(k -> {
//            SnsTopic snsTopic = SnsTopic.create(snsClient, k);
//            k.setCreated(true);
//            k.setResultingTopic(snsTopic);
//            resources.getSnsTopics().add(snsTopic);
//        });
//
//        specs.firehoseStreamSpecs.stream().filter((t) -> !t.isCreated()).forEach(k -> {
//            try {
//                FirehoseStream firehoseStream = FirehoseStream.create(firehoseClient, k);
//                k.setCreated(true);
//                k.setResultingStream(firehoseStream);
//                resources.getFirehoseStreams().add(firehoseStream);
//            }
//            catch (InterruptedException ie) {
//                log.atError().log("Interrupted in creating Firehose Stream");
//            }
//        });
//
//        specs.s3BucketSpecs.stream().filter((t) -> !t.isCreated()).forEach(k -> {
//            S3Bucket s3Bucket = S3Bucket.create(s3Client, k);
//            k.setCreated(true);
//            k.setResultingBucket(s3Bucket);
//            resources.getS3Buckets().add(s3Bucket);
//        });
//
//        specs.getIotRoleAliasSpecs().stream().filter(ra -> !ra.isCreated())
//                .forEach(ra -> createIotRoleAlias(ra));
//
//        specs.getIotPolicySpecs().stream().filter(ip -> !ip.isCreated())
//                .forEach(ip -> createIotPolicy(ip));

        log.atInfo().log("All requested AWS resources created {}", resources);
    }

//    private void createIotPolicy(IotPolicySpec ip) {
//        IotPolicy policy = IotPolicy.create(iotClient, ip);
//        ip.setCreated(true);
//        ip.setResultingPolicy(policy);
//        resources.policies.add(policy);
//    }
//
//    private void createIotRoleAlias(IotRoleAliasSpec ra) {
//        Result<Pair<IAMRole, IAMPolicy>> creation;
//        try {
//            creation = IAMRole.create(iamClient, ra.getRoleSpecBuilder().build());
//        } catch (InterruptedException ie) {
//            log.atError().log("Interrupted in creating iam role");
//            return;
//        }
//        if (creation.getResult() != null) {
//            resources.iamRoles.add(creation.getResult().getL());
//            ra.setCreatedRole(creation.getResult().getL());
//            if (creation.getResult().getR() != null) {
//                resources.iamPolicies.add(creation.getResult().getR());
//            }
//        }
//        if (!creation.ok()) {
//            throw creation.getError();
//        }
//
//        IotRoleAlias roleAlias = IotRoleAlias.create(iotClient, ra);
//        resources.getRoleAliases().add(roleAlias);
//        ra.setCreated(true);
//        ra.setResultingRoleAlias(roleAlias);
//    }

    @Override
    @ResourceRelease
    public void close() throws IOException {
        resources.close();
    }

    @Data
    public static class Resources implements Closeable {
        IotClient iotClient;
        IamClient iamClient;
//        LambdaClient lambdaClient;
//        AWSGreengrassV2 greengrass;
//        AmazonKinesis kinesisClient;
//        AWSIoTSiteWise ioTSiteWiseClient;
//        AWSIoTAnalytics ioTAnalyticsClient;
//        CloudWatchLogsClient cloudWatchLogsClient;
//        AmazonS3 s3Client;
//        SecretsManagerClient secretsManagerClient;
//        AmazonEventBridge eventBridgeClient;
//        SqsClient sqsClient;
//        EcrClient ecrClient;
//        IotDataPlaneClient iotDataPlaneClient;
//        SnsClient snsClient;
//        FirehoseClient firehoseClient;

        public Resources(final IotShadowResources resources) {
            this.iotClient = resources.iotClient;
            this.iamClient = resources.iamClient;
//            this.lambdaClient = resources.lambdaClient;
//            this.greengrass = resources.greengrass;
//            this.kinesisClient = resources.kinesisClient;
//            this.ioTSiteWiseClient = resources.ioTSiteWiseClient;
//            this.cloudWatchLogsClient = resources.cloudWatchLogsClient;
//            this.ioTAnalyticsClient = resources.ioTAnalyticsClient;
//            this.s3Client = resources.s3Client;
//            this.secretsManagerClient = resources.secretsManagerClient;
//            this.eventBridgeClient = resources.eventBridgeClient;
//            this.sqsClient = resources.sqsClient;
//            this.ecrClient = resources.ecrClient;
//            this.snsClient = resources.snsClient;
//            this.firehoseClient = resources.firehoseClient;
//            this.iotDataPlaneClient = resources.iotDataPlaneClient;
        }

        List<IotThing> things = new ArrayList<>();
        List<IotThingGroup> rootThingGroups = new ArrayList<>();   // group hierarchy for creation and removal
        List<IotThingGroup> thingGroups = new ArrayList<>();  // flat list of all groups for easy lookup
        List<IotPolicy> policies = new ArrayList<>();
        List<IotRoleAlias> roleAliases = new ArrayList<>();
        List<IotCertificate> certificates = new ArrayList<>();
//        List<IAMRole> iamRoles = new ArrayList<>();
//        List<IAMPolicy> iamPolicies = new ArrayList<>();
//        List<LambdaFunction> lambdaFunctions = new ArrayList<>();
//        List<EvergreenComponent> evergreenComponents = new ArrayList<>();
//        List<EvergreenComponent> evergreenPublicComponents = new ArrayList<>();
//        List<EvergreenDeployment> evergreenDeployments = new ArrayList<>();
//        List<KinesisStream> kinesisStreams = new ArrayList<>();
//        List<IoTSiteWiseModel> ioTSiteWiseModels = new ArrayList<>();
//        List<IoTAnalyticsChannel> ioTAnalyticsChannels = new ArrayList<>();
//        List<CloudWatchLogStream> cloudWatchLogStreams = new ArrayList<>();
//        S3Directory s3Directory;
//        List<Secret> secrets = new ArrayList<>();
//        List<SqsQueue> sqsQueue = new ArrayList<>();
//        List<EventBridge> eventBridge = new ArrayList<>();
//        List<EcrPrivateRepo> ecrPrivateRepos = new ArrayList<>();
        List<IoTShadow> ioTShadows = new ArrayList<>();
//        List<SnsTopic> snsTopics = new ArrayList<>();
//        List<FirehoseStream> firehoseStreams = new ArrayList<>();
        List<S3Bucket> s3Buckets = new ArrayList<>();

        @Override
        public void close() throws IOException {
            log.atInfo().log("Removing all AWS resources");

            final AtomicReference<Throwable> ex = new AtomicReference<>(null);
            Consumer<Throwable> updateEx = (e) -> {
                if (ex.get() == null) {
                    ex.set(e);
                } else {
                    ex.get().addSuppressed(e);
                }
                log.atError().log("Exception while removing resource", e);
            };

//            iamRoles.stream().filter(r -> !r.isPersist()).forEach(role -> {
//                try {
//                    role.remove(iamClient);
//                } catch (Throwable e) {
//                    updateEx.accept(e);
//                }
//            });
//
//            iamPolicies.stream().filter(p -> !p.isPersist()).forEach(policy -> {
//                try {
//                    policy.remove(iamClient);
//                } catch (Throwable e) {
//                    updateEx.accept(e);
//                }
//            });
//
//            for (EvergreenDeployment configuration : evergreenDeployments) {
//                try {
//                    configuration.remove(greengrass);
//                } catch (Throwable e) {
//                    // Ignore if the deployment is already canceled
//                    if (!(e instanceof ValidationException)) {
//                        updateEx.accept(e);
//                    }
//                }
//            }
//
//            // Delete Greengrass Cores using the Thing list
//            for (IotThing thing : things) {
//                try {
//                    greengrass.deleteCoreDevice(
//                            new DeleteCoreDeviceRequest().withCoreDeviceThingName(thing.getThingName()));
//                } catch (ResourceNotFoundException ignored) {
//                    // If the thing isn't a core, then that's fine, no big deal
//                } catch (Throwable e) {
//                    updateEx.accept(e);
//                }
//            }

            for (IotThing thing : things) {
                try {
                    thing.remove(iotClient);
                } catch (Throwable e) {
                    updateEx.accept(e);
                }
            }

            for (IotThingGroup group : rootThingGroups) {
                try {
                    group.remove(iotClient);
                } catch (Throwable e) {
                    updateEx.accept(e);
                }
            }

//            for (IotPolicy policy : policies) {
//                try {
//                    if (!policy.isPersist()) {
//                        policy.remove(iotClient);
//                    }
//                } catch (Throwable e) {
//                    updateEx.accept(e);
//                }
//            }
//
//            roleAliases.stream().filter(r -> !r.isPersist()).forEach(alias -> {
//                try {
//                    alias.remove(iotClient);
//                } catch (Throwable e) {
//                    updateEx.accept(e);
//                }
//            });

            for (IotCertificate certificate : certificates) {
                try {
                    certificate.remove(iotClient);
                } catch (Throwable e) {
                    updateEx.accept(e);
                }
            }

//            for (LambdaFunction lambda : lambdaFunctions) {
//                try {
//                    lambda.remove(lambdaClient);
//                } catch (Throwable e) {
//                    updateEx.accept(e);
//                }
//            }
//
//            for (KinesisStream kinesisStream: kinesisStreams) {
//                try {
//                    kinesisStream.remove(kinesisClient);
//                } catch (Throwable e) {
//                    updateEx.accept(e);
//                }
//            }
//
//            for (IoTSiteWiseModel ioTSiteWiseModel: ioTSiteWiseModels) {
//                try {
//                    ioTSiteWiseModel.remove(ioTSiteWiseClient);
//                } catch (Throwable e) {
//                    updateEx.accept(e);
//                }
//            }
//
//            for (IoTAnalyticsChannel ioTA: ioTAnalyticsChannels) {
//                try {
//                    ioTA.remove(ioTAnalyticsClient);
//                } catch (Throwable e) {
//                    updateEx.accept(e);
//                }
//            }
//
//            for (EvergreenComponent component : evergreenComponents) {
//                try {
//                    component.remove(greengrass);
//                } catch(com.amazonaws.services.greengrassv2.model.ResourceNotFoundException
//                        | com.amazonaws.services.greengrassv2.model.ValidationException e) {
//                    log.info("Component already deleted");
//                } catch (Throwable e) {
//                    updateEx.accept(e);
//                }
//            }
//
//            for (CloudWatchLogStream stream : cloudWatchLogStreams) {
//                try {
//                    stream.remove(cloudWatchLogsClient);
//                } catch (Throwable e) {
//                    updateEx.accept(e);
//                }
//            }
//
//            if (s3Directory != null) {
//                try {
//                    s3Directory.remove(s3Client);
//                } catch (Throwable e) {
//                    updateEx.accept(e);
//                }
//            }
//
//            for (Secret secret: secrets) {
//                try {
//                    secret.remove(secretsManagerClient);
//                } catch (Throwable e) {
//                    updateEx.accept(e);
//                }
//            }
//
//            for (SqsQueue q: sqsQueue) {
//                try {
//                    q.remove(sqsClient);
//                } catch (Throwable e) {
//                    updateEx.accept(e);
//                }
//            }
//
//            for (EventBridge eventBridge :eventBridge) {
//                try {
//                    eventBridge.remove(eventBridgeClient);
//                } catch (Throwable e) {
//                    updateEx.accept(e);
//                }
//            }
//
//            for (EcrPrivateRepo ecrPrivateRepo : ecrPrivateRepos) {
//                try {
//                    ecrPrivateRepo.remove(ecrClient);
//                } catch (Throwable e) {
//                    updateEx.accept(e);
//                }
//            }
//
//            for (IoTShadow ioTShadow : ioTShadows) {
//                try {
//                    ioTShadow.remove(iotDataPlaneClient);
//                } catch (Throwable e) {
//                    updateEx.accept(e);
//                }
//            }

//            for (SnsTopic snsTopic : snsTopics) {
//                try {
//                    snsTopic.remove(snsClient);
//                } catch (Throwable e) {
//                    updateEx.accept(e);
//                }
//            }
//
//            for (FirehoseStream firehoseStream : firehoseStreams) {
//                try {
//                    firehoseStream.remove(firehoseClient);
//                } catch (Throwable e) {
//                    updateEx.accept(e);
//                }
//            }
//
//            for (S3Bucket s3Bucket : s3Buckets) {
//                try {
//                    s3Bucket.remove(s3Client);
//                } catch (Throwable e) {
//                    updateEx.accept(e);
//                }
//            }

            if (ex.get() != null) {
                throw new IOException(ex.get());
            }
        }
    }

    public static String randomName() {
        return String.format("e2e-%d-%s", System.currentTimeMillis(), UUID.randomUUID().toString());
    }

    static Tag getIamTag() {
        return Tag.builder().key("test").value("true").build();
    }

    static software.amazon.awssdk.services.iot.model.Tag getIotTag() {
        return software.amazon.awssdk.services.iot.model.Tag.builder().key("test").value("true").build();
    }
}
