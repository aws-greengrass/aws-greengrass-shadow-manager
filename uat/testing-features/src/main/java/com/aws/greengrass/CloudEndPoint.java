package com.aws.greengrass;

public class CloudEndPoint {
    // Pass in all these cloud endpoints as system properties from outsider orchestrator
    // You can include these in your library as you want
    public static final String GREENGRASS_ENDPOINT = System.getProperty("cmsEndpoint");
    public static final String CLOUDSERVICE_REGION = System.getProperty("cloudServiceRegion");
    public static final String CLOUDSERVICE_STAGE = System.getProperty("cloudServiceStage", "prod");
}
