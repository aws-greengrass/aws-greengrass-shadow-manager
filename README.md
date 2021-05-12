## AWS Greengrass Shadow Manager

This is an AWS GreengrassV2 Component that handles offline device shadow
document storage and synchronization to the IoT Device Shadow Service.

## FAQ

## Sample Configuration
**YAML example**
```
Manifests:
  - Dependencies:
      aws.greengrass.ShadowManager
  - aws.greengrass.ShadowManager:
      Configuration:
        synchronize:
          # explicit config for Nucleus
          nucleusThing:
            classic: true // default is true
            namedShadows:
            - "foo"
            - "bar"
          # Explicit config for "other" IoT Things
          shadowDocuments:
          - thingName: "MyThing"
            classic: false
            namedShadows:
            - "foo"
            - "bar"
          - thingName: "OtherThing"
          - thingName: "YetAnotherThing"
          # include sync status in response messages
          provideSyncStatus: true
          # number of outgoing sync updates per second 
          # (useful to constrain bandwidth)
          # https://docs.aws.amazon.com/general/latest/gr/iot-core.html#device-shadow-limits
          maxOutboundSyncUpdatesPerSecond: 50 # 400 is max TPS for some regions (account level), others are 4000
        # other config
        shadowDocumentSizeLimitBytes: 8192 # default is 8192, max is 30720
        maxDiskUtilizationMegaBytes: 16 # set some max boundary (2000 shadows if there was 0 overhead)
        
        # number of inbound shadow requests per second per thing.
        maxLocalRequestsPerSecondPerThing: 10 # Iot Device Shadow defaults to 20
```

**JSON example**
```
{
  "synchronize":{
    "nucleusThing":{
      "classic":true,
      "namedShadows":[
        "foo",
        "bar"
      ]
    },
    "shadowDocuments":[
      {
        "thingName":"MyThing",
        "classic":false,
        "namedShadows":[
          "foo",
          "bar"
        ]
      },
      {
        "thingName":"OtherThing",
        "classic":true,
        "namedShadows":[
          
        ]
      }
    ],
    "provideSyncStatus":true,
    "maxOutboundSyncUpdatesPerSecond":50
  },
  "shadowDocumentSizeLimitBytes":8192,
  "maxDiskUtilizationMegaBytes":16,
  "maxLocalRequestsPerSecondPerThing":10
}
```

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.

