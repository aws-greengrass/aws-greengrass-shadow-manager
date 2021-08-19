## AWS Greengrass Shadow Manager

This is an AWS GreengrassV2 Component that handles offline device shadow
document storage and synchronization to the AWS IoT Device Shadow Service.

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
          coreThing:
            classic: true // default is true
            namedShadows:
            - "foo"
            - "bar"
          # Explicit config for "other" IoT Things
          shadowDocuments:
            <thingName>:
              classic: false
              namedShadows:
              - "foo"
              - "bar"
            <thingName>:
              classic: true
            <thingName>:
              classic: false

        rateLimits:
          # number of outgoing sync updates per second (useful to constrain bandwidth)
          # https://docs.aws.amazon.com/general/latest/gr/iot-core.html#device-shadow-limits
          # 400 is max TPS for some regions (account level), others are 4000          
          maxOutboundSyncUpdatesPerSecond: 50 # default 100
          # Rates for inbound shadow request (overall rate and rate per thing)
          maxTotalLocalRequestsRate = 100 # default 200
          maxLocalRequestsPerSecondPerThing: 10 # default 20 (Iot Device Shadow default value)
        
        # other config
        shadowDocumentSizeLimitBytes: 8192 # default is 8192, max is 30720
```

**JSON example**
```
{
  "synchronize":{
    "coreThing":{
      "classic":true,
      "namedShadows":[
        "foo",
        "bar"
      ]
    },
    "shadowDocuments":{
      "MyThing": {
        "classic":false,
        "namedShadows":[
          "foo",
          "bar"
        ]
      },
      "OtherThing": {
        "classic":true,
        "namedShadows":[
          "foo2"
        ]
      }
    ],
  },
  "rateLimits": {
    "maxOutboundSyncUpdatesPerSecond":50,
    "maxTotalLocalRequestsRate":100,
    "maxLocalRequestsPerSecondPerThing":10
  },
  "shadowDocumentSizeLimitBytes":8192
}
```

### Notes
- For Shadow manager 2.1 synchronize configuration supports both data types for syncing shadows. The sync configuration
can either be a `map` or a `list`. The map approach is preferred since this allows the customers to use the deployment 
`merge` command to add in new shadows to sync.

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.

