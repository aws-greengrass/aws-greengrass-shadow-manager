@ShadowManager @Shadow-6 @Greengate
Feature: Shadow-6

  As a customer I can reset the default fields before deployment, it should work correctly.

  Background:
    Given my device is registered as a Thing
    And my device is running Greengrass

  @stable @functional
  Scenario: Shadow-6-T1: As a customer I can reset the default fields before deployment, it should work correctly.
    When I create a Greengrass deployment with components
      | aws.greengrass.ShadowManager | LATEST |
    And I update my Greengrass deployment configuration, setting the component aws.greengrass.ShadowManager configuration to:
      """
      {
         "MERGE":{
            "strategy":{
               "type":"periodic",
               "delay":30
            },
            "maxDiskUtilizationMegaBytes":16
         }
      }
      """
    And I update my Greengrass deployment configuration, setting the component aws.greengrass.Nucleus configuration to:
        """
        {"MERGE":{"logging": {"level": "DEBUG"}}}
        """
    And I deploy the Greengrass deployment configuration
    Then the Greengrass deployment is COMPLETED on the device after 2 minutes