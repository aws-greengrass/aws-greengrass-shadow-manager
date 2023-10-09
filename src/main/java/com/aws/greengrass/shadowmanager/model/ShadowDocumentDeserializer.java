/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.model;

import com.aws.greengrass.util.SerializerFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.io.IOException;

public class ShadowDocumentDeserializer extends JsonDeserializer<ShadowDocument> {
    // hack to prevent StackOverflowException for custom deserialize,
    // allows us to use jackson's default deserialization for type within
    // a custom deserializer
    static {
        SerializerFactory.getFailSafeJsonObjectMapper().addMixIn(ShadowDocument.class, DefaultJsonDeserializer.class);
    }

    @JsonDeserialize
    private interface DefaultJsonDeserializer {
        // Reset default json deserializer
    }

    @Override
    public ShadowDocument deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {

        ShadowDocument document = ctxt.readValue(p, ShadowDocument.class);
        if (document.getState() == null) { // handle {"state": null} document
            ShadowDocument clearDocument = new ShadowDocument(document);
            clearDocument.getState().setClear(true);
            return clearDocument;
        }
        return document;
    }
}
