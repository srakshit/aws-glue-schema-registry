/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.amazonaws.services.crossregion.schemaregistry.kafkaconnect;

import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializerCrossRegionImpl;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.kafkaconnect.avrodata.AvroData;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializerImpl;

import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

import java.util.Map;

/**
 * Amazon Schema Registry MM2 converter for Kafka Connect users.
 */

@Slf4j
@Data
public class CrossRegionReplicationSchemaRegistryMm2Converter implements Converter {
    private GlueSchemaRegistrySerializerImpl serializer;
    private GlueSchemaRegistryDeserializerCrossRegionImpl remoteDeserializer;
    private AvroData avroData;

    private boolean isKey;

    /**
     * Constructor used by Kafka Connect user.
     */
    public CrossRegionReplicationSchemaRegistryMm2Converter() {
    }

    /**
     * Configure the AWS Schema Registry Converter.
     * @param configs configuration elements for the converter
     * @param isKey true if key, false otherwise
     */
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
	    this.isKey = isKey;
        serializer = new GlueSchemaRegistrySerializerImpl(DefaultCredentialsProvider.builder().build(), new GlueSchemaRegistryConfiguration(configs));
        //TODO: CR_GSR: Add user agent Kafka Connect
        //serializer.setUserAgentApp(UserAgents.KAFKACONNECT);
        remoteDeserializer = new GlueSchemaRegistryDeserializerCrossRegionImpl(DefaultCredentialsProvider.builder().build(), new GlueSchemaRegistryConfiguration(configs));
        //remoteDeserializer.setUserAgentApp(UserAgents.KAFKACONNECT);
    }

    /**
     * Convert orginal Connect data to Schema Registry supported format serialized byte array
     * @param topic topic name
     * @param schema original Connect schema
     * @param value original Connect data
     * @return Schema Registry format serialized byte array
     */
    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        try {
            byte[] deserialized;
            com.amazonaws.services.schemaregistry.common.Schema remoteSchema;
            if (value == null) return new byte[0];

            deserialized = remoteDeserializer.getData((byte[]) value);
            remoteSchema = remoteDeserializer.getSchema((byte[]) value);

            //TODO: CR_GSR: Remove this
            log.info("Deserialised data: " + deserialized.toString());

            byte[] serialized;
            serialized = serializer.encode(topic, remoteSchema, deserialized);
            return serialized;
        } catch (SerializationException | AWSSchemaRegistryException e) {
            throw new DataException("Converting Kafka Connect data to byte[] failed due to serialization/deserialization error: ", e);
        }
    }

    /**
     * Convert Schema Registry supported format serialized byte array to Connect schema and data
     * @param topic topic name
     * @param value Schema Registry format serialized byte array
     * @return Connect schema and data
     */
    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        return SchemaAndValue.NULL;
    }
}
