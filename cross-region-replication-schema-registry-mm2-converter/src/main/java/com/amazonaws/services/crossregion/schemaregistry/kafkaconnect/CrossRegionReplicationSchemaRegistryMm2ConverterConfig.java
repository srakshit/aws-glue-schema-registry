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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

/**
 * Amazon Schema Registry MM2 converter config.
 */
public class CrossRegionReplicationSchemaRegistryMm2ConverterConfig extends AbstractConfig {
    public static ConfigDef configDef() {
        return new ConfigDef();
    }
    /**TODO
     * Constructor used by CrossRegionReplicationSchemaRegistryMm2Converter.
     * @param props property elements for the converter config
     */

    public CrossRegionReplicationSchemaRegistryMm2ConverterConfig(Map<String, ?> props) {
        super(configDef(), props);
    }
}
