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

package com.amazonaws.services.schemaregistry.serializers;

import com.amazonaws.services.schemaregistry.common.AWSSchemaRegistryClient;
import com.amazonaws.services.schemaregistry.common.AWSSerializerInput;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.serializers.avro.GlueSchemaRegistryValidationUtil;
import com.amazonaws.services.schemaregistry.serializers.avro.CustomerProvidedSchemaNamingStrategy;
import com.amazonaws.services.schemaregistry.serializers.avro.User;
import com.amazonaws.services.schemaregistry.utils.AVROUtils;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.RecordGenerator;
import com.amazonaws.services.schemaregistry.utils.SchemaLoader;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.glue.model.DataFormat;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GlueSchemaRegistryKafkaSerializerTest extends GlueSchemaRegistryValidationUtil {
    private AWSSchemaRegistryClient mockClient;
    private final Map<String, Object> configs = new HashMap<>();
    private static final UUID USER_SCHEMA_VERSION_ID = UUID.fromString("b7b4a7f0-9c96-4e4a-a687-fb5de9ef0c63");
    private static final UUID EMPLOYEE_SCHEMA_VERSION_ID = UUID.fromString("2f8e6498-29af-4722-b4ae-80f2be386bee");
    private static final String AVRO_USER_SCHEMA_FILE = "src/test/resources/avro/user.avsc";
    private static final String AVRO_EMP_RECORD_SCHEMA_FILE_PATH = "src/test/resources/avro/emp_record.avsc";

    private static Schema userAvroSchema;
    private static String userSchemaDefinition;
    private static Schema employeeAvroSchema;
    private static String employeeSchemaDefinition;
    private User userDefinedPojo;
    private static GenericRecord genericUserAvroRecord;
    private static GenericRecord genericEmployeeAvroRecord;
    private static Map<String, UUID> schemaDefinitionToSchemaVersionIdMap = new HashMap<>();

    @BeforeEach
    public void setup() {
        mockClient = mock(AWSSchemaRegistryClient.class);

        userDefinedPojo = User.newBuilder().setName("test_avros_schema").setFavoriteColor("violet")
                .setFavoriteNumber(10).build();
        Map<String, String> testTags = new HashMap<>();
        testTags.put("testKey", "testValue");

        userAvroSchema = SchemaLoader.loadAvroSchema(AVRO_USER_SCHEMA_FILE);
        employeeAvroSchema = SchemaLoader.loadAvroSchema(AVRO_EMP_RECORD_SCHEMA_FILE_PATH);

        genericUserAvroRecord = RecordGenerator.createGenericAvroRecord();
        genericEmployeeAvroRecord = RecordGenerator.createGenericEmpRecord();

        userSchemaDefinition = AVROUtils.getInstance().getSchemaDefinition(genericUserAvroRecord);
        employeeSchemaDefinition = AVROUtils.getInstance().getSchemaDefinition(genericEmployeeAvroRecord);

        schemaDefinitionToSchemaVersionIdMap.put(userSchemaDefinition, USER_SCHEMA_VERSION_ID);
        schemaDefinitionToSchemaVersionIdMap.put(employeeSchemaDefinition, EMPLOYEE_SCHEMA_VERSION_ID);

        configs.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, "https://test");
        configs.put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
        configs.put(AWSSchemaRegistryConstants.SCHEMA_NAME, "User-Topic");
        configs.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);
        configs.put(AWSSchemaRegistryConstants.TAGS, testTags);
        configs.put(AWSSchemaRegistryConstants.DATA_FORMAT, DataFormat.AVRO.name());
    }

    @Test
    public void testConfigure_schemaName_schemaNameMatches() {
        AwsCredentialsProvider cred = mock(AwsCredentialsProvider.class);

        GlueSchemaRegistryKafkaSerializer glueSchemaRegistryKafkaSerializer = new GlueSchemaRegistryKafkaSerializer(cred, null);
        glueSchemaRegistryKafkaSerializer.configure(configs, true);
        assertEquals("User-Topic", glueSchemaRegistryKafkaSerializer.getSchemaName());
        assertNull(glueSchemaRegistryKafkaSerializer.getSchemaNamingStrategy());
    }

    @ParameterizedTest
    @EnumSource(value = DataFormat.class, mode = EnumSource.Mode.EXCLUDE, names = {"UNKNOWN_TO_SDK_VERSION"})
    public void testConfigure_schemaName_schemaNamingStrategyMatches(DataFormat dataFormat) {
        Map<String, Object> configs = new HashMap<>();

        configs.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, "https://test");
        configs.put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
        configs.put(AWSSchemaRegistryConstants.DATA_FORMAT, dataFormat.name());

        AwsCredentialsProvider cred = mock(AwsCredentialsProvider.class);

        GlueSchemaRegistryKafkaSerializer glueSchemaRegistryKafkaSerializer = new GlueSchemaRegistryKafkaSerializer(cred, null);
        glueSchemaRegistryKafkaSerializer.configure(configs, true);
        assertNotNull(glueSchemaRegistryKafkaSerializer.getSchemaNamingStrategy());
        assertEquals("com.amazonaws.services.schemaregistry.common.AWSSchemaNamingStrategyDefaultImpl",
                glueSchemaRegistryKafkaSerializer.getSchemaNamingStrategy().getClass().getName());
    }

    @ParameterizedTest
    @EnumSource(value = DataFormat.class, mode = EnumSource.Mode.EXCLUDE, names = {"UNKNOWN_TO_SDK_VERSION"})
    public void testConfigure_customerProvidedStrategy_schemaNamingStrategyMatches(DataFormat dataFormat) {
        Map<String, Object> configs = new HashMap<>();

        configs.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, "https://test");
        configs.put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
        configs.put(AWSSchemaRegistryConstants.SCHEMA_NAMING_GENERATION_CLASS,
                "com.amazonaws.services.schemaregistry.serializers.avro.CustomerProvidedSchemaNamingStrategy");
        configs.put(AWSSchemaRegistryConstants.DATA_FORMAT, dataFormat.name());

        AwsCredentialsProvider cred = mock(AwsCredentialsProvider.class);

        GlueSchemaRegistryKafkaSerializer glueSchemaRegistryKafkaSerializer = new GlueSchemaRegistryKafkaSerializer(cred, null);
        glueSchemaRegistryKafkaSerializer.configure(configs, true);
        assertNotNull(glueSchemaRegistryKafkaSerializer.getSchemaNamingStrategy());
        assertEquals("com.amazonaws.services.schemaregistry.serializers.avro.CustomerProvidedSchemaNamingStrategy",
                glueSchemaRegistryKafkaSerializer.getSchemaNamingStrategy().getClass().getName());
    }

    @ParameterizedTest
    @EnumSource(value = DataFormat.class, mode = EnumSource.Mode.EXCLUDE, names = {"UNKNOWN_TO_SDK_VERSION"})
    public void testConfigure_customerProvidedStrategy_throwsException(DataFormat dataFormat) {
        Map<String, Object> configs = new HashMap<>();

        configs.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, "https://test");
        configs.put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
        configs.put(AWSSchemaRegistryConstants.SCHEMA_NAMING_GENERATION_CLASS,
                "com.amazonaws.services.schemaregistry.serializers.avro.CustomerProvidedSchemaNamingStrategy1");
        configs.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);
        configs.put(AWSSchemaRegistryConstants.DATA_FORMAT, dataFormat.name());

        AwsCredentialsProvider cred = mock(AwsCredentialsProvider.class);
        GlueSchemaRegistryKafkaSerializer glueSchemaRegistryKafkaSerializer = new GlueSchemaRegistryKafkaSerializer(cred, null);
        AWSSchemaRegistryException awsSchemaRegistryException = Assertions.assertThrows(AWSSchemaRegistryException.class,
                () -> glueSchemaRegistryKafkaSerializer.configure(configs, true));

        String exceptedExceptionMessage = "Unable to locate the naming strategy class, check in the classpath for classname = "
                + configs.get(AWSSchemaRegistryConstants.SCHEMA_NAMING_GENERATION_CLASS);
        assertEquals(exceptedExceptionMessage, awsSchemaRegistryException.getMessage());
    }

    @Test
    public void testConfigure_nullConfigMapWithVersionId_throwsException() {
        AwsCredentialsProvider cred = mock(AwsCredentialsProvider.class);

        GlueSchemaRegistryKafkaSerializer glueSchemaRegistryKafkaSerializer = new GlueSchemaRegistryKafkaSerializer(cred, null);
        assertThrows(IllegalArgumentException.class, () ->  glueSchemaRegistryKafkaSerializer.configure((Map<String, ?>) null, true));
    }

    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION.class)
    public void testSerialize_customerProvidedStrategy_succeeds(AWSSchemaRegistryConstants.COMPRESSION compressionType) throws Exception {
        Map<String, Object> configs = new HashMap<>();

        configs.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, "https://test");
        configs.put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
        configs.put(AWSSchemaRegistryConstants.SCHEMA_NAMING_GENERATION_CLASS,
                "com.amazonaws.services.schemaregistry.serializers.avro.CustomerProvidedSchemaNamingStrategy");
        configs.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);
        configs.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, compressionType.name());
        configs.put(AWSSchemaRegistryConstants.DATA_FORMAT, DataFormat.AVRO.name());

        String fileName = "src/test/resources/avro/user3.avsc";
        Schema schema = getSchema(fileName);

        GenericData.EnumSymbol k = new GenericData.EnumSymbol(schema, "ONE");
        ArrayList<Integer> al = new ArrayList<>();
        al.add(1);

        GenericData.Record genericRecordWithAllTypes = new GenericData.Record(schema);
        Map<String, Long> map = new HashMap<>();
        map.put("test", 1L);

        genericRecordWithAllTypes.put("name", "Joe");
        genericRecordWithAllTypes.put("favorite_number", 1);
        genericRecordWithAllTypes.put("meta", map);
        genericRecordWithAllTypes.put("listOfColours", al);
        genericRecordWithAllTypes.put("integerEnum", k);

        String schemaDefinition = AVROUtils.getInstance().getSchemaDefinition(genericRecordWithAllTypes);
        GlueSchemaRegistryKafkaSerializer glueSchemaRegistryKafkaSerializer = initializeGSRKafkaSerializer(configs, schemaDefinition, mockClient, USER_SCHEMA_VERSION_ID);

        String schemaName =
                new CustomerProvidedSchemaNamingStrategy().getSchemaName("User-Topic", genericRecordWithAllTypes, true);

        when(mockClient.getORRegisterSchemaVersionId(eq(schemaDefinition), eq(schemaName),
                                                     eq(DataFormat.AVRO.name()), anyMap())).thenReturn(USER_SCHEMA_VERSION_ID);

        byte[] serialize = glueSchemaRegistryKafkaSerializer.serialize("User-Topic", genericRecordWithAllTypes);
        testForSerializedData(serialize, USER_SCHEMA_VERSION_ID, compressionType);
    }

    @Test
    public void testConstructor_defaultCredentialProvider_credentialProviderMatches() {
        GlueSchemaRegistryKafkaSerializer glueSchemaRegistryKafkaSerializer = new GlueSchemaRegistryKafkaSerializer();
        assertNull(glueSchemaRegistryKafkaSerializer.getSchemaVersionId());
        assertEquals("software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider",
                glueSchemaRegistryKafkaSerializer.getCredentialProvider().getClass().getName());

    }

    @Test
    public void testConstructor_nullCredentialProvider_succeeds() {
        assertDoesNotThrow(() -> new GlueSchemaRegistryKafkaSerializer(null, USER_SCHEMA_VERSION_ID, configs));
    }

    @Test
    public void testConstructor_configMap_succeeds() {
        assertDoesNotThrow(() ->  new GlueSchemaRegistryKafkaSerializer(configs));
        GlueSchemaRegistryKafkaSerializer glueSchemaRegistryKafkaSerializer = new GlueSchemaRegistryKafkaSerializer(configs);
        assertNotNull(glueSchemaRegistryKafkaSerializer);
    }

    @Test
    public void testConstructor_nullConfigMapWithVersionId_throwsException() {
        assertThrows(IllegalArgumentException.class, () -> new GlueSchemaRegistryKafkaSerializer((Map<String, ?>) null, USER_SCHEMA_VERSION_ID));
    }

    @Test
    public void testSerialize_nullData_returnsNull() {
        GlueSchemaRegistryKafkaSerializer glueSchemaRegistryKafkaSerializer = new GlueSchemaRegistryKafkaSerializer();
        assertNull(glueSchemaRegistryKafkaSerializer.serialize("test", null));
    }

    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION.class)
    public void testSerialize_customPojos_succeeds(AWSSchemaRegistryConstants.COMPRESSION compressionType) {
        configs.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, compressionType.name());

        GlueSchemaRegistryKafkaSerializer glueSchemaRegistryKafkaSerializer = initializeGSRKafkaSerializer(configs, userSchemaDefinition, mockClient, USER_SCHEMA_VERSION_ID);
        byte[] serialize = glueSchemaRegistryKafkaSerializer.serialize("test-topic", userDefinedPojo);

        testForSerializedData(serialize, USER_SCHEMA_VERSION_ID, compressionType);
    }

    @Test
    public void testSerialize_nullSchemaIdFromAvroSerializer_returnsNullByte() {
        AWSSerializerInput awsSerializerInput = AWSSerializerInput.builder()
                        .schemaDefinition(AVROUtils
                        .getInstance()
                        .getSchemaDefinition(genericUserAvroRecord))
                        .schemaName("User-Topic")
                        .build();

        GlueSchemaRegistryKafkaSerializer glueSchemaRegistryKafkaSerializer = new GlueSchemaRegistryKafkaSerializer(configs, null);
        GlueSchemaRegistrySerializationFacade mockGlueSchemaRegistrySerializationFacade =
                mock(GlueSchemaRegistrySerializationFacade.class);

        glueSchemaRegistryKafkaSerializer.setGlueSchemaRegistrySerializationFacade(mockGlueSchemaRegistrySerializationFacade);
        when(mockGlueSchemaRegistrySerializationFacade.getOrRegisterSchemaVersion(awsSerializerInput))
                .thenReturn(null);

        assertNull(glueSchemaRegistryKafkaSerializer.serialize("User-Topic", genericUserAvroRecord));
    }

    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION.class)
    public void testSerialize_parseSchema_succeeds(AWSSchemaRegistryConstants.COMPRESSION compressionType) {
        configs.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, compressionType.name());

        GlueSchemaRegistryKafkaSerializer glueSchemaRegistryKafkaSerializer = initializeGSRKafkaSerializer(configs, userSchemaDefinition, mockClient, USER_SCHEMA_VERSION_ID);
        byte[] serialize = glueSchemaRegistryKafkaSerializer.serialize("test-topic", genericUserAvroRecord);
        testForSerializedData(serialize, USER_SCHEMA_VERSION_ID, compressionType);
    }

    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION.class)
    public void testSerialize_multipleRecords_succeeds(AWSSchemaRegistryConstants.COMPRESSION compressionType) {
        configs.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, compressionType.name());

        GlueSchemaRegistryKafkaSerializer glueSchemaRegistryKafkaSerializer = initializeGSRKafkaSerializer(configs, mockClient, schemaDefinitionToSchemaVersionIdMap);
        byte[] userSerializedData = glueSchemaRegistryKafkaSerializer.serialize("test-topic", genericUserAvroRecord);
        testForSerializedData(userSerializedData, USER_SCHEMA_VERSION_ID, compressionType);

        byte[] employeeSerializedData = glueSchemaRegistryKafkaSerializer.serialize("test-topic", genericEmployeeAvroRecord);
        testForSerializedData(employeeSerializedData, EMPLOYEE_SCHEMA_VERSION_ID, compressionType);
    }

    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION.class)
    public void testSerialize_preProvidedSchemaVersionId_succeeds(AWSSchemaRegistryConstants.COMPRESSION compressionType) {
        configs.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, compressionType.name());
        GlueSchemaRegistryKafkaSerializer glueSchemaRegistryKafkaSerializer = initializeGSRKafkaSerializer(configs, USER_SCHEMA_VERSION_ID);
        byte[] serializedData = glueSchemaRegistryKafkaSerializer.serialize("test-topic", genericUserAvroRecord);
        testForSerializedData(serializedData, USER_SCHEMA_VERSION_ID, compressionType);
    }

    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION.class)
    public void testSerialize_preProvidedSchemaVersionIdWithAnyRecord_succeeds(AWSSchemaRegistryConstants.COMPRESSION compressionType) {
        configs.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, compressionType.name());
        GlueSchemaRegistryKafkaSerializer glueSchemaRegistryKafkaSerializer = initializeGSRKafkaSerializer(configs, USER_SCHEMA_VERSION_ID);
        byte[] serializedUserData = glueSchemaRegistryKafkaSerializer.serialize("test-topic", genericEmployeeAvroRecord);
        testForSerializedData(serializedUserData, USER_SCHEMA_VERSION_ID, compressionType);

        // This is the validation of a case where pre-provided schemaVersionId is honored and any record will be serialized
        // with pre-provided schemaVersionId - a call to schema registry is not made by serializer
        // So - this will certainly fail while deserialization
        byte[] employeeSerializedData = glueSchemaRegistryKafkaSerializer.serialize("test-topic", genericEmployeeAvroRecord);
        testForSerializedData(employeeSerializedData, USER_SCHEMA_VERSION_ID, compressionType);
    }

    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION.class)
    public void testSerialize_sendMultipleMsgs_throwsExceptionAndSchemaVersionIdStateNotSaved(AWSSchemaRegistryConstants.COMPRESSION compressionType)
            throws Exception {
        configs.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, compressionType.name());

        String fileName = "src/test/resources/avro/user_array_String.avsc";
        Schema schema = getSchema(fileName);

        GenericData.Array<String> array1 = new GenericData.Array<>(1, schema);
        array1.add("1");
        GenericData.Array<String> array2 = new GenericData.Array<>(1, schema);
        array1.add("2");

        String schemaDefinition = AVROUtils.getInstance().getSchemaDefinition(array1);
        GlueSchemaRegistryKafkaSerializer glueSchemaRegistryKafkaSerializer = initializeGSRKafkaSerializer(configs, schemaDefinition, mockClient,
                                                                                                               null);
        EntityNotFoundException.Builder builder = EntityNotFoundException.builder().message(AWSSchemaRegistryConstants.SCHEMA_VERSION_NOT_FOUND_MSG);
        EntityNotFoundException entityNotFoundException = builder.build();
        AWSSchemaRegistryException awsSchemaRegistryException = new AWSSchemaRegistryException(entityNotFoundException);
        when(mockClient.getORRegisterSchemaVersionId(eq(schemaDefinition), eq("User-Topic"), eq(DataFormat.AVRO.name()), anyMap()))
                .thenThrow(awsSchemaRegistryException);

        assertThrows(AWSSchemaRegistryException.class, () -> glueSchemaRegistryKafkaSerializer.serialize("test-topic", array1));
        assertThrows(AWSSchemaRegistryException.class, () -> glueSchemaRegistryKafkaSerializer.serialize("test-topic", array2));
        assertNull(glueSchemaRegistryKafkaSerializer.getSchemaVersionId());
    }

    @Test
    public void testPrepareInput_nullDefinitionData_throwsException() throws NoSuchMethodException {
        GlueSchemaRegistryKafkaSerializer glueSchemaRegistryKafkaSerializer = new GlueSchemaRegistryKafkaSerializer();
        Method method = GlueSchemaRegistryKafkaSerializer.class.getDeclaredMethod("prepareInput", Object.class,
                                                                                  String.class, Boolean.class);
        method.setAccessible(true);
        try {
            method.invoke(glueSchemaRegistryKafkaSerializer,  null, "User-Topic", true);
        } catch(Exception e) {
            assertEquals(IllegalArgumentException.class, e.getCause().getClass());
        }
    }
}