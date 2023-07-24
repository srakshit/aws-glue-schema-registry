package com.amazonaws.services.crossregion.schemaregistry.kafkaconnect;

import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializerImpl;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializerImpl;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)

public class CrossRegionReplicationSchemaRegistryMm2ConverterTest {
    private CrossRegionReplicationSchemaRegistryMm2Converter converter;
    @Mock
    private GlueSchemaRegistrySerializerImpl serializer;
    @Mock
    private GlueSchemaRegistryDeserializerImpl deserializer;

    private static final String TRANSPORT_NAME = "stream-foo";
    private static final String TEST_TOPIC = "test-topic";
    private final static byte[] USER_DATA = new byte[] { 12, 83, 82 };
    private final static byte[] ENCODED_DATA = new byte[] { 8, 9, 12, 83, 82 };
    private static final byte[] genericBytes = new byte[] {3, 0, -73, -76, -89, -16, -100, -106, 78, 74, -90, -121, -5,
            93, -23, -17, 12, 99, 10, 115, 97, 110, 115, 97, -58, 1, 6, 114, 101, 100};
    private static final com.amazonaws.services.schemaregistry.common.Schema SCHEMA_REGISTRY_SCHEMA =
            new com.amazonaws.services.schemaregistry.common.Schema("{}", "AVRO", "schemaFoo");

    @BeforeEach
    void setUp() {
        converter = new CrossRegionReplicationSchemaRegistryMm2Converter(serializer, deserializer);
    }


    /**
     * Test for Mm2Converter config method.
     */
    @Test
    public void testConverter_configure() {
        converter = new CrossRegionReplicationSchemaRegistryMm2Converter();
        converter.configure(getProperties(), false);
        assertNotNull(converter);
        assertNotNull(converter.getSerializer());
        assertNotNull(converter.getDeserializer());
        assertNotNull(converter.isKey());
    }

    /**
     * Test Mm2Converter when serializer throws exception.
     */
    @Test
    public void testConverter_fromConnectData_succeeds() {
        Struct expected = createStructRecord();
        doReturn(USER_DATA)
                .when(deserializer).getData(genericBytes);
        doReturn(SCHEMA_REGISTRY_SCHEMA)
                .when(deserializer).getSchema(genericBytes);
        doReturn(ENCODED_DATA)
                .when(serializer).encode(TRANSPORT_NAME, SCHEMA_REGISTRY_SCHEMA, USER_DATA);

        assertEquals(Arrays.toString((converter.fromConnectData(TRANSPORT_NAME, expected.schema(), genericBytes))), Arrays.toString(ENCODED_DATA));
    }

    /**
     * Test Mm2Converter when it returns byte 0 given the input value is null.
     */
    @Test
    public void testConverter_fromConnectData_returnsByte0() {
        Struct expected = createStructRecord();
        assertEquals(Arrays.toString(converter.fromConnectData(TRANSPORT_NAME, expected.schema(), null)), Arrays.toString(new byte[0]));
    }

    /**
     * Test Mm2Converter when serializer throws exception.
     */
    @Test
    public void testConverter_fromConnectData_serializer_ThrowsException() {
        Struct expected = createStructRecord();
//        doReturn(USER_DATA)
//                .when(remoteDeserializer).getData(genericBytes);
//        doReturn(SCHEMA_REGISTRY_SCHEMA)
//                .when(remoteDeserializer).getSchema(genericBytes);
        doReturn(USER_DATA)
                .when(deserializer).getData(genericBytes);
        doReturn(SCHEMA_REGISTRY_SCHEMA)
                .when(deserializer).getSchema(genericBytes);
        when(serializer.encode(TRANSPORT_NAME, SCHEMA_REGISTRY_SCHEMA, USER_DATA)).thenThrow(new AWSSchemaRegistryException());
        assertThrows(DataException.class, () -> converter.fromConnectData(TRANSPORT_NAME, expected.schema(), genericBytes));
    }

    /**
     * Test Mm2Converter when the deserializer throws exception.
     */
    @Test
    public void testConverter_fromConnectData_deserializer_getData_ThrowsException() {
        Struct expected = createStructRecord();
        when((deserializer).getData(genericBytes)).thenThrow(new AWSSchemaRegistryException());
        doReturn(SCHEMA_REGISTRY_SCHEMA)
                .when(deserializer).getSchema(genericBytes);
        doReturn(ENCODED_DATA)
                .when(serializer).encode(TRANSPORT_NAME, SCHEMA_REGISTRY_SCHEMA, USER_DATA);
        assertThrows(DataException.class, () -> converter.fromConnectData(TRANSPORT_NAME, expected.schema(), genericBytes));
    }

    /**
     * Test Mm2Converter when toConnectData outputs SchemaAndValue.NULL.
     */
    @Test
    public void testConverter_toConnectData(){
        assertEquals(converter.toConnectData(TEST_TOPIC, ENCODED_DATA), SchemaAndValue.NULL);
    }


    /**
     * To create a map of configurations.
     *
     * @return a map of configurations
     */
    private Map<String, Object> getProperties() {
        Map<String, Object> props = new HashMap<>();

        props.put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
        props.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, "https://test");
        props.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);
        props.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, AvroRecordType.GENERIC_RECORD.getName());

        return props;
    }

    /**
     * To create a Connect Struct record.
     *
     * @return a Connect Struct
     */
    private Struct createStructRecord() {
        Schema schema = SchemaBuilder.struct()
                .build();
        return new Struct(schema);
    }
}
