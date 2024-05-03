import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.ExtractField;
import org.apache.kafka.connect.transforms.ValueToKey;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class ExtractFieldJmsTest {

    public static final Schema PROPERTY_VALUE_SCHEMA = SchemaBuilder.struct()
            .name("io.confluent.connect.jms.PropertyValue")
            .field(
                    "propertyType",
                    SchemaBuilder.string().build()
            ).field(
                    "boolean",
                    SchemaBuilder.bool().optional().build()
            ).field(
                    "byte",
                    SchemaBuilder.int8().optional().build()
            ).field(
                    "short",
                    SchemaBuilder.int16().optional().build()
            ).field(
                    "integer",
                    SchemaBuilder.int32().optional().build()
            ).field(
                    "long",
                    SchemaBuilder.int64().optional().build()
            ).field(
                    "float",
                    SchemaBuilder.float32().optional().build()
            ).field(
                    "double",
                    SchemaBuilder.float64().optional().build()
            ).field(
                    "string",
                    SchemaBuilder.string().optional().build()
            ).field(
                    "bytes",
                    SchemaBuilder.bytes().optional().build()
            ).build();

    public static final Schema PROPERTIES_SCHEMA = SchemaBuilder.map(Schema.STRING_SCHEMA, PROPERTY_VALUE_SCHEMA).build();

    public static final Schema VALUE_SCHEMA = SchemaBuilder.struct()
            .name("io.confluent.connect.jms.Value")
            .field(
                    "messageID",
                    SchemaBuilder.string().build()
            ).field(
                    "properties",
                    PROPERTIES_SCHEMA
            ).build();

    public static final Struct myValue = new Struct(VALUE_SCHEMA)
            .put("messageID", "someId")
            .put("properties", Map.of(
                    "myProperty", new Struct(PROPERTY_VALUE_SCHEMA)
                            .put("propertyType", "string")
                            .put("string", "myValue")));


    public static final SourceRecord sourceRecord = new SourceRecord(null, null, "test", 0, VALUE_SCHEMA, myValue);


    @Test
    public void testFailure() {
        ValueToKey<SourceRecord> valueToKeySmt = new ValueToKey<>();
        valueToKeySmt.configure(Map.of("fields", "properties"));

        ExtractField.Key<SourceRecord> extractFieldSmtProperties = new ExtractField.Key<>();
        extractFieldSmtProperties.configure(Map.of("field", "properties"));

        ExtractField.Key<SourceRecord> extractFieldSmtMyProperty = new ExtractField.Key<>();
        extractFieldSmtMyProperty.configure(Map.of("field", "myProperty"));

        try {
            SourceRecord transformedRecord;
            transformedRecord = valueToKeySmt.apply(sourceRecord);
            transformedRecord = extractFieldSmtProperties.apply(transformedRecord);
            transformedRecord = extractFieldSmtMyProperty.apply(transformedRecord);
        } catch (DataException e) {
            assertThat(e.getMessage()).isEqualTo("Only Struct objects supported for [field extraction], found: java.util.ImmutableCollections$Map1");
        }
    }

    @Test
    public void testSuccess() {
        JsonFieldToKey<SourceRecord> smt = new JsonFieldToKey<>();
        smt.configure(Map.of("field", "$[\"properties\"][\"myProperty\"][\"string\"]"));
        SourceRecord transformedRecord = smt.apply(sourceRecord);
        assertThat(transformedRecord.key()).isEqualTo("myValue");


    }
}