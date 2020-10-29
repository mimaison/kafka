/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.transforms;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

public class HeaderFromTest {

    private static final List<String> FIELDS = Arrays.asList("f1", "f2");
    private static final List<String> HEADERS = Arrays.asList("h1", "h2");
    private static final Map<String, String> HEADERS_TO_FIELDS = new HashMap<>();
    static {
        HEADERS_TO_FIELDS.put("h1", "f1");
        HEADERS_TO_FIELDS.put("h2", "f2");
    }

    private static HeaderFrom<SinkRecord> valueTransform(List<String> fields, List<String> headers, String operation) {
       return transform(new HeaderFrom.Value<>(), fields, headers, operation);
    }

    private static HeaderFrom<SinkRecord> keyTransform(List<String> fields, List<String> headers, String operation) {
        return transform(new HeaderFrom.Key<>(), fields, headers, operation);
    }

    private static HeaderFrom<SinkRecord> transform(HeaderFrom<SinkRecord> transform, List<String> fields, List<String> headers, String operation) {
        Map<String, Object> props = new HashMap<>();
        props.put("fields", fields);
        props.put("headers", headers);
        props.put("operation", operation);
        transform.configure(props);
        return transform;
    }

    private static SinkRecord recordValue(Schema schema, Object value, Headers headers) {
        return new SinkRecord("", 0, null, null, schema, value, 0, null, TimestampType.NO_TIMESTAMP_TYPE, headers);
    }

    private static SinkRecord recordKey(Schema schema, Object key, Headers headers) {
        return new SinkRecord("", 0, schema, key, null, null, 0, null, TimestampType.NO_TIMESTAMP_TYPE, headers);
    }

    @SuppressWarnings("resource")
    @Test
    public void testConfigure() {
        Map<String, Object> props = new HashMap<>();
        HeaderFrom<SinkRecord> transform = new HeaderFrom.Value<>();
        try {
            transform.configure(props);
            fail("Should have thrown");
        } catch (ConfigException ce) {}
        props.put("fields", Arrays.asList("f1"));
        try {
            transform.configure(props);
            fail("Should have thrown");
        } catch (ConfigException ce) {}
        props.put("operation", "test");
        try {
            transform.configure(props);
            fail("Should have thrown");
        } catch (ConfigException ce) {}
        props.put("operation", "move");
        props.put("headers", Arrays.asList("h1", "h2"));
        try {
            transform.configure(props);
            fail("Should have thrown");
        } catch (ConfigException ce) {}
        props.put("headers", Arrays.asList("h1"));
        transform.configure(props);
    }

    @Test
    public void testSchemaLessValueCopy() {
        Map<String, Object> value = new HashMap<>();
        value.put("f1", 42);
        value.put("f2", true);
        value.put("f3", "test");
        HeaderFrom<SinkRecord> transform = valueTransform(FIELDS, HEADERS, "copy");
        SinkRecord record = recordValue(null, value, null);
        SinkRecord transformed = transform.apply(record);

        assertEquals(2, transformed.headers().size());
        for (Header header : transformed.headers()) {
            String fieldName = HEADERS_TO_FIELDS.get(header.key());
            assertEquals(String.valueOf(value.get(fieldName)), header.value());
            assertEquals(Schema.STRING_SCHEMA, header.schema());
        }
        assertEquals(value, transformed.value());
    }

    @Test
    public void testSchemaLessValueMove() {
        Map<String, Object> value = new HashMap<>();
        value.put("f1", 42);
        value.put("f2", true);
        value.put("f3", "test");
        HeaderFrom<SinkRecord> transform = valueTransform(FIELDS, HEADERS, "move");
        SinkRecord record = recordValue(null, value, null);
        SinkRecord transformed = transform.apply(record);

        assertEquals(2, transformed.headers().size());
        for (Header header : transformed.headers()) {
            String fieldName = HEADERS_TO_FIELDS.get(header.key());
            assertEquals(String.valueOf(value.get(fieldName)), header.value());
            assertEquals(Schema.STRING_SCHEMA, header.schema());
        }
        Map<String, Object> expectedValue = new HashMap<>(value);
        expectedValue.remove("f1");
        expectedValue.remove("f2");
        assertEquals(expectedValue, transformed.value());
    }

    @Test
    public void testSchemaLessKeyCopy() {
        Map<String, Object> key = new HashMap<>();
        key.put("f1", 42);
        key.put("f2", true);
        key.put("f3", "test");
        HeaderFrom<SinkRecord> transform = keyTransform(FIELDS, HEADERS, "copy");
        SinkRecord record = recordKey(null, key, null);
        SinkRecord transformed = transform.apply(record);

        assertEquals(2, transformed.headers().size());
        for (Header header : transformed.headers()) {
            String fieldName = HEADERS_TO_FIELDS.get(header.key());
            assertEquals(String.valueOf(key.get(fieldName)), header.value());
            assertEquals(Schema.STRING_SCHEMA, header.schema());
        }
        assertEquals(key, transformed.key());
    }

    @Test
    public void testSchemaLessKeyMove() {
        Map<String, Object> key = new HashMap<>();
        key.put("f1", 42);
        key.put("f2", true);
        key.put("f3", "test");
        HeaderFrom<SinkRecord> transform = keyTransform(FIELDS, HEADERS, "move");
        SinkRecord record = recordKey(null, key, null);
        SinkRecord transformed = transform.apply(record);

        assertEquals(2, transformed.headers().size());
        for (Header header : transformed.headers()) {
            String fieldName = HEADERS_TO_FIELDS.get(header.key());
            assertEquals(String.valueOf(key.get(fieldName)), header.value());
            assertEquals(Schema.STRING_SCHEMA, header.schema());
        }
        Map<String, Object> expectedKey = new HashMap<>(key);
        expectedKey.remove("f1");
        expectedKey.remove("f2");
        assertEquals(expectedKey, transformed.key());
    }

    @Test
    public void testSchemaValueCopy() {
        final Schema schema = SchemaBuilder.struct()
                .field("f1", Schema.STRING_SCHEMA)
                .field("f2", Schema.INT32_SCHEMA)
                .field("f3", Schema.BOOLEAN_SCHEMA)
                .build();
        final Struct value = new Struct(schema);
        value.put("f1", "whatever");
        value.put("f2", 42);
        value.put("f3", true);
        HeaderFrom<SinkRecord> transform = valueTransform(FIELDS, HEADERS, "copy");
        SinkRecord record = recordValue(schema, value, null);
        SinkRecord transformed = transform.apply(record);

        assertEquals(2, transformed.headers().size());
        for (Header header : transformed.headers()) {
            String fieldName = HEADERS_TO_FIELDS.get(header.key());
            assertEquals(value.get(fieldName), header.value());
            assertEquals(schema.field(fieldName).schema(), header.schema());
        }
        assertEquals(value, transformed.value());
        assertEquals(schema, transformed.valueSchema());
    }

    @Test
    public void testSchemaValueMove() {
        final Schema schema = SchemaBuilder.struct()
                .field("f1", Schema.STRING_SCHEMA)
                .field("f2", Schema.INT32_SCHEMA)
                .field("f3", Schema.BOOLEAN_SCHEMA)
                .build();
        final Struct value = new Struct(schema);
        value.put("f1", "whatever");
        value.put("f2", 42);
        value.put("f3", true);
        HeaderFrom<SinkRecord> transform = valueTransform(FIELDS, HEADERS, "move");
        SinkRecord record = recordValue(schema, value, null);
        SinkRecord transformed = transform.apply(record);

        assertEquals(2, transformed.headers().size());
        for (Header header : transformed.headers()) {
            String fieldName = HEADERS_TO_FIELDS.get(header.key());
            assertEquals(value.get(fieldName), header.value());
            assertEquals(schema.field(fieldName).schema(), header.schema());
        }
        final Struct expectedValue = new Struct(schema);
        expectedValue.put("f3", true);
        assertEquals(expectedValue, transformed.value());
        final Schema expectedSchema = SchemaBuilder.struct()
                .field("f3", Schema.BOOLEAN_SCHEMA)
                .build();
        assertEquals(expectedSchema, transformed.valueSchema());
    }

    @Test
    public void testSchemaKeyCopy() {
        final Schema schema = SchemaBuilder.struct()
                .field("f1", Schema.STRING_SCHEMA)
                .field("f2", Schema.INT32_SCHEMA)
                .field("f3", Schema.BOOLEAN_SCHEMA)
                .build();
        final Struct key = new Struct(schema);
        key.put("f1", "whatever");
        key.put("f2", 42);
        key.put("f3", true);
        HeaderFrom<SinkRecord> transform = keyTransform(FIELDS, HEADERS, "copy");
        SinkRecord record = recordKey(schema, key, null);
        SinkRecord transformed = transform.apply(record);

        assertEquals(2, transformed.headers().size());
        for (Header header : transformed.headers()) {
            String fieldName = HEADERS_TO_FIELDS.get(header.key());
            assertEquals(key.get(fieldName), header.value());
            assertEquals(schema.field(fieldName).schema(), header.schema());
        }
        assertEquals(key, transformed.key());
        assertEquals(schema, transformed.keySchema());
    }

    @Test
    public void testSchemaKeyMove() {
        final Schema schema = SchemaBuilder.struct()
                .field("f1", Schema.STRING_SCHEMA)
                .field("f2", Schema.INT32_SCHEMA)
                .field("f3", Schema.BOOLEAN_SCHEMA)
                .build();
        final Struct key = new Struct(schema);
        key.put("f1", "whatever");
        key.put("f2", 42);
        key.put("f3", true);
        HeaderFrom<SinkRecord> transform = keyTransform(FIELDS, HEADERS, "move");
        SinkRecord record = recordKey(schema, key, null);
        SinkRecord transformed = transform.apply(record);

        assertEquals(2, transformed.headers().size());
        for (Header header : transformed.headers()) {
            String fieldName = HEADERS_TO_FIELDS.get(header.key());
            assertEquals(key.get(fieldName), header.value());
            assertEquals(schema.field(fieldName).schema(), header.schema());
        }
        final Struct expectedKey = new Struct(schema);
        expectedKey.put("f3", true);
        assertEquals(expectedKey, transformed.key());
        final Schema expectedSchema = SchemaBuilder.struct()
                .field("f3", Schema.BOOLEAN_SCHEMA)
                .build();
        assertEquals(expectedSchema, transformed.keySchema());
    }
}
