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

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.ValidString;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

public abstract class HeaderFrom<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC = "Moves or copies moves or copies fields in the key/value on a record into that record's headers.";

    private static final String FIELDS_CONFIG = "fields";
    private static final String FIELDS_DOC = "A comma-separated list of field names whose values are to be copied/moved to headers.";
    private static final String HEADERS_CONFIG = "headers";
    private static final String HEADERS_DOC = "A comma-separated list of header names, in the same order as the field names listed in the fields configuration property";
    private static final String OPERATION_CONFIG = "operation";
    private static final String OPERATION_DOC = "Either <code>move>/code> if the fields are to be moved, or <code>copy</code> if the fields are to just be copied and left on the record.";
    private static final String OPERATION_COPY = "copy";
    private static final String OPERATION_MOVE = "move";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELDS_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, FIELDS_DOC)
            .define(HEADERS_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, HEADERS_DOC)
            .define(OPERATION_CONFIG, ConfigDef.Type.STRING, OPERATION_COPY, ValidString.in(OPERATION_COPY, OPERATION_MOVE), ConfigDef.Importance.MEDIUM, OPERATION_DOC);

    private static final String PURPOSE = "HeaderFrom";

    private String operation;
    private Map<String, String> fieldsToHeaders = new HashMap<>();

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        List<String> fieldNames = config.getList(FIELDS_CONFIG);
        List<String> headerNames = config.getList(HEADERS_CONFIG);

        if (fieldNames.size() != headerNames.size()) {
            throw new ConfigException("HeaderFrom requires the same number of fields and headers");
        }

        operation = config.getString(OPERATION_CONFIG);
        fieldsToHeaders = new HashMap<>();
        for (int i = 0; i < fieldNames.size(); i++) {
            fieldsToHeaders.put(fieldNames.get(i), headerNames.get(i));
        }
    }

    @Override
    public R apply(R record) {
        if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);
        final Struct updatedValue = new Struct(value.schema());
        final SchemaBuilder schemaBuilder = new SchemaBuilder(value.schema().type());
        final Map<String, SchemaAndValue> newHeaders = new HashMap<>();
        for (Field field : value.schema().fields()) {
            String fieldName = field.name();
            if (fieldsToHeaders.containsKey(fieldName)) {
                newHeaders.put(fieldsToHeaders.get(fieldName), new SchemaAndValue(field.schema(), value.get(fieldName)));
                if (OPERATION_MOVE.equals(operation)) continue;
            }
            schemaBuilder.field(fieldName, field.schema());
            updatedValue.put(field, value.get(fieldName));
        }
        return newRecord(record, schemaBuilder.build(), updatedValue, newHeaders);
    }

    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);
        final Map<String, Object> updatedValue = new HashMap<>(value);
        final Map<String, SchemaAndValue> newHeaders = new HashMap<>();
        for (Map.Entry<String, String> entry : fieldsToHeaders.entrySet()) {
            String fieldName = entry.getKey();
            String headerName = entry.getValue();
            newHeaders.put(headerName, new SchemaAndValue(Schema.STRING_SCHEMA, String.valueOf(value.get(fieldName))));
            if (OPERATION_MOVE.equals(operation)) {
                updatedValue.remove(fieldName);
            }
        }
        return newRecord(record, null, updatedValue, newHeaders);
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);
    
    protected abstract R newRecord(R record, Schema schema, Object oj, Map<String, SchemaAndValue> newHeaders);

    @Override
    public void close() {
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    public static final class Key<R extends ConnectRecord<R>> extends HeaderFrom<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedKeySchema, Object updatedKey, Map<String, SchemaAndValue> newHeaders) {
            Headers headers = toHeaders(record.headers(), newHeaders);
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedKeySchema, updatedKey, record.valueSchema(), record.value(), record.timestamp(), headers);
        }
    }

    public static final class Value<R extends ConnectRecord<R>> extends HeaderFrom<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedValueSchema, Object updatedValue, Map<String, SchemaAndValue> newHeaders) {
            Headers headers = toHeaders(record.headers(), newHeaders);
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedValueSchema, updatedValue, record.timestamp(), headers);
        }
    }

    private static Headers toHeaders(Headers headers, Map<String, SchemaAndValue> newHeaders) {
        Headers allHeaders = headers.duplicate();
        for (Map.Entry<String, SchemaAndValue> entry : newHeaders.entrySet()) {
            allHeaders.add(entry.getKey(), entry.getValue());
        }
        return allHeaders;
    }

}
