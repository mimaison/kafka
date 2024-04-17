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
package org.apache.kafka.connect.data;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.text.CharacterIterator;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.text.StringCharacterIterator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

public class Parser {

    private static final Logger LOG = LoggerFactory.getLogger(Parser.class);

    private static final BigInteger LONG_MIN = BigInteger.valueOf(Long.MIN_VALUE);
    private static final BigInteger LONG_MAX = BigInteger.valueOf(Long.MAX_VALUE);
    private static final String TRUE_LITERAL = Boolean.TRUE.toString();
    private static final String FALSE_LITERAL = Boolean.FALSE.toString();
    private static final SchemaAndValue TRUE_SCHEMA_AND_VALUE = new SchemaAndValue(Schema.BOOLEAN_SCHEMA, Boolean.TRUE);
    private static final SchemaAndValue FALSE_SCHEMA_AND_VALUE = new SchemaAndValue(Schema.BOOLEAN_SCHEMA, Boolean.FALSE);

    private static final String QUOTE_DELIMITER = "\"";
    private static final String COMMA_DELIMITER = ",";
    private static final String ENTRY_DELIMITER = ":";
    private static final String ARRAY_BEGIN_DELIMITER = "[";
    private static final String ARRAY_END_DELIMITER = "]";
    private static final String MAP_BEGIN_DELIMITER = "{";
    private static final String MAP_END_DELIMITER = "}";
    private static final int ISO_8601_DATE_LENGTH = Values.ISO_8601_DATE_FORMAT_PATTERN.length();
    private static final int ISO_8601_TIME_LENGTH = Values.ISO_8601_TIME_FORMAT_PATTERN.length() - 2; // subtract single quotes
    private static final int ISO_8601_TIMESTAMP_LENGTH = Values.ISO_8601_TIMESTAMP_FORMAT_PATTERN.length() - 4; // subtract single quotes

    private static final Set<String> TEMPORAL_LOGICAL_TYPE_NAMES =
            Collections.unmodifiableSet(
                    new HashSet<>(
                            Arrays.asList(Time.LOGICAL_NAME,
                                    Timestamp.LOGICAL_NAME,
                                    Date.LOGICAL_NAME
                            )
                    )
            );

    private static final SchemaAndValue NULL_SCHEMA_AND_VALUE = new SchemaAndValue(null, null);

    private final String original;
    private final CharacterIterator iter;
    private String nextToken = null;
    private String previousToken = null;

    public Parser(String original) {
        this.original = original;
        this.iter = new StringCharacterIterator(this.original);
    }

    private SchemaAndValue parseAsExactDecimal(BigDecimal decimal) {
        BigDecimal ceil = decimal.setScale(0, RoundingMode.CEILING);
        BigDecimal floor = decimal.setScale(0, RoundingMode.FLOOR);
        if (ceil.equals(floor)) {
            BigInteger num = ceil.toBigIntegerExact();
            if (ceil.precision() >= 19 && (num.compareTo(LONG_MIN) < 0 || num.compareTo(LONG_MAX) > 0)) {
                return null;
            }
            long integral = num.longValue();
            byte int8 = (byte) integral;
            short int16 = (short) integral;
            int int32 = (int) integral;
            if (int8 == integral) {
                return new SchemaAndValue(Schema.INT8_SCHEMA, int8);
            } else if (int16 == integral) {
                return new SchemaAndValue(Schema.INT16_SCHEMA, int16);
            } else if (int32 == integral) {
                return new SchemaAndValue(Schema.INT32_SCHEMA, int32);
            } else {
                return new SchemaAndValue(Schema.INT64_SCHEMA, integral);
            }
        }
        return null;
    }

    private SchemaAndValue parseAsTemporal(String token) {
        if (token == null) {
            return null;
        }
        // If the colons were escaped, we'll see the escape chars and need to remove them
        token = token.replace("\\:", ":");
        int tokenLength = token.length();
        if (tokenLength == ISO_8601_TIME_LENGTH) {
            return parseAsTemporalType(token, Time.SCHEMA, Values.ISO_8601_TIME_FORMAT_PATTERN);
        } else if (tokenLength == ISO_8601_TIMESTAMP_LENGTH) {
            return parseAsTemporalType(token, Timestamp.SCHEMA, Values.ISO_8601_TIMESTAMP_FORMAT_PATTERN);
        } else if (tokenLength == ISO_8601_DATE_LENGTH) {
            return parseAsTemporalType(token, Date.SCHEMA, Values.ISO_8601_DATE_FORMAT_PATTERN);
        } else {
            return null;
        }
    }

    private SchemaAndValue parseMap() {
        Map<Object, Object> result = new LinkedHashMap<>();
        SchemaMerger keySchema = new SchemaMerger();
        SchemaMerger valueSchema = new SchemaMerger();
        while (hasNext()) {
            if (canConsume(MAP_END_DELIMITER)) {
                Schema mapSchema;
                if (keySchema.hasCommonSchema() && valueSchema.hasCommonSchema()) {
                    mapSchema = SchemaBuilder.map(keySchema.schema(), valueSchema.schema()).build();
                    result = alignMapKeysAndValuesWithSchema(mapSchema, result);
                } else if (keySchema.hasCommonSchema()) {
                    mapSchema = SchemaBuilder.mapWithNullValues(keySchema.schema());
                    result = alignMapKeysWithSchema(mapSchema, result);
                } else {
                    mapSchema = SchemaBuilder.mapOfNull().build();
                }
                return new SchemaAndValue(mapSchema, result);
            }

            if (canConsume(COMMA_DELIMITER)) {
                throw new DataException("Unable to parse a map entry with no key or value: " + original());
            }
            SchemaAndValue key = parse(true);
            if (key == null || key.value() == null) {
                throw new DataException("Map entry may not have a null key: " + original());
            } else if (!canConsume(ENTRY_DELIMITER)) {
                throw new DataException("Map entry is missing '" + ENTRY_DELIMITER
                        + "' at " + position()
                        + " in " + original());
            }
            SchemaAndValue value = parse(true);
            Object entryValue = value != null ? value.value() : null;
            result.put(key.value(), entryValue);

            canConsume(COMMA_DELIMITER);
            keySchema.merge(key);
            valueSchema.merge(value);
        }
        // Missing either a comma or an end delimiter
        if (COMMA_DELIMITER.equals(previous())) {
            throw new DataException("Map is missing element after ',': " + original());
        }
        throw new DataException("Map is missing terminating '}': " + original());
    }

    private SchemaAndValue parseNextToken(boolean embedded, String token) {
        char firstChar = token.charAt(0);
        boolean firstCharIsDigit = Character.isDigit(firstChar);

        // Temporal types are more restrictive, so try them first
        if (firstCharIsDigit) {
            SchemaAndValue temporal = parseMultipleTokensAsTemporal(token);
            if (temporal != null) {
                return temporal;
            }
        }
        if (firstCharIsDigit || firstChar == '+' || firstChar == '-') {
            try {
                return parseAsNumber(token);
            } catch (NumberFormatException e) {
                // can't parse as a number
            }
        }
        if (embedded) {
            throw new DataException("Failed to parse embedded value");
        }
        // At this point, the only thing this non-embedded value can be is a string.
        return new SchemaAndValue(Schema.STRING_SCHEMA, original());
    }

    private SchemaAndValue parseQuotedString() {
        StringBuilder sb = new StringBuilder();
        while (hasNext()) {
            if (canConsume(QUOTE_DELIMITER)) {
                break;
            }
            sb.append(next());
        }
        String content = sb.toString();
        // We can parse string literals as temporal logical types, but all others
        // are treated as strings
        SchemaAndValue parsed = parseString(content);
        if (parsed != null && TEMPORAL_LOGICAL_TYPE_NAMES.contains(parsed.schema().name())) {
            return parsed;
        }
        return new SchemaAndValue(Schema.STRING_SCHEMA, content);
    }

    private SchemaAndValue parseAsNumber(String token) {
        // Try to parse as a number ...
        BigDecimal decimal = new BigDecimal(token);
        SchemaAndValue exactDecimal = parseAsExactDecimal(decimal);
        float fValue = decimal.floatValue();
        double dValue = decimal.doubleValue();
        if (exactDecimal != null) {
            return exactDecimal;
        } else if (fValue != Float.NEGATIVE_INFINITY && fValue != Float.POSITIVE_INFINITY
                && decimal.scale() != 0) {
            return new SchemaAndValue(Schema.FLOAT32_SCHEMA, fValue);
        } else if (dValue != Double.NEGATIVE_INFINITY && dValue != Double.POSITIVE_INFINITY
                && decimal.scale() != 0) {
            return new SchemaAndValue(Schema.FLOAT64_SCHEMA, dValue);
        } else {
            Schema schema = Decimal.schema(decimal.scale());
            return new SchemaAndValue(schema, decimal);
        }
    }

    private SchemaAndValue parseAsTemporalType(String token, Schema schema, String pattern) {
        ParsePosition pos = new ParsePosition(0);
        java.util.Date result = new SimpleDateFormat(pattern).parse(token, pos);
        if (pos.getIndex() != 0) {
            return new SchemaAndValue(schema, result);
        }
        return null;
    }

    protected boolean canParseSingleTokenLiteral(boolean embedded, String tokenLiteral) {
        int startPosition = mark();
        // If the next token is what we expect, then either...
        if (canConsume(tokenLiteral)) {
            //   ...we're reading an embedded value, in which case the next token will be handled appropriately
            //      by the caller if it's something like an end delimiter for a map or array, or a comma to
            //      separate multiple embedded values...
            //   ...or it's being parsed as part of a top-level string, in which case, any other tokens should
            //      cause use to stop parsing this single-token literal as such and instead just treat it like
            //      a string. For example, the top-level string "true}" will be tokenized as the tokens "true" and
            //      "}", but should ultimately be parsed as just the string "true}" instead of the boolean true.
            if (embedded || !hasNext()) {
                return true;
            }
        }
        rewindTo(startPosition);
        return false;
    }

    protected SchemaAndValue parse(boolean embedded) throws NoSuchElementException {
        if (!hasNext()) {
            return null;
        } else if (embedded && canConsume(QUOTE_DELIMITER)) {
            return parseQuotedString();
        } else if (canParseSingleTokenLiteral(embedded, Values.NULL_VALUE)) {
            return null;
        } else if (canParseSingleTokenLiteral(embedded, TRUE_LITERAL)) {
            return TRUE_SCHEMA_AND_VALUE;
        } else if (canParseSingleTokenLiteral(embedded, FALSE_LITERAL)) {
            return FALSE_SCHEMA_AND_VALUE;
        }

        int startPosition = mark();

        try {
            if (canConsume(ARRAY_BEGIN_DELIMITER)) {
                return parseArray();
            } else if (canConsume(MAP_BEGIN_DELIMITER)) {
                return parseMap();
            }
        } catch (DataException e) {
            LOG.trace("Unable to parse the value as a map or an array; reverting to string", e);
            rewindTo(startPosition);
        }

        String token = next();
        if (Utils.isBlank(token)) {
            return new SchemaAndValue(Schema.STRING_SCHEMA, token);
        } else {
            return parseNextToken(embedded, token.trim());
        }
    }

    /**
     * Parse the specified string representation of a value into its schema and value.
     *
     * @param value the string form of the value
     * @return the schema and value; never null, but whose schema and value may be null
     * @see Values#convertToString
     */
    public static SchemaAndValue parseString(String value) {
        if (value == null) {
            return NULL_SCHEMA_AND_VALUE;
        }
        if (value.isEmpty()) {
            return new SchemaAndValue(Schema.STRING_SCHEMA, value);
        }
        Parser parser = new Parser(value);
        return parser.parse(false);
    }

    private SchemaAndValue parseArray() {
        List<Object> result = new ArrayList<>();
        SchemaMerger elementSchema = new SchemaMerger();
        while (hasNext()) {
            if (canConsume(ARRAY_END_DELIMITER)) {
                Schema listSchema;
                if (elementSchema.hasCommonSchema()) {
                    listSchema = SchemaBuilder.array(elementSchema.schema()).schema();
                    result = alignListEntriesWithSchema(listSchema, result);
                } else {
                    // Every value is null
                    listSchema = SchemaBuilder.arrayOfNull().build();
                }
                return new SchemaAndValue(listSchema, result);
            }

            if (canConsume(COMMA_DELIMITER)) {
                throw new DataException("Unable to parse an empty array element: " + original());
            }
            SchemaAndValue element = parse(true);
            elementSchema.merge(element);
            result.add(element != null ? element.value() : null);

            int currentPosition = mark();
            if (canConsume(ARRAY_END_DELIMITER)) {
                rewindTo(currentPosition);
            } else if (!canConsume(COMMA_DELIMITER)) {
                throw new DataException("Array elements missing '" + COMMA_DELIMITER + "' delimiter");
            }
        }

        // Missing either a comma or an end delimiter
        if (COMMA_DELIMITER.equals(previous())) {
            throw new DataException("Array is missing element after ',': " + original());
        }
        throw new DataException("Array is missing terminating ']': " + original());
    }

    private SchemaAndValue parseMultipleTokensAsTemporal(String token) {
        // The time and timestamp literals may be split into 5 tokens since an unescaped colon
        // is a delimiter. Check these first since the first of these tokens is a simple numeric
        int position = mark();
        String remainder = next(4);
        if (remainder != null) {
            String timeOrTimestampStr = token + remainder;
            SchemaAndValue temporal = parseAsTemporal(timeOrTimestampStr);
            if (temporal != null) {
                return temporal;
            }
        }
        // No match was found using the 5 tokens, so rewind and see if the current token has a date, time, or timestamp
        rewindTo(position);
        return parseAsTemporal(token);
    }

    public int position() {
        return iter.getIndex();
    }

    public int mark() {
        return iter.getIndex() - (nextToken != null ? nextToken.length() : 0);
    }

    public void rewindTo(int position) {
        iter.setIndex(position);
        nextToken = null;
        previousToken = null;
    }

    public String original() {
        return original;
    }

    public boolean hasNext() {
        return nextToken != null || canConsumeNextToken();
    }

    protected boolean canConsumeNextToken() {
        return iter.getEndIndex() > iter.getIndex();
    }

    public String next() {
        if (nextToken != null) {
            previousToken = nextToken;
            nextToken = null;
        } else {
            previousToken = consumeNextToken();
        }
        return previousToken;
    }

    public String next(int n) {
        int current = mark();
        int start = mark();
        for (int i = 0; i != n; ++i) {
            if (!hasNext()) {
                rewindTo(start);
                return null;
            }
            next();
        }
        return original.substring(current, position());
    }

    private String consumeNextToken() throws NoSuchElementException {
        boolean escaped = false;
        int start = iter.getIndex();
        char c = iter.current();
        while (canConsumeNextToken()) {
            switch (c) {
                case '\\':
                    escaped = !escaped;
                    break;
                case ':':
                case ',':
                case '{':
                case '}':
                case '[':
                case ']':
                case '\"':
                    if (!escaped) {
                        if (start < iter.getIndex()) {
                            // Return the previous token
                            return original.substring(start, iter.getIndex());
                        }
                        // Consume and return this delimiter as a token
                        iter.next();
                        return original.substring(start, start + 1);
                    }
                    // escaped, so continue
                    escaped = false;
                    break;
                default:
                    // If escaped, then we don't care what was escaped
                    escaped = false;
                    break;
            }
            c = iter.next();
        }
        return original.substring(start, iter.getIndex());
    }

    public String previous() {
        return previousToken;
    }

    public boolean canConsume(String expected) {
        return canConsume(expected, true);
    }

    public boolean canConsume(String expected, boolean ignoreLeadingAndTrailingWhitespace) {
        if (isNext(expected, ignoreLeadingAndTrailingWhitespace)) {
            // consume this token ...
            nextToken = null;
            return true;
        }
        return false;
    }

    protected boolean isNext(String expected, boolean ignoreLeadingAndTrailingWhitespace) {
        if (nextToken == null) {
            if (!hasNext()) {
                return false;
            }
            // There's another token, so consume it
            nextToken = consumeNextToken();
        }
        if (ignoreLeadingAndTrailingWhitespace) {
            while (Utils.isBlank(nextToken) && canConsumeNextToken()) {
                nextToken = consumeNextToken();
            }
        }
        return ignoreLeadingAndTrailingWhitespace
                ? nextToken.trim().equals(expected)
                : nextToken.equals(expected);
    }

    /**
     * Utility for merging various optional primitive numeric schemas into a common schema.
     * If a non-numeric type appears (including logical numeric types), no common schema will be inferred.
     * This class is not thread-safe and should only be accessed by one thread.
     */
    private static class SchemaMerger {
        /**
         * Schema which applies to all of the values passed to {@link #merge(SchemaAndValue)}
         * Null if no non-null schemas have been seen, or if the values seen do not have a common schema
         */
        private Schema common = null;
        /**
         * Flag to determine the meaning of the null sentinel in {@link #common}
         * If true, null means "any optional type", as no non-null values have appeared.
         * If false, null means "no common type", as one or more non-null values had mutually exclusive schemas.
         */
        private boolean compatible = true;

        protected void merge(SchemaAndValue latest) {
            if (latest != null && latest.schema() != null && compatible) {
                if (common == null) {
                    // This null means any type is valid, so choose the new schema.
                    common = latest.schema();
                } else {
                    // There is a previous type restriction, so merge the new schema into the old one.
                    common = mergeSchemas(common, latest.schema());
                    // If there isn't a common schema any longer, then give up on finding further compatible schemas.
                    compatible = common != null;
                }
            }
        }

        protected boolean hasCommonSchema() {
            return common != null;
        }

        protected Schema schema() {
            return common;
        }
    }

    /**
     * Merge two schemas to a common schema which can represent values from both input schemas.
     * @param previous One Schema, non-null
     * @param newSchema Another schema, non-null
     * @return A schema that is a superset of both input schemas, or null if no common schema is found.
     */
    private static Schema mergeSchemas(Schema previous, Schema newSchema) {
        Schema.Type previousType = previous.type();
        Schema.Type newType = newSchema.type();
        if (previousType != newType) {
            switch (previous.type()) {
                case INT8:
                    return commonSchemaForInt8(newSchema, newType);
                case INT16:
                    return commonSchemaForInt16(previous, newSchema, newType);
                case INT32:
                    return commonSchemaForInt32(previous, newSchema, newType);
                case INT64:
                    return commonSchemaForInt64(previous, newSchema, newType);
                case FLOAT32:
                    return commonSchemaForFloat32(previous, newSchema, newType);
                case FLOAT64:
                    return commonSchemaForFloat64(previous, newType);
            }
            return null;
        }
        if (previous.isOptional() == newSchema.isOptional()) {
            // Use the optional one
            return previous.isOptional() ? previous : newSchema;
        }
        if (!previous.equals(newSchema)) {
            return null;
        }
        return previous;
    }

    private static Schema commonSchemaForInt8(Schema newSchema, Schema.Type newType) {
        if (newType == Schema.Type.INT16 || newType == Schema.Type.INT32 || newType == Schema.Type.INT64
                || newType == Schema.Type.FLOAT32 || newType == Schema.Type.FLOAT64) {
            return newSchema;
        }
        return null;
    }

    private static Schema commonSchemaForInt16(Schema previous, Schema newSchema, Schema.Type newType) {
        if (newType == Schema.Type.INT8) {
            return previous;
        } else if (newType == Schema.Type.INT32 || newType == Schema.Type.INT64
                || newType == Schema.Type.FLOAT32 || newType == Schema.Type.FLOAT64) {
            return newSchema;
        }
        return null;
    }

    private static Schema commonSchemaForInt32(Schema previous, Schema newSchema, Schema.Type newType) {
        if (newType == Schema.Type.INT8 || newType == Schema.Type.INT16) {
            return previous;
        } else if (newType == Schema.Type.INT64 || newType == Schema.Type.FLOAT32 || newType == Schema.Type.FLOAT64) {
            return newSchema;
        }
        return null;
    }

    private static Schema commonSchemaForInt64(Schema previous, Schema newSchema, Schema.Type newType) {
        if (newType == Schema.Type.INT8 || newType == Schema.Type.INT16 || newType == Schema.Type.INT32) {
            return previous;
        } else if (newType == Schema.Type.FLOAT32 || newType == Schema.Type.FLOAT64) {
            return newSchema;
        }
        return null;
    }

    private static Schema commonSchemaForFloat32(Schema previous, Schema newSchema, Schema.Type newType) {
        if (newType == Schema.Type.INT8 || newType == Schema.Type.INT16 || newType == Schema.Type.INT32 || newType == Schema.Type.INT64) {
            return previous;
        } else if (newType == Schema.Type.FLOAT64) {
            return newSchema;
        }
        return null;
    }

    private static Schema commonSchemaForFloat64(Schema previous, Schema.Type newType) {
        if (newType == Schema.Type.INT8 || newType == Schema.Type.INT16 || newType == Schema.Type.INT32 || newType == Schema.Type.INT64
                || newType == Schema.Type.FLOAT32) {
            return previous;
        }
        return null;
    }

    protected static List<Object> alignListEntriesWithSchema(Schema schema, List<Object> input) {
        Schema valueSchema = schema.valueSchema();
        List<Object> result = new ArrayList<>();
        for (Object value : input) {
            Object newValue = Values.convertTo(valueSchema, null, value);
            result.add(newValue);
        }
        return result;
    }

    protected static Map<Object, Object> alignMapKeysAndValuesWithSchema(Schema mapSchema, Map<Object, Object> input) {
        Schema keySchema = mapSchema.keySchema();
        Schema valueSchema = mapSchema.valueSchema();
        Map<Object, Object> result = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : input.entrySet()) {
            Object newKey = Values.convertTo(keySchema, null, entry.getKey());
            Object newValue = Values.convertTo(valueSchema, null, entry.getValue());
            result.put(newKey, newValue);
        }
        return result;
    }

    protected static Map<Object, Object> alignMapKeysWithSchema(Schema mapSchema, Map<Object, Object> input) {
        Schema keySchema = mapSchema.keySchema();
        Map<Object, Object> result = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : input.entrySet()) {
            Object newKey = Values.convertTo(keySchema, null, entry.getKey());
            result.put(newKey, entry.getValue());
        }
        return result;
    }
}
