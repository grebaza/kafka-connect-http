package com.github.castorm.kafka.connect.http.response.jackson;

/*-
 * #%L
 * kafka-connect-http
 * %%
 * Copyright (C) 2020 CastorM
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import static java.util.Optional.ofNullable;
import static java.util.UUID.nameUUIDFromBytes;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.castorm.kafka.connect.http.model.HttpResponse;
import com.github.castorm.kafka.connect.http.model.KvParsedResponse;
import com.github.castorm.kafka.connect.http.model.Offset;
import com.github.castorm.kafka.connect.http.record.model.KvRecord;
import com.github.castorm.kafka.connect.http.response.spi.KvParsedResponseHttpResponseParser;
import com.github.castorm.kafka.connect.http.response.timestamp.spi.TimestampParser;
import java.time.Instant;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class JacksonKvParsedResponseHttpResponseParser implements KvParsedResponseHttpResponseParser {

    private final Function<Map<String, ?>, JacksonKvParsedResponseHttpResponseParserConfig> configFactory;

    private JacksonRecordParser recordParser;
    private JacksonSerializer serializer;
    private TimestampParser timestampParser;
    private Map<String, String> addFieldsTargetNames;
    private JsonNode jsonBody;
    private Map<String, Supplier<Map<String, ?>>> addFieldsSuppliers = Stream.of(
                    new SimpleEntry<>("paging", (Supplier<Map<String, ?>>) () -> recordParser.getPaging(jsonBody)),
                    new SimpleEntry<>("metadata", (Supplier<Map<String, ?>>) () -> recordParser.getMetadata(jsonBody)))
            .collect(toMap(Entry::getKey, Entry::getValue));

    public JacksonKvParsedResponseHttpResponseParser() {
        this(JacksonKvParsedResponseHttpResponseParserConfig::new);
    }

    @Override
    public void configure(Map<String, ?> configs) {
        JacksonKvParsedResponseHttpResponseParserConfig config = configFactory.apply(configs);
        recordParser = config.getRecordParser();
        serializer = config.getSerializer();
        timestampParser = config.getTimestampParser();
        addFieldsTargetNames = config.getRecordAddFieldsTargetNames();
    }

    @Override
    public KvParsedResponse parse(HttpResponse response) {
        jsonBody = serializer.deserialize(response.getBody());

        return KvParsedResponse.of(
                recordParser.getRecords(jsonBody).map(this::toKvRecord).collect(toList()),
                recordParser.getPaging(jsonBody),
                recordParser.getMetadata(jsonBody));
    }

    private KvRecord toKvRecord(JsonNode jsonRecord) {
        Map<String, Object> offsets = recordParser.getOffset(jsonRecord);

        String key = ofNullable(offsets.get("key"))
                .map(String.class::cast)
                .orElseGet(() -> generateConsistentKey(recordParser.getValue(jsonRecord)));

        Optional<Instant> timestamp =
                ofNullable(offsets.get("timestamp")).map(String.class::cast).map(timestampParser::parse);

        Offset offset = timestamp.map(ts -> Offset.of(offsets, key, ts)).orElseGet(() -> Offset.of(offsets, key));

        return KvRecord.builder()
                .key(key)
                .value(getRecordValue(jsonRecord))
                .offset(offset)
                .build();
    }

    private static String generateConsistentKey(String body) {
        return nameUUIDFromBytes(body.getBytes()).toString();
    }

    private String getRecordValue(JsonNode jsonRecord) {
        if (addFieldsTargetNames.isEmpty()) {
            return recordParser.getValue(jsonRecord);
        } else {
            final JsonNode value = recordParser.getValueObject(jsonRecord);
            addFieldsTargetNames.entrySet().stream()
                    .map(e -> getAddFieldTask(e))
                    .forEach(t -> t.ifPresent(e -> ((ObjectNode) value).putPOJO(e.getKey(), e.getValue())));

            return value.isObject() ? serializer.serialize(value) : value.asText();
        }
    }

    /**
     * Get Add-field Key and Value as an Optional Map Entry.
     * Value is a Map of Paging or Metadata fields enabled in RECORD_ADD_FIELDS config.
     */
    private Optional<Entry<String, Map<String, ?>>> getAddFieldTask(Entry<String, String> entry) {
        return ofNullable(addFieldsSuppliers.get(entry.getKey()))
                .map(e -> new SimpleEntry<>(entry.getValue(), ((Supplier<Map<String, ?>>) e).get()));
    }
}
