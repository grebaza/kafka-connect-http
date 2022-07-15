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

import com.fasterxml.jackson.databind.JsonNode;
import com.github.castorm.kafka.connect.http.model.HttpResponse;
import com.github.castorm.kafka.connect.http.model.Offset;
import com.github.castorm.kafka.connect.http.record.model.KvRecord;
import com.github.castorm.kafka.connect.http.response.spi.KvRecordHttpResponseParser;
import com.github.castorm.kafka.connect.http.response.timestamp.spi.TimestampParser;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class JacksonKvRecordHttpResponseParser implements KvRecordHttpResponseParser {

    private final Function<Map<String, ?>, JacksonKvRecordHttpResponseParserConfig> configFactory;

    private JacksonRecordParser recordParser;
    private JacksonSerializer serializer;
    private TimestampParser timestampParser;

    public JacksonKvRecordHttpResponseParser() {
        this(JacksonKvRecordHttpResponseParserConfig::new);
    }

    @Override
    public void configure(Map<String, ?> configs) {
        JacksonKvRecordHttpResponseParserConfig config = configFactory.apply(configs);
        recordParser = config.getRecordParser();
        serializer = config.getSerializer();
        timestampParser = config.getTimestampParser();
    }

    @Override
    public List<KvRecord> parse(HttpResponse response) {
        JsonNode jsonBody = serializer.deserialize(response.getBody());

        return recordParser.getRecords(jsonBody).map(this::toKvRecord).collect(toList());
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
                .value(recordParser.getValue(jsonRecord))
                .offset(offset)
                .build();
    }

    private static String generateConsistentKey(String body) {
        return nameUUIDFromBytes(body.getBytes()).toString();
    }
}
