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

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.Configurable;

@RequiredArgsConstructor
public class JacksonRecordParser implements Configurable {

    private final Function<Map<String, ?>, JacksonRecordParserConfig> configFactory;

    private final JacksonSerializer serializer;

    private JsonPointer recordsPointer;
    private List<JsonPointer> keyPointer;
    private Optional<JsonPointer> timestampPointer;
    private Map<String, JsonPointer> offsetPointers;
    private JsonPointer valuePointer;

    private Optional<JsonPointer> pagingRootPointer;
    private Map<String, JsonPointer> pagingNodePointers;
    private Optional<JsonPointer> metadataRootPointer;
    private Map<String, JsonPointer> metadataNodePointers;

    public JacksonRecordParser() {
        this(new JacksonSerializer(new ObjectMapper()));
    }

    public JacksonRecordParser(JacksonSerializer serializer) {
        this(JacksonRecordParserConfig::new, serializer);
    }

    @Override
    public void configure(Map<String, ?> settings) {
        JacksonRecordParserConfig config = configFactory.apply(settings);
        recordsPointer = config.getRecordsPointer();
        keyPointer = config.getKeyPointer();
        valuePointer = config.getValuePointer();
        offsetPointers = config.getOffsetPointers();
        timestampPointer = config.getTimestampPointer();
        pagingRootPointer = config.getPagingRootPointer();
        pagingNodePointers = config.getPagingNodePointers();
        metadataRootPointer = config.getMetadataRootPointer();
        metadataNodePointers = config.getMetadataNodePointers();
    }

    /** @deprecated Replaced by Offset */
    @Deprecated
    Optional<String> getKey(JsonNode node) {
        String key = keyPointer.stream()
                .map(pointer -> serializer.getObjectAt(node, pointer).asText())
                .filter(it -> !it.isEmpty())
                .collect(joining("+"));
        return key.isEmpty() ? Optional.empty() : Optional.of(key);
    }

    /** @deprecated Replaced by Offset */
    @Deprecated
    Optional<String> getTimestamp(JsonNode node) {
        return timestampPointer.map(
                pointer -> serializer.getObjectAt(node, pointer).asText());
    }

    Map<String, Object> getOffset(JsonNode node) {
        return offsetPointers.entrySet().stream().collect(toMap(Entry::getKey, entry -> serializer
                .getObjectAt(node, entry.getValue())
                .asText()));
    }

    String getValue(JsonNode node) {
        JsonNode value = serializer.getObjectAt(node, valuePointer);

        return value.isObject() ? serializer.serialize(value) : value.asText();
    }

    JsonNode getValueObject(JsonNode node) {
        return serializer.getObjectAt(node, valuePointer);
    }

    Stream<JsonNode> getRecords(JsonNode node) {
        return serializer.getArrayAt(node, recordsPointer);
    }

    /**
     * Returns Paging only if JsonPointers exist
     */
    public Map<String, Object> getPaging(JsonNode node) {
        final JsonNode fromNode =
                pagingRootPointer.flatMap(p -> serializer.getAt(node, p)).orElse(node);
        return getMapFromPointersAtNode(pagingNodePointers, fromNode);
    }

    /**
     * Returns Metadata only if JsonPointers exist
     */
    public Map<String, Object> getMetadata(JsonNode node) {
        final JsonNode fromNode =
                metadataRootPointer.flatMap(p -> serializer.getAt(node, p)).orElse(node);
        return getMapFromPointersAtNode(metadataNodePointers, fromNode);
    }

    private Map<String, Object> getMapFromPointersAtNode(Map<String, JsonPointer> pointers, JsonNode node) {
        return pointers.entrySet().stream()
                .map(entry -> new AbstractMap.SimpleEntry<>(
                        entry.getKey(),
                        serializer
                                .getAt(node, entry.getValue())
                                .map(JsonNode::asText)
                                .orElse(null)))
                .filter(entry -> entry.getValue() != null)
                .collect(toMap(Entry::getKey, Entry::getValue));
    }
}
