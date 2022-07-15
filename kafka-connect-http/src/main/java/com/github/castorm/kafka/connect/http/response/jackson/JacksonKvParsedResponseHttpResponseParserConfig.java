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

import static com.github.castorm.kafka.connect.common.ConfigUtils.breakDownList;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toMap;
import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Type.CLASS;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

import com.github.castorm.kafka.connect.http.response.timestamp.EpochMillisOrDelegateTimestampParser;
import com.github.castorm.kafka.connect.http.response.timestamp.spi.TimestampParser;
import java.util.Map;
import lombok.Getter;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

@Getter
public class JacksonKvParsedResponseHttpResponseParserConfig extends AbstractConfig {

    static final String RECORD_TIMESTAMP_PARSER_CLASS = "http.response.record.timestamp.parser";
    static final String RECORD_ADD_FIELDS_PREFIX = "http.response.record.add.fields.prefix";
    static final String RECORD_ADD_FIELDS = "http.response.record.add.fields";

    private final TimestampParser timestampParser;
    private final JacksonSerializer serializer;
    private final JacksonRecordParser recordParser;
    private final String recordAddFieldsPrefix;
    private final Map<String, String> recordAddFieldsTargetNames;

    JacksonKvParsedResponseHttpResponseParserConfig(Map<String, ?> originals) {
        super(config(), originals);
        serializer = new JacksonSerializer();
        recordParser = new JacksonRecordParser(serializer);
        recordParser.configure(originals);
        timestampParser = getConfiguredInstance(RECORD_TIMESTAMP_PARSER_CLASS, TimestampParser.class);
        recordAddFieldsPrefix = ofNullable(getString(RECORD_ADD_FIELDS_PREFIX)).orElse("");
        recordAddFieldsTargetNames = breakDownList(
                        ofNullable(getString(RECORD_ADD_FIELDS)).orElse(""))
                .stream()
                .collect(toMap(e -> e, e -> recordAddFieldsPrefix.concat(e), (e1, e2) -> e1));
    }

    public static ConfigDef config() {
        return new ConfigDef()
                .define(
                        RECORD_TIMESTAMP_PARSER_CLASS,
                        CLASS,
                        EpochMillisOrDelegateTimestampParser.class,
                        LOW,
                        "Record Timestamp parser class")
                .define(
                        RECORD_ADD_FIELDS,
                        STRING,
                        null,
                        MEDIUM,
                        "List of response-level fields to add to record's value (available options: paging, metadata)")
                .define(RECORD_ADD_FIELDS_PREFIX, STRING, "__", MEDIUM, "Optional prefix for added fields");
    }
}
