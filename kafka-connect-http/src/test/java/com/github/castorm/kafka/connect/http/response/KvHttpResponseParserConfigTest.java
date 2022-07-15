package com.github.castorm.kafka.connect.http.response;

/*-
 * #%L
 * Kafka Connect HTTP Plugin
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

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;

import com.github.castorm.kafka.connect.http.model.HttpResponse;
import com.github.castorm.kafka.connect.http.model.KvParsedResponse;
import com.github.castorm.kafka.connect.http.record.SchemedKvSourceRecordMapper;
import com.github.castorm.kafka.connect.http.record.model.KvRecord;
import com.github.castorm.kafka.connect.http.record.spi.KvSourceRecordMapper;
import com.github.castorm.kafka.connect.http.response.jackson.JacksonKvParsedResponseHttpResponseParser;
import com.github.castorm.kafka.connect.http.response.spi.KvParsedResponseHttpResponseParser;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

class KvHttpResponseParserConfigTest {

    @Test
    void whenNoDelegate_thenDefault() {
        assertThat(config(emptyMap()).getResponseParser())
                .isInstanceOf(JacksonKvParsedResponseHttpResponseParser.class);
    }

    @Test
    void whenDelegate_thenInitialized() {
        assertThat(config(ImmutableMap.of("http.response.response.parser", TestResponseParser.class.getName()))
                        .getResponseParser())
                .isInstanceOf(TestResponseParser.class);
    }

    @Test
    void whenNoPolicy_thenDefault() {
        assertThat(config(emptyMap()).getRecordMapper()).isInstanceOf(SchemedKvSourceRecordMapper.class);
    }

    @Test
    void whenPolicy_thenInitialized() {
        assertThat(config(ImmutableMap.of("http.response.record.mapper", TestRecordMapper.class.getName()))
                        .getRecordMapper())
                .isInstanceOf(TestRecordMapper.class);
    }

    public static class TestResponseParser implements KvParsedResponseHttpResponseParser {

        @Override
        public KvParsedResponse parse(HttpResponse response) {
            return KvParsedResponse.of(null, null, null);
        }
    }

    public static class TestRecordMapper implements KvSourceRecordMapper {

        @Override
        public SourceRecord map(KvRecord record) {
            return null;
        }
    }

    private static KvHttpResponseParserConfig config(Map<String, Object> settings) {
        Map<String, Object> defaultSettings = new HashMap<String, Object>() {
            {
                put("kafka.topic", "topic");
            }
        };
        defaultSettings.putAll(settings);
        return new KvHttpResponseParserConfig(defaultSettings);
    }
}
