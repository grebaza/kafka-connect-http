package com.github.castorm.kafka.connect.http.response;

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

import static java.util.stream.Collectors.toList;

import com.github.castorm.kafka.connect.http.model.HttpResponse;
import com.github.castorm.kafka.connect.http.model.KvParsedResponse;
import com.github.castorm.kafka.connect.http.model.ParsedResponse;
import com.github.castorm.kafka.connect.http.record.spi.KvSourceRecordMapper;
import com.github.castorm.kafka.connect.http.response.spi.HttpResponseParser;
import com.github.castorm.kafka.connect.http.response.spi.KvParsedResponseHttpResponseParser;
import java.util.Map;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class KvHttpResponseParser implements HttpResponseParser {

    private final Function<Map<String, ?>, KvHttpResponseParserConfig> configFactory;

    private KvParsedResponseHttpResponseParser responseParser;

    private KvSourceRecordMapper recordMapper;

    public KvHttpResponseParser() {
        this(KvHttpResponseParserConfig::new);
    }

    @Override
    public void configure(Map<String, ?> configs) {
        KvHttpResponseParserConfig config = configFactory.apply(configs);
        responseParser = config.getResponseParser();
        recordMapper = config.getRecordMapper();
    }

    @Override
    public ParsedResponse parse(HttpResponse response) {
        return map(responseParser.parse(response));
    }

    private ParsedResponse map(KvParsedResponse parsed) {
        return ParsedResponse.of(
                parsed.getRecords().stream().map(recordMapper::map).collect(toList()),
                parsed.getPaging(),
                parsed.getMetadata());
    }
}
