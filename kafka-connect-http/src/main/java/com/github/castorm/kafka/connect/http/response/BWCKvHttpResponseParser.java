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
import com.github.castorm.kafka.connect.http.record.spi.KvSourceRecordMapper;
import com.github.castorm.kafka.connect.http.response.spi.BWCHttpResponseParser;
import com.github.castorm.kafka.connect.http.response.spi.KvRecordHttpResponseParser;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.connect.source.SourceRecord;

@RequiredArgsConstructor
public class BWCKvHttpResponseParser implements BWCHttpResponseParser {

    private final Function<Map<String, ?>, BWCKvHttpResponseParserConfig> configFactory;

    private KvRecordHttpResponseParser recordParser;

    private KvSourceRecordMapper recordMapper;

    public BWCKvHttpResponseParser() {
        this(BWCKvHttpResponseParserConfig::new);
    }

    @Override
    public void configure(Map<String, ?> configs) {
        BWCKvHttpResponseParserConfig config = configFactory.apply(configs);
        recordParser = config.getRecordParser();
        recordMapper = config.getRecordMapper();
    }

    @Override
    public List<SourceRecord> parse(HttpResponse response) {
        return recordParser.parse(response).stream().map(recordMapper::map).collect(toList());
    }
}
