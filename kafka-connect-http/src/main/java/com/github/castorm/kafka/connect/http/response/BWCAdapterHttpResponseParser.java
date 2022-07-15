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

import com.github.castorm.kafka.connect.http.model.HttpResponse;
import com.github.castorm.kafka.connect.http.model.ParsedResponse;
import com.github.castorm.kafka.connect.http.response.spi.BWCHttpResponseParser;
import com.github.castorm.kafka.connect.http.response.spi.HttpResponseParser;
import java.util.Map;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class BWCAdapterHttpResponseParser implements HttpResponseParser {

    private final Function<Map<String, ?>, BWCAdapterHttpResponseParserConfig> configFactory;

    private BWCHttpResponseParser adaptee;

    public BWCAdapterHttpResponseParser() {
        this(BWCAdapterHttpResponseParserConfig::new);
    }

    @Override
    public void configure(Map<String, ?> settings) {
        BWCAdapterHttpResponseParserConfig config = configFactory.apply(settings);
        adaptee = config.getAdapteeParser();
    }

    @Override
    public ParsedResponse parse(HttpResponse response) {
        return ParsedResponse.of(adaptee.parse(response));
    }
}
