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

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Type.CLASS;

import com.github.castorm.kafka.connect.http.response.spi.BWCHttpResponseParser;
import java.util.Map;
import lombok.Getter;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

@Getter
public class BWCAdapterHttpResponseParserConfig extends AbstractConfig {

    static final String ADAPTEE_PARSER = "http.response.backward.compatible.parser";

    private final BWCHttpResponseParser adapteeParser;

    public BWCAdapterHttpResponseParserConfig(Map<String, ?> originals) {
        super(config(), originals);
        adapteeParser = getConfiguredInstance(ADAPTEE_PARSER, BWCHttpResponseParser.class);
    }

    public static ConfigDef config() {
        return new ConfigDef()
                .define(
                        ADAPTEE_PARSER,
                        CLASS,
                        BWCKvHttpResponseParser.class,
                        HIGH,
                        "Backward-compatible Response Parser Class");
    }
}
