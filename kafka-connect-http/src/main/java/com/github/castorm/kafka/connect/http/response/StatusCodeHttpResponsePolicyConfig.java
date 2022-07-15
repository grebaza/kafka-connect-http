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

import static com.github.castorm.kafka.connect.common.ConfigUtils.parseIntegerRangedList;
import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

import java.util.Map;
import java.util.Set;
import lombok.Getter;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

@Getter
public class StatusCodeHttpResponsePolicyConfig extends AbstractConfig {

    private static final String PROCESS_CODES = "http.response.policy.codes.process";
    private static final String SKIP_CODES = "http.response.policy.codes.skip";

    private final Set<Integer> processCodes;

    private final Set<Integer> skipCodes;

    public StatusCodeHttpResponsePolicyConfig(Map<String, ?> originals) {
        super(config(), originals);
        processCodes = parseIntegerRangedList(getString(PROCESS_CODES));
        skipCodes = parseIntegerRangedList(getString(SKIP_CODES));
    }

    public static ConfigDef config() {
        return new ConfigDef()
                .define(PROCESS_CODES, STRING, "200..299", HIGH, "Process Codes")
                .define(SKIP_CODES, STRING, "300..399", HIGH, "Skip Codes");
    }
}
