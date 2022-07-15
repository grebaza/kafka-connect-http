package com.github.castorm.kafka.connect.http.record.spi;

/*-
 * #%L
 * Kafka Connect HTTP
 * %%
 * Copyright (C) 2020 - 2022 Cástor Rodríguez
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

import com.github.castorm.kafka.connect.http.model.KvParsedResponse;
import com.github.castorm.kafka.connect.http.model.ParsedResponse;
import java.util.Map;
import org.apache.kafka.common.Configurable;

@FunctionalInterface
public interface KvParsedResponseMapper extends Configurable {

    ParsedResponse map(KvParsedResponse record);

    default void configure(Map<String, ?> map) {
        // Do nothing
    }
}
