package com.github.castorm.kafka.connect.http.record;

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

import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

import com.github.castorm.kafka.connect.http.record.ByElementSourceRecordSorter.OrderField;
import java.util.Map;
import lombok.Getter;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

@Getter
public class ByElementSourceRecordSorterConfig extends AbstractConfig {

    static final String ORDER_FIELD = "http.response.list.order.field";

    private final OrderField orderField;

    ByElementSourceRecordSorterConfig(Map<String, ?> originals) {
        super(config(), originals);
        orderField = OrderField.valueOf(getString(ORDER_FIELD).toUpperCase());
    }

    public static ConfigDef config() {
        return new ConfigDef()
                .define(
                        ORDER_FIELD,
                        STRING,
                        "TIMESTAMP",
                        LOW,
                        "Field for sorting the list, either TOPIC, PARTITION, TIMESTAMP");
    }
}
