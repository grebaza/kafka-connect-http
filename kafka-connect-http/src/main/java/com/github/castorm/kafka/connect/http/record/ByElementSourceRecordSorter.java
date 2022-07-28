package com.github.castorm.kafka.connect.http.record;

/*-
 * #%L
 * Kafka Connect HTTP
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

import com.github.castorm.kafka.connect.http.record.spi.SourceRecordSorter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.connect.source.SourceRecord;

@RequiredArgsConstructor
public class ByElementSourceRecordSorter implements SourceRecordSorter {

    private final Function<Map<String, ?>, ByElementSourceRecordSorterConfig> configFactory;

    private OrderField orderField;

    public ByElementSourceRecordSorter() {
        this(ByElementSourceRecordSorterConfig::new);
    }

    @Override
    public void configure(Map<String, ?> settings) {
        orderField = configFactory.apply(settings).getOrderField();
    }

    @Override
    public List<SourceRecord> sort(List<SourceRecord> records) {
        return sortWithField(records, orderField);
    }

    private static List<SourceRecord> sortWithField(List<SourceRecord> records, OrderField field) {
        final Comparator<SourceRecord> comparator;
        final List<SourceRecord> newRecords = new ArrayList<>(records);
        switch (field) {
            case TOPIC:
                comparator = (o1, o2) -> o1.topic().compareTo(o2.topic());
                break;
            case PARTITION:
                comparator = (o1, o2) -> o1.kafkaPartition().compareTo(o2.kafkaPartition());
                break;
            case TIMESTAMP:
            default:
                comparator = (o1, o2) -> o1.timestamp().compareTo(o2.timestamp());
                break;
        }
        newRecords.sort(comparator);
        return newRecords;
    }

    public enum OrderField {
        TOPIC,
        PARTITION,
        TIMESTAMP
    }
}
