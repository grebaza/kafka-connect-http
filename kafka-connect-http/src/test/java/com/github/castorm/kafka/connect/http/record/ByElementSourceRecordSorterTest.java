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

import static com.github.castorm.kafka.connect.http.record.ByElementSourceRecordSorter.OrderField.TIMESTAMP;
import static com.github.castorm.kafka.connect.http.record.ByElementSourceRecordSorterTest.Fixture.mid;
import static com.github.castorm.kafka.connect.http.record.ByElementSourceRecordSorterTest.Fixture.newer;
import static com.github.castorm.kafka.connect.http.record.ByElementSourceRecordSorterTest.Fixture.older;
import static com.github.castorm.kafka.connect.http.record.ByElementSourceRecordSorterTest.Fixture.ordered;
import static com.github.castorm.kafka.connect.http.record.ByElementSourceRecordSorterTest.Fixture.premid;
import static com.github.castorm.kafka.connect.http.record.ByElementSourceRecordSorterTest.Fixture.prenewer;
import static com.github.castorm.kafka.connect.http.record.ByElementSourceRecordSorterTest.Fixture.unordered;
import static java.lang.Long.MAX_VALUE;
import static java.lang.Long.MIN_VALUE;
import static java.util.Arrays.asList;
import static java.util.Collections.shuffle;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

import com.github.castorm.kafka.connect.http.record.ByElementSourceRecordSorter.OrderField;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.UnaryOperator;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ByElementSourceRecordSorterTest {

    ByElementSourceRecordSorter sorter;

    @Mock
    ByElementSourceRecordSorterConfig config;

    @Test
    void givenTimestamp_whenOrderedRecords_thenAsIs() {

        givenField(TIMESTAMP);

        assertThat(sorter.sort(ordered)).containsExactly(older, premid, mid, prenewer, newer);
    }

    @Test
    void givenTimestamp_whenUnOrderedRecords_thenOrdered() {

        givenField(TIMESTAMP);

        assertThat(sorter.sort(unordered)).containsExactly(older, premid, mid, prenewer, newer);
    }

    private void givenField(OrderField field) {
        sorter = new ByElementSourceRecordSorter(__ -> config);
        given(config.getOrderField()).willReturn(field);
        sorter.configure(Collections.emptyMap());
    }

    interface Fixture {
        SourceRecord older = new SourceRecord(null, null, null, null, null, null, null, null, MIN_VALUE);
        SourceRecord premid = new SourceRecord(null, null, null, null, null, null, null, null, MIN_VALUE / 2);
        SourceRecord mid = new SourceRecord(null, null, null, null, null, null, null, null, 0L);
        SourceRecord prenewer = new SourceRecord(null, null, null, null, null, null, null, null, MAX_VALUE / 2);
        SourceRecord newer = new SourceRecord(null, null, null, null, null, null, null, null, MAX_VALUE);
        UnaryOperator<List<SourceRecord>> shuffler = (o) -> {
            List<SourceRecord> l = new ArrayList<>(o);
            shuffle(l);
            return l;
        };
        List<SourceRecord> ordered = asList(older, premid, mid, prenewer, newer);
        List<SourceRecord> unordered = shuffler.apply(ordered);
    }
}
