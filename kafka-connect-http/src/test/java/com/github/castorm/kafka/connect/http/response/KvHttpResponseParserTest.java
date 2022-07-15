package com.github.castorm.kafka.connect.http.response;

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

import static com.github.castorm.kafka.connect.http.response.KvHttpResponseParserTest.Fixture.kvParsedResponse;
import static com.github.castorm.kafka.connect.http.response.KvHttpResponseParserTest.Fixture.record;
import static com.github.castorm.kafka.connect.http.response.KvHttpResponseParserTest.Fixture.response;
import static com.github.castorm.kafka.connect.http.response.KvHttpResponseParserTest.Fixture.sourceRecord;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;

import com.github.castorm.kafka.connect.http.model.HttpResponse;
import com.github.castorm.kafka.connect.http.model.KvParsedResponse;
import com.github.castorm.kafka.connect.http.record.model.KvRecord;
import com.github.castorm.kafka.connect.http.record.spi.KvSourceRecordMapper;
import com.github.castorm.kafka.connect.http.response.spi.KvParsedResponseHttpResponseParser;
import java.util.List;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KvHttpResponseParserTest {

    KvHttpResponseParser parser;

    @Mock
    KvHttpResponseParserConfig config;

    @Mock
    KvParsedResponseHttpResponseParser responseParser;

    @Mock
    KvSourceRecordMapper recordFactory;

    @BeforeEach
    void setUp() {
        parser = new KvHttpResponseParser(__ -> config);
        given(config.getResponseParser()).willReturn(responseParser);
        given(config.getRecordMapper()).willReturn(recordFactory);
        parser.configure(emptyMap());
    }

    @Test
    void givenEmptyList_whenParse_thenEmpty() {

        given(responseParser.parse(response)).willReturn(kvParsedResponse(emptyList()));

        assertThat(parser.parse(response).getRecords()).isEmpty();
    }

    @Test
    void givenList_whenParse_thenItemsMapped() {

        given(responseParser.parse(response)).willReturn(kvParsedResponse(singletonList(record)));

        parser.parse(response);

        then(recordFactory).should().map(record);
    }

    @Test
    void givenEmptyList_whenParse_thenItemsMappedReturned() {

        given(responseParser.parse(response)).willReturn(kvParsedResponse(singletonList(record)));
        given(recordFactory.map(record)).willReturn(sourceRecord);

        assertThat(parser.parse(response).getRecords()).containsExactly(sourceRecord);
    }

    interface Fixture {
        HttpResponse response = HttpResponse.builder().build();
        KvRecord record = KvRecord.builder().build();
        SourceRecord sourceRecord = new SourceRecord(null, null, null, null, null);

        static KvParsedResponse kvParsedResponse(List<KvRecord> records) {
            return KvParsedResponse.of(records);
        }
    }
}
