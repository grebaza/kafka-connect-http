package com.github.castorm.kafka.connect.http.model;

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

import static java.util.Optional.ofNullable;

import edu.emory.mathcs.backport.java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * Data-model for Parsed Http Response
 */
@ToString
@AllArgsConstructor
@Getter
public class ParsedResponse {

    final List<SourceRecord> records;
    final Map<String, ?> paging;
    final Map<String, ?> metadata;

    public static ParsedResponse of() {
        return of(null, null, null);
    }

    public static ParsedResponse of(List<SourceRecord> records) {
        return of(records, null, null);
    }

    public static ParsedResponse of(List<SourceRecord> records, Map<String, ?> paging) {
        return of(records, paging, null);
    }

    public static ParsedResponse of(List<SourceRecord> records, Map<String, ?> paging, Map<String, ?> metadata) {
        return new ParsedResponse(
                ofNullable(records).orElseGet(Collections::emptyList),
                ofNullable(paging).orElseGet(Collections::emptyMap),
                ofNullable(metadata).orElseGet(Collections::emptyMap));
    }
}
