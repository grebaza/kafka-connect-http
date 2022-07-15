package com.github.castorm.kafka.connect.http.model;

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

import static java.util.Optional.ofNullable;

import com.github.castorm.kafka.connect.http.record.model.KvRecord;
import edu.emory.mathcs.backport.java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.Value;
import lombok.With;

@With
@Value
@Builder
public class KvParsedResponse {

    List<KvRecord> records;

    Map<String, ?> paging;

    Map<String, ?> metadata;

    public static KvParsedResponse of(List<KvRecord> records) {
        return of(records, null, null);
    }

    public static KvParsedResponse of(List<KvRecord> records, Map<String, ?> paging) {
        return of(records, paging, null);
    }

    public static KvParsedResponse of(List<KvRecord> records, Map<String, ?> paging, Map<String, ?> metadata) {
        return new KvParsedResponse(
                ofNullable(records).orElseGet(Collections::emptyList),
                ofNullable(paging).orElseGet(Collections::emptyMap),
                ofNullable(metadata).orElseGet(Collections::emptyMap));
    }
}
