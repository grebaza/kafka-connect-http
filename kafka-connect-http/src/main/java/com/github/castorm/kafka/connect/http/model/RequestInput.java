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

import static java.util.Collections.emptyMap;
import static java.util.Optional.ofNullable;

import edu.emory.mathcs.backport.java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.ToString;

/**
 * Data-model for HttpRequest Factory
 */
@ToString
@AllArgsConstructor
@Getter
public class RequestInput {

    /**
     * offset of record
     */
    @Default
    Map<String, ?> offset = emptyMap();

    @Default
    Map<String, ?> paging = emptyMap();

    @Default
    Map<String, ?> metadata = emptyMap();

    RequestInput last;

    public static RequestInput of(Offset offset) {
        return of(offset, null, null);
    }

    public static RequestInput of(Offset offset, Map<String, ?> paging) {
        return of(offset, paging, null);
    }

    public static RequestInput of(Offset offset, Map<String, ?> paging, Map<String, ?> metadata) {
        return of(offset, paging, metadata, null);
    }

    public static RequestInput of(
            Offset offset, Map<String, ?> paging, Map<String, ?> metadata, RequestInput requestInput) {
        return new RequestInput(offset.toMap(), paging, metadata, requestInput);
    }

    public static RequestInput immutableOf(Offset offset, Map<String, ?> paging, Map<String, ?> metadata) {
        return new RequestInput(
                ofNullable(offset.toMap()).orElseGet(Collections::emptyMap),
                ofNullable(paging).map(HashMap::new).orElseGet(HashMap::new),
                ofNullable(metadata).map(HashMap::new).orElseGet(HashMap::new),
                null);
    }
}
