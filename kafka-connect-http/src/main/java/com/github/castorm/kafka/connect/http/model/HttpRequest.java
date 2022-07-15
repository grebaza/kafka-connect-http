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

import static com.github.castorm.kafka.connect.http.model.HttpRequest.HttpMethod.GET;
import static java.util.Collections.emptyMap;

import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Value;

@Value
@Builder
public class HttpRequest {

    @Default
    HttpMethod method = GET;

    String url;

    @Default
    Map<String, List<String>> queryParams = emptyMap();

    @Default
    Map<String, List<String>> headers = emptyMap();

    byte[] body;

    public enum HttpMethod {
        GET,
        HEAD,
        POST,
        PUT,
        PATCH
    }
}
