package com.github.castorm.kafka.connect.http.response.jackson;

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

import static com.fasterxml.jackson.core.JsonPointer.compile;
import static com.github.castorm.kafka.connect.http.response.jackson.JacksonRecordParserConfigTest.Fixture.config;
import static com.github.castorm.kafka.connect.http.response.jackson.JacksonRecordParserConfigTest.Fixture.configWithout;
import static com.github.castorm.kafka.connect.http.response.jackson.JacksonRecordParserConfigTest.Fixture.metadataConfig;
import static com.github.castorm.kafka.connect.http.response.jackson.JacksonRecordParserConfigTest.Fixture.offsetConfig;
import static com.github.castorm.kafka.connect.http.response.jackson.JacksonRecordParserConfigTest.Fixture.pagingConfig;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class JacksonRecordParserConfigTest {

    @Test
    void whenItemsPointerConfigured_thenInitialized() {
        assertThat(config(JacksonRecordParserConfig.LIST_POINTER, "/test-pointer")
                        .getRecordsPointer())
                .isEqualTo(compile("/test-pointer"));
    }

    @Test
    void whenMissingItemKeyPointerConfigured_thenInitialized() {
        assertThat(configWithout(JacksonRecordParserConfig.ITEM_KEY_POINTER).getKeyPointer())
                .isEmpty();
    }

    @Test
    void whenItemKeyPointerConfigured_thenInitialized() {
        assertThat(config(JacksonRecordParserConfig.ITEM_KEY_POINTER, "/test-pointer")
                        .getKeyPointer())
                .isEqualTo(asList(compile("/test-pointer")));
    }

    @Test
    void whenItemKeyPointersConfigured_thenInitialized() {
        assertThat(config(JacksonRecordParserConfig.ITEM_KEY_POINTER, "/a,/b").getKeyPointer())
                .isEqualTo(asList(compile("/a"), compile("/b")));
    }

    @Test
    void whenItemValuePointerConfigured_thenInitialized() {
        assertThat(config(JacksonRecordParserConfig.ITEM_POINTER, "/test-pointer")
                        .getValuePointer())
                .isEqualTo(compile("/test-pointer"));
    }

    @Test
    void whenMissingTimestampPointerConfigured_thenInitialized() {
        assertThat(configWithout(JacksonRecordParserConfig.ITEM_TIMESTAMP_POINTER)
                        .getTimestampPointer())
                .isEmpty();
    }

    @Test
    void whenItemTimestampPointerConfigured_thenInitialized() {
        assertThat(config(JacksonRecordParserConfig.ITEM_TIMESTAMP_POINTER, "/test-pointer")
                        .getTimestampPointer())
                .isEqualTo(Optional.of(compile("/test-pointer")));
    }

    @Test
    void whenItemOffsetPointersHalfConfigured_thenEmpty() {
        assertThat(catchThrowable(() -> offsetConfig("key").getOffsetPointers()))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void whenItemOffsetPointersConfigured_thenInitialized() {
        assertThat(offsetConfig("key=/value").getOffsetPointers()).isEqualTo(ImmutableMap.of("key", compile("/value")));
    }

    @Test
    void whenItemOffsetMultiplePointersConfigured_thenInitialized() {
        assertThat(offsetConfig("key=/value,key2=/value2").getOffsetPointers())
                .isEqualTo(ImmutableMap.of("key", compile("/value"), "key2", compile("/value2")));
    }

    @Test
    void whenItemOffsetMultiplePointersWithSpacesConfigured_thenInitialized() {
        assertThat(offsetConfig("  key  =  /value  ,  key2  =  /value2  ").getOffsetPointers())
                .isEqualTo(ImmutableMap.of("key", compile("/value"), "key2", compile("/value2")));
    }

    @Test
    void whenItemOffsetMoreValuesPointersLengthConfigured_thenInitialized() {
        assertThat(catchThrowable(() -> offsetConfig("key=/value,/value2"))).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void whenItemOffsetMoreKeysPointersLengthConfigured_thenInitialized() {
        assertThat(catchThrowable(() -> offsetConfig("key=/value,key2"))).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void whenItemPagingPointersHalfConfigured_thenEmpty() {
        assertThat(catchThrowable(() -> pagingConfig("key").getPagingNodePointers()))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void whenItemPagingMultiplePointersWithSpacesConfigured_thenInitialized() {
        assertThat(pagingConfig("  key  =  /value  ,  key2  =  /value2  ").getPagingNodePointers())
                .isEqualTo(ImmutableMap.of("key", compile("/value"), "key2", compile("/value2")));
    }

    @Test
    void whenItemPagingMoreValuesPointersLengthConfigured_thenInitialized() {
        assertThat(catchThrowable(() -> pagingConfig("key=/value,/value2"))).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void whenItemMetadataPointersHalfConfigured_thenEmpty() {
        assertThat(catchThrowable(() -> metadataConfig("key").getMetadataNodePointers()))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void whenItemMetadataMultiplePointersWithSpacesConfigured_thenInitialized() {
        assertThat(metadataConfig("  key  =  /value  ,  key2  =  /value2  ").getMetadataNodePointers())
                .isEqualTo(ImmutableMap.of("key", compile("/value"), "key2", compile("/value2")));
    }

    @Test
    void whenItemMetadataMoreValuesPointersLengthConfigured_thenInitialized() {
        assertThat(catchThrowable(() -> metadataConfig("key=/value,/value2")))
                .isInstanceOf(IllegalStateException.class);
    }

    interface Fixture {
        static JacksonRecordParserConfig config(String key, String value) {
            Map<String, String> customMap = new HashMap<>();
            customMap.put(key, value);
            return new JacksonRecordParserConfig(customMap);
        }

        static JacksonRecordParserConfig configWithout(String key) {
            Map<String, String> customMap = new HashMap<>();
            customMap.remove(key);
            return new JacksonRecordParserConfig(customMap);
        }

        static JacksonRecordParserConfig offsetConfig(String values) {
            Map<String, String> customMap = new HashMap<>();
            customMap.put(JacksonRecordParserConfig.ITEM_OFFSET_VALUE_POINTER, values);
            return new JacksonRecordParserConfig(customMap);
        }

        static JacksonRecordParserConfig pagingConfig(String values) {
            Map<String, String> customMap = new HashMap<>();
            customMap.put(JacksonRecordParserConfig.PAGING_POINTERS, values);
            return new JacksonRecordParserConfig(customMap);
        }

        static JacksonRecordParserConfig metadataConfig(String values) {
            Map<String, String> customMap = new HashMap<>();
            customMap.put(JacksonRecordParserConfig.METADATA_POINTERS, values);
            return new JacksonRecordParserConfig(customMap);
        }
    }
}
