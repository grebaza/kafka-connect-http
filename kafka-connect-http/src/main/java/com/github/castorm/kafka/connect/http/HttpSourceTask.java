package com.github.castorm.kafka.connect.http;

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

import static com.github.castorm.kafka.connect.common.CollectionUtils.merge;
import static com.github.castorm.kafka.connect.common.VersionUtils.getVersion;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import com.github.castorm.kafka.connect.http.ack.ConfirmationWindow;
import com.github.castorm.kafka.connect.http.client.spi.HttpClient;
import com.github.castorm.kafka.connect.http.model.HttpRequest;
import com.github.castorm.kafka.connect.http.model.HttpResponse;
import com.github.castorm.kafka.connect.http.model.Offset;
import com.github.castorm.kafka.connect.http.model.ParsedResponse;
import com.github.castorm.kafka.connect.http.model.RequestInput;
import com.github.castorm.kafka.connect.http.record.spi.SourceRecordFilterFactory;
import com.github.castorm.kafka.connect.http.record.spi.SourceRecordSorter;
import com.github.castorm.kafka.connect.http.request.spi.HttpRequestFactory;
import com.github.castorm.kafka.connect.http.response.spi.HttpResponseParser;
import com.github.castorm.kafka.connect.timer.TimerThrottler;
import edu.emory.mathcs.backport.java.util.Collections;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

@Slf4j
public class HttpSourceTask extends SourceTask {
    private static final String RECORD_0_KEY = "rec1_offset";

    private final Function<Map<String, String>, HttpSourceConnectorConfig> configFactory;

    private TimerThrottler throttler;

    private HttpRequestFactory requestFactory;

    private HttpClient requestExecutor;

    private HttpResponseParser responseParser;

    private SourceRecordSorter recordSorter;

    private SourceRecordFilterFactory recordFilterFactory;

    private volatile ConfirmationWindow<Map<String, ?>> confirmationWindow = new ConfirmationWindow<>(emptyList());

    protected static enum State {
        RUNNING,
        STOPPED;
    }

    private final AtomicReference<State> state = new AtomicReference<State>(State.STOPPED);

    /**
     * used to ensure that start(), stop() and commit() calls are serialized.
     */
    private final ReentrantLock stateLock = new ReentrantLock();

    /**
     * confirmed record offset
     */
    @Getter
    private volatile Offset offset;

    private ParsedResponse parsedResponse = ParsedResponse.of();

    private volatile String taskName;

    HttpSourceTask(Function<Map<String, String>, HttpSourceConnectorConfig> configFactory) {
        this.configFactory = configFactory;
    }

    public HttpSourceTask() {
        this(HttpSourceConnectorConfig::new);
    }

    @Override
    public void start(Map<String, String> settings) {
        stateLock.lock();

        try {
            taskName = context == null ? null : context.configs().get("name");
            if (!state.compareAndSet(State.STOPPED, State.RUNNING)) {
                log.info("Task {} Connector has already been started", taskName);
                return;
            }
            HttpSourceConnectorConfig config = configFactory.apply(settings);

            throttler = config.getThrottler();
            requestFactory = config.getRequestFactory();
            requestExecutor = config.getClient();
            responseParser = config.getResponseParser();
            recordSorter = config.getRecordSorter();
            recordFilterFactory = config.getRecordFilterFactory();
            offset = loadOffset(config.getInitialOffset());
        } finally {
            stateLock.unlock();
        }
    }

    private Offset loadOffset(Map<String, String> initialOffset) {
        Map<String, Object> restoredOffset =
                ofNullable(context.offsetStorageReader().offset(emptyMap())).orElseGet(Collections::emptyMap);
        return Offset.of(!restoredOffset.isEmpty() ? restoredOffset : initialOffset);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {

        throttler.throttle(offset.getTimestamp().orElseGet(Instant::now));

        RequestInput requestInput = createRequestInputFromConfirmedOffsetAndParsedResponse();

        HttpRequest request = requestFactory.createRequest(requestInput);

        HttpResponse response = execute(request);

        parsedResponse = responseParser.parse(response);

        log.debug("Task {} parsed response {}", taskName, parsedResponse);

        final List<SourceRecord> unseenRecords = recordSorter.sort(parsedResponse.getRecords()).stream()
                .filter(recordFilterFactory.create(offset))
                .collect(toList());

        log.info(
                "Task {} request {} yields {}/{} new records and paging {}",
                taskName,
                requestInput,
                unseenRecords.size(),
                parsedResponse.getRecords().size(),
                parsedResponse.getPaging());

        confirmationWindow = new ConfirmationWindow<>(extractOffsets(unseenRecords));

        return unseenRecords;
    }

    private HttpResponse execute(HttpRequest request) {
        try {
            return requestExecutor.execute(request);
        } catch (IOException e) {
            throw new RetriableException(e);
        }
    }

    private RequestInput createRequestInputFromConfirmedOffsetAndParsedResponse() {
        final Map<String, ?> metadata;

        if (!parsedResponse.getRecords().isEmpty()) {
            metadata = merge((Map<String, Object>) parsedResponse.getMetadata(), (Map<String, Object>)
                    Stream.of(parsedResponse.getRecords().get(0).sourceOffset())
                            .collect(toMap(i -> RECORD_0_KEY, i -> (Object) i, (i1, i2) -> i1)));
        } else {
            metadata = parsedResponse.getMetadata();
        }

        return RequestInput.of(offset, parsedResponse.getPaging(), metadata);
    }

    private static List<Map<String, ?>> extractOffsets(List<SourceRecord> recordsToSend) {
        return recordsToSend.stream().map(SourceRecord::sourceOffset).collect(toList());
    }

    @Override
    public void commitRecord(SourceRecord record, RecordMetadata metadata) {
        confirmationWindow.confirm(record.sourceOffset());
    }

    @Override
    public void commit() {
        boolean locked = stateLock.tryLock();

        if (locked) {
            try {
                offset = confirmationWindow
                        .getLowWatermarkOffset()
                        .map(Offset::of)
                        .orElse(offset);
                log.debug("Task {} Offset set to {}", taskName, offset);
            } finally {
                stateLock.unlock();
            }
        } else {
            log.warn(
                    "Task {} couldn't commit processed log positions with the "
                            + "source http due to a concurrent connector shutdown or restart",
                    taskName);
        }
    }

    @Override
    public void stop() {
        stateLock.lock();

        try {
            if (!state.compareAndSet(State.RUNNING, State.STOPPED)) {
                log.info("Task {} Connector has already been stopped", taskName);
                return;
            }
            log.info("Task {} Stopping down connector", taskName);
        } finally {
            stateLock.unlock();
        }
    }

    @Override
    public String version() {
        return getVersion();
    }
}
