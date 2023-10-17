/*
 * Copyright 2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.mantisrx.sourcejob.kafka;

import io.mantisrx.common.codec.Codec;
import io.mantisrx.connector.kafka.KafkaAckable;
import io.mantisrx.mql.jvm.core.Query;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.computation.ScalarComputation;
import io.mantisrx.sourcejob.kafka.core.TaggedData;
import io.mantisrx.sourcejob.kafka.sink.MQLQueryManager;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;


public abstract class AbstractAckableTaggingStage implements ScalarComputation<KafkaAckable, TaggedData> {

    public static final String MANTIS_META_IS_COMPLETE_DATA = "mantis.meta.isCompleteData";
    public static final String MANTIS_META_SOURCE_NAME = "mantis.meta.sourceName";
    public static final String MANTIS_META_SOURCE_TIMESTAMP = "mantis.meta.timestamp";
    public static final String MANTIS_QUERY_COUNTER = "mantis_query_out";
    public static final String MQL_COUNTER = "mql_out";
    public static final String MQL_FAILURE = "mql_failure";
    public static final String MQL_CLASSLOADER_ERROR = "mql_classloader_error";

    private static final Logger logger = LoggerFactory.getLogger(AbstractAckableTaggingStage.class);
    private static final String MANTIS_META = "mantis.meta";
    protected AtomicBoolean trackIsComplete = new AtomicBoolean(false);
    private AtomicBoolean errorLogged = new AtomicBoolean(false);

    public Observable<TaggedData> call(Context context, Observable<KafkaAckable> data) {
        context.getMeterRegistry().counter("mql_" + MQL_COUNTER);
        context.getMeterRegistry().counter("mql_" + MQL_FAILURE);
        context.getMeterRegistry().counter("mql_" + MQL_CLASSLOADER_ERROR);
        context.getMeterRegistry().counter("mql_" + MANTIS_QUERY_COUNTER);

        return data
            .map(ackable -> {
                Map<String, Object> rawData = processAndAck(context, ackable);
                return preProcess(rawData);
            })
            .filter((d) -> !d.isEmpty())
            .map(mapData -> applyPreMapping(context, mapData))
            .filter((d) -> !d.isEmpty())
            .flatMapIterable(d -> tagData(d, context));
    }

    protected abstract Map<String, Object> processAndAck(final Context context, KafkaAckable ackable);

    protected abstract Map<String, Object> preProcess(final Map<String, Object> rawData);

    protected Map<String, Object> applyPreMapping(final Context context, final Map<String, Object> rawData) {
        return rawData;
    }

    private boolean isMetaEvent(Map<String, Object> d) {
        return d.containsKey(MANTIS_META_IS_COMPLETE_DATA) || d.containsKey(MANTIS_META);
    }

    @SuppressWarnings("unchecked")
    protected List<TaggedData> tagData(Map<String, Object> d, Context context) {
        List<TaggedData> taggedDataList = new ArrayList<>();
        MeterRegistry meterRegistry = context.getMeterRegistry();
        boolean metaEvent = isMetaEvent(d);

        Collection<Query> queries = MQLQueryManager.getInstance().getRegisteredQueries();
        Iterator<Query> it = queries.iterator();
        while (it.hasNext()) {
            Query query = it.next();
            try {
                if (metaEvent) {
                    TaggedData tg = new TaggedData(d);
                    tg.addMatchedClient(query.getSubscriptionId());
                    taggedDataList.add(tg);
                } else if (query.matches(d)) {
                    Map<String, Object> projected = query.project(d);
                    projected.put(MANTIS_META_SOURCE_NAME, d.get(MANTIS_META_SOURCE_NAME));
                    projected.put(MANTIS_META_SOURCE_TIMESTAMP, d.get(MANTIS_META_SOURCE_TIMESTAMP));
                    TaggedData tg = new TaggedData(projected);
                    tg.addMatchedClient(query.getSubscriptionId());
                    taggedDataList.add(tg);
                }
            } catch (Exception ex) {
                if (ex instanceof ClassNotFoundException) {
                    logger.error("Error loading MQL: " + ex.getMessage());
                    ex.printStackTrace();

                } else {
                    ex.printStackTrace();
                    Counter mqlFailure = meterRegistry.find("mql_" + MQL_FAILURE).counter();
                    mqlFailure.increment();
                    logger.error("MQL Error: " + ex.getMessage());
                    logger.error("MQL Query: " + query.getRawQuery());
                    logger.error("MQL Datum: " + d);
                }
            } catch (Error e) {
                Counter mqlFailure = meterRegistry.find("mql_" + MQL_FAILURE).counter();
                mqlFailure.increment();
                if (!errorLogged.get()) {
                    logger.error("caught Error when processing MQL {} on {}", query.getRawQuery(), d.toString(), e);
                    errorLogged.set(true);
                }
            }
        }

        return taggedDataList;
    }

    public static Codec<TaggedData> taggedDataCodec() {

        return new Codec<TaggedData>() {
            @Override
            public TaggedData decode(byte[] bytes) {
                return new TaggedData(new HashMap<>());
            }

            @Override
            public byte[] encode(final TaggedData value) {
                return new byte[128];
            }
        };
    }

}
