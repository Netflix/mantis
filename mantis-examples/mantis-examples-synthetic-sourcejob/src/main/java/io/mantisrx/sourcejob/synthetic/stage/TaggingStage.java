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

package io.mantisrx.sourcejob.synthetic.stage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import com.mantisrx.common.utils.JsonUtility;
import io.mantisrx.common.codec.Codec;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.spectator.MetricGroupId;
import io.mantisrx.mql.jvm.core.Query;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.ScalarToScalar;
import io.mantisrx.runtime.computation.ScalarComputation;
import io.mantisrx.sourcejob.synthetic.core.MQLQueryManager;
import io.mantisrx.sourcejob.synthetic.core.TaggedData;
import lombok.extern.slf4j.Slf4j;
import rx.Observable;


/**
 * Tags incoming events with ids of queries that evaluate to true against the data.
 *
 * Each event is first transformed into a Map, next each query from the list of Registered MQL queries
 * is applied against the event. The event is tagged with the ids of queries that matched.
 *
 */
@Slf4j
public class TaggingStage  implements ScalarComputation<String, TaggedData> {

    public static final String MANTIS_META_SOURCE_NAME = "mantis.meta.sourceName";
    public static final String MANTIS_META_SOURCE_TIMESTAMP = "mantis.meta.timestamp";
    public static final String MANTIS_QUERY_COUNTER = "mantis_query_out";
    public static final String MQL_COUNTER = "mql_out";
    public static final String MQL_FAILURE = "mql_failure";
    public static final String MQL_CLASSLOADER_ERROR = "mql_classloader_error";
    public static final String SYNTHETIC_REQUEST_SOURCE = "SyntheticRequestSource";

    private AtomicBoolean errorLogged = new AtomicBoolean(false);
    @Override
    public Observable<TaggedData> call(Context context, Observable<String> dataO) {
        return dataO
                .map((event) -> {
                    try {
                        return JsonUtility.jsonToMap(event);
                    } catch (Exception e) {
                        log.error(e.getMessage());
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .flatMapIterable(d -> tagData(d, context));
    }

    @Override
    public void init(Context context) {

        context.getMetricsRegistry().registerAndGet(new Metrics.Builder()
                .name("mql")
                .addCounter(MQL_COUNTER)
                .addCounter(MQL_FAILURE)
                .addCounter(MQL_CLASSLOADER_ERROR)
                .addCounter(MANTIS_QUERY_COUNTER).build());
    }


    private List<TaggedData> tagData(Map<String, Object> d, Context context) {
        List<TaggedData> taggedDataList = new ArrayList<>();

        Metrics metrics = context.getMetricsRegistry().getMetric(new MetricGroupId("mql"));

        Collection<Query> queries = MQLQueryManager.getInstance().getRegisteredQueries();
        Iterator<Query> it = queries.iterator();
        while (it.hasNext()) {
            Query query = it.next();
            try {
                if (query.matches(d)) {
                    Map<String, Object> projected = query.project(d);
                    projected.put(MANTIS_META_SOURCE_NAME, SYNTHETIC_REQUEST_SOURCE);
                    projected.put(MANTIS_META_SOURCE_TIMESTAMP, System.currentTimeMillis());
                    TaggedData tg = new TaggedData(projected);
                    tg.addMatchedClient(query.getSubscriptionId());
                    taggedDataList.add(tg);
                }
            } catch (Exception ex) {
                if (ex instanceof ClassNotFoundException) {
                    log.error("Error loading MQL: " + ex.getMessage());
                    ex.printStackTrace();
                    metrics.getCounter(MQL_CLASSLOADER_ERROR).increment();
                } else {
                    ex.printStackTrace();
                    metrics.getCounter(MQL_FAILURE).increment();
                    log.error("MQL Error: " + ex.getMessage());
                    log.error("MQL Query: " + query.getRawQuery());
                    log.error("MQL Datum: " + d);
                }
            } catch (Error e) {
                metrics.getCounter(MQL_FAILURE).increment();
                if (!errorLogged.get()) {
                    log.error("caught Error when processing MQL {} on {}", query.getRawQuery(), d.toString(), e);
                    errorLogged.set(true);
                }
            }
        }

        return taggedDataList;
    }

    public static ScalarToScalar.Config<String, TaggedData> config() {
        return new ScalarToScalar.Config<String, TaggedData>()
                .concurrentInput()
                .codec(TaggingStage.taggedDataCodec());
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
