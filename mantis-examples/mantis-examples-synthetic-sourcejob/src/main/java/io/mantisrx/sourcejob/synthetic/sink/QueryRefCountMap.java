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

package io.mantisrx.sourcejob.synthetic.sink;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import io.mantisrx.sourcejob.synthetic.core.MQLQueryManager;
import lombok.extern.slf4j.Slf4j;


/**
 * This class keeps track of number of clients that have the exact same query registered for
 * deduplication purposes.
 * When all references to a query are gone the query is deregistered.
 */
@Slf4j
final class QueryRefCountMap {

    public static final QueryRefCountMap INSTANCE = new QueryRefCountMap();
    private final ConcurrentHashMap<String, AtomicInteger> refCntMap = new ConcurrentHashMap<>();

    private QueryRefCountMap() { }

    void addQuery(String subId, String query) {
        log.info("adding query " + subId + " query " + query);
        if (refCntMap.containsKey(subId)) {
            int newVal = refCntMap.get(subId).incrementAndGet();
            log.info("query exists already incrementing refcnt to " + newVal);
        } else {
            MQLQueryManager.getInstance().registerQuery(subId, query);
            refCntMap.putIfAbsent(subId, new AtomicInteger(1));
            log.info("new query registering it");
        }
    }

    void removeQuery(String subId) {
        if (refCntMap.containsKey(subId)) {
            AtomicInteger refCnt = refCntMap.get(subId);
            int currVal = refCnt.decrementAndGet();

            if (currVal == 0) {
                MQLQueryManager.getInstance().deregisterQuery(subId);
                refCntMap.remove(subId);
                log.info("All references to query are gone removing query");
            } else {
                log.info("References to query still exist. decrementing refcnt to " + currVal);
            }
        } else {
            log.warn("No query with subscriptionId " + subId);
        }
    }

    /**
     * For testing
     *
     * @param subId
     *
     * @return
     */
    int getQueryRefCount(String subId) {
        if (refCntMap.containsKey(subId)) {
            return refCntMap.get(subId).get();
        } else {
            return 0;
        }
    }

}
