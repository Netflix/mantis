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

package io.mantisrx.sourcejob.kafka.sink;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;


final class QueryRefCountMap {

    private static Logger logger = Logger.getLogger(QueryRefCountMap.class);
    public static final QueryRefCountMap INSTANCE = new QueryRefCountMap();
    private final ConcurrentHashMap<String, AtomicInteger> refCntMap = new ConcurrentHashMap<>();

    private QueryRefCountMap() {

    }

    void addQuery(String subId, String query) {
        logger.info("adding query " + subId + " query " + query);
        if (refCntMap.containsKey(subId)) {
            int newVal = refCntMap.get(subId).incrementAndGet();
            logger.info("query exists already incrementing refcnt to " + newVal);
        } else {
            MQLQueryManager.getInstance().registerQuery(subId, query);
            refCntMap.putIfAbsent(subId, new AtomicInteger(1));
            logger.info("new query registering it");
        }
    }

    void removeQuery(String subId) {
        if (refCntMap.containsKey(subId)) {
            AtomicInteger refCnt = refCntMap.get(subId);
            int currVal = refCnt.decrementAndGet();

            if (currVal == 0) {
                MQLQueryManager.getInstance().deregisterQuery(subId);
                refCntMap.remove(subId);
                logger.info("All references to query are gone removing query");
            } else {
                logger.info("References to query still exist. decremeting refcnt to " + currVal);
            }
        } else {
            logger.warn("No query with subscriptionId " + subId);
        }
    }

    public int getUniqueSubscriptionsCount() {
        return refCntMap.size();
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
