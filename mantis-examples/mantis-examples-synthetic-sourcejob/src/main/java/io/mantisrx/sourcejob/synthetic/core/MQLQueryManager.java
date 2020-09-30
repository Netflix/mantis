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

package io.mantisrx.sourcejob.synthetic.core;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

import io.mantisrx.mql.jvm.core.Query;


public class MQLQueryManager {

    static class LazyHolder {

        private static final MQLQueryManager INSTANCE = new MQLQueryManager();
    }

    private ConcurrentHashMap<String, Query> queries = new ConcurrentHashMap<>();

    public static MQLQueryManager getInstance() {
        return LazyHolder.INSTANCE;
    }

    private MQLQueryManager() { }

    public void registerQuery(String id, String query) {
        query = MQL.transformLegacyQuery(query);
        Query q = MQL.makeQuery(id, query);
        queries.put(id, q);
    }

    public void deregisterQuery(String id) {
        queries.remove(id);
    }

    public Collection<Query> getRegisteredQueries() {
        return queries.values();
    }

    public void clear() {
        queries.clear();
    }

    public static void main(String[] args) throws Exception {
        MQLQueryManager qm = getInstance();
        String query = "SELECT * WHERE true SAMPLE {\"strategy\":\"RANDOM\",\"threshold\":1}";
        qm.registerQuery("fake2", query);
        System.out.println(MQL.parses(MQL.transformLegacyQuery(query)));
        System.out.println(MQL.getParseError(MQL.transformLegacyQuery(query)));
        System.out.println(qm.getRegisteredQueries());
    }
}


