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

package io.mantisrx.publish.internal.mql;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.function.Function;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import io.mantisrx.mql.core.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MQL {

    private static final Logger LOG = LoggerFactory.getLogger(MQL.class);
    private static IFn require = Clojure.var("clojure.core", "require");
    private static IFn cljMakeQuery = Clojure.var("io.mantisrx.mql.components", "make-query");
    private static IFn cljSuperset = Clojure.var("io.mantisrx.mql.core", "queries->superset-projection");

    static {
        require.invoke(Clojure.read("io.mantisrx.mql.core"));
        require.invoke(Clojure.read("io.mantisrx.mql.components"));
    }

    public static void init() {
        LOG.info("Initializing MQL Runtime.");
    }

    public static Query query(String subscriptionId, String query) {
        return (Query) cljMakeQuery.invoke(subscriptionId, query.trim());
    }

    @SuppressWarnings("unchecked")
    public static Function<Map<String, Object>, Map<String, Object>> makeSupersetProjector(
            HashSet<Query> queries) {
        ArrayList<String> qs = new ArrayList<>(queries.size());
        for (Query query : queries) {
            qs.add(query.getRawQuery());
        }

        IFn ssProjector = (IFn) cljSuperset.invoke(new ArrayList(qs));
        return (datum) -> (Map<String, Object>) (ssProjector.invoke(datum));
    }

    public static String preprocess(String criterion) {
        return criterion.toLowerCase().equals("true") ? "select * where true" :
                criterion.toLowerCase().equals("false") ? "select * where false" :
                        criterion;
    }

    public static boolean isContradictionQuery(String query) {
        return query.equals("false") ||
                query.equals("select * where false") ||
                query.equals("select * from stream where false");
    }
}
