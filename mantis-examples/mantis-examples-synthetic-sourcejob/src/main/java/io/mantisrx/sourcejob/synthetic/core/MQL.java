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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import io.mantisrx.mql.jvm.core.Query;
import io.mantisrx.mql.shaded.clojure.java.api.Clojure;
import io.mantisrx.mql.shaded.clojure.lang.IFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Func1;


/**
 * The MQL class provides a Java/Scala friendly static interface to MQL functionality which is written in Clojure.
 * This class provides a few pieces of functionality;
 * - It wraps the Clojure interop so that the user interacts with typed methods via the static interface.
 * - It provides methods for accessing individual bits of query functionality, allowing interesting uses
 * such as aggregator-mql which uses these components to implement the query in a horizontally scalable / distributed
 * fashion on Mantis.
 * - It functions as an Rx Transformer of MantisServerSentEvent to MQLResult allowing a user to inline all MQL
 * functionality quickly as such: `myObservable.compose(MQL.parse(myQuery));`
 */
public class MQL {

    //
    // Clojure Interop
    //

    private static IFn require = Clojure.var("io.mantisrx.mql.shaded.clojure.core", "require");

    static {
        require.invoke(Clojure.read("io.mantisrx.mql.jvm.interfaces.core"));
        require.invoke(Clojure.read("io.mantisrx.mql.jvm.interfaces.server"));
    }

    private static IFn cljMakeQuery = Clojure.var("io.mantisrx.mql.jvm.interfaces.server", "make-query");
    private static IFn cljSuperset = Clojure.var("io.mantisrx.mql.jvm.interfaces.core", "queries->superset-projection");
    private static IFn parser = Clojure.var("io.mantisrx.mql.jvm.interfaces.core", "parser");
    private static IFn parses = Clojure.var("io.mantisrx.mql.jvm.interfaces.core", "parses?");
    private static IFn getParseError = Clojure.var("io.mantisrx.mql.jvm.interfaces.core", "get-parse-error");
    private static IFn queryToGroupByFn = Clojure.var("io.mantisrx.mql.jvm.interfaces.core", "query->groupby");
    private static IFn queryToHavingPred = Clojure.var("io.mantisrx.mql.jvm.interfaces.core", "query->having-pred");
    private static IFn queryToOrderBy = Clojure.var("io.mantisrx.mql.jvm.interfaces.core", "query->orderby");
    private static IFn queryToLimit = Clojure.var("io.mantisrx.mql.jvm.interfaces.core", "query->limit");
    private static IFn queryToExtrapolationFn = Clojure.var("io.mantisrx.mql.jvm.interfaces.core", "query->extrapolator");
    private static IFn queryToAggregateFn = Clojure.var("io.mantisrx.mql.jvm.interfaces.core", "agg-query->projection");
    private static IFn queryToWindow = Clojure.var("io.mantisrx.mql.jvm.interfaces.core", "query->window");


    private static Logger logger = LoggerFactory.getLogger(MQL.class);

    private static ConcurrentHashMap<HashSet<Query>, IFn> superSetProjectorCache = new ConcurrentHashMap<>();

    private final String query;
    private final boolean threadingEnabled;
    private final Optional<String> sourceJobName;

    public static void init() {
        logger.info("Initializing MQL runtime.");
    }

    //
    // Constructors and Static Factory Methods
    //

    public MQL(String query, boolean threadingEnabled) {
        if (query == null) {
            throw new IllegalArgumentException("MQL cannot be used as an operator with a null query.");
        }
        this.query = transformLegacyQuery(query);
        if (!parses(query)) {
            throw new IllegalArgumentException(getParseError(query));
        }

        this.threadingEnabled = threadingEnabled;
        this.sourceJobName = Optional.empty();
    }

    public MQL(String query, String sourceJobName) {
        if (query == null) {
            throw new IllegalArgumentException("MQL cannot be used as an operator with a null query.");
        }
        this.query = transformLegacyQuery(query);
        if (!parses(query)) {
            throw new IllegalArgumentException(getParseError(query));
        }

        this.threadingEnabled = false;
        this.sourceJobName = Optional.ofNullable(sourceJobName);
    }

    public static MQL parse(String query) {
        return new MQL(query, false);
    }

    public static MQL parse(String query, boolean threadingEnabled) { return new MQL(query, threadingEnabled); }

    public static MQL parse(String query, String sourceName) { return new MQL(query, sourceName); }

    //
    // Source Job Integration
    //

    /**
     * Constructs an object implementing the Query interface.
     * This includes functions;
     * matches (Map<String, Object>>) -> Boolean
     * Returns true iff the data contained within the map parameter satisfies the query's WHERE clause.
     * project (Map<String, Object>>) -> Map<String, Object>>
     * Returns the provided map in accordance with the SELECT clause of the query.
     * sample  (Map<String, Object>>) -> Boolean
     * Returns true if the data should be sampled, this function is a tautology if no SAMPLE clause is provided.
     *
     * @param subscriptionId The ID representing the subscription.
     * @param query          The (valid) MQL query to parse.
     *
     * @return An object implementing the Query interface.
     */
    public static Query makeQuery(String subscriptionId, String query) {
        /*
        if (!parses(query)) {
            String error = getParseError(query);
            logger.error("Failed to parse query [" + query + "]\nError: " + error + ".");
            throw new IllegalArgumentException(error);
        }
         */
        return (Query) cljMakeQuery.invoke(subscriptionId, query.trim());
    }

    @SuppressWarnings("unchecked")
    private static IFn computeSuperSetProjector(HashSet<Query> queries) {
        ArrayList<String> qs = new ArrayList<>(queries.size());
        for (Query query : queries) {
            qs.add(query.getRawQuery());
        }
        return (IFn) cljSuperset.invoke(new ArrayList(qs));
    }

    /**
     * Projects a single Map<String, Object> which contains a superset of all fields for the provided queries.
     * This is useful in use cases such as the mantis-realtime-events library in which we desire to minimize the data
     * egressed off box. This should minimize JSON serialization time as well as network bandwidth used to transmit
     * the events.
     * <p>
     * NOTE: This function caches the projectors for performance reasons, this has implications for memory usage as each
     * combination of queries results in a new cached function. In practice this has had little impact for <= 100
     * queries.
     *
     * @param queries A Collection of Query objects generated using #makeQuery(String subscriptionId, String query).
     * @param datum   A Map representing the input event to be projected.
     *
     * @return A Map representing the union (superset) of all fields required for processing all queries passed in.
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> projectSuperSet(Collection<Query> queries, Map<String, Object> datum) {
        IFn superSetProjector = superSetProjectorCache.computeIfAbsent(new HashSet<Query>(queries), (qs) -> {
            return computeSuperSetProjector(qs);
        });
        return (Map<String, Object>) superSetProjector.invoke(datum);
    }

    //
    // Partial Query Functionality
    //

    public static Func1<Map<String, Object>, Object> getGroupByFn(String query) {
        IFn func = (IFn) queryToGroupByFn.invoke(query);
        return func::invoke;
    }

    @SuppressWarnings("unchecked")
    public static Func1<Map<String, Object>, Boolean> getHavingPredicate(String query) {
        IFn func = (IFn) queryToHavingPred.invoke(query);
        return (datum) -> (Boolean) func.invoke(datum);
    }

    @SuppressWarnings("unchecked")
    public static Func1<Observable<Map<String, Object>>, Observable<Map<String, Object>>> getAggregateFn(String query) {
        IFn func = (IFn) queryToAggregateFn.invoke(query);
        return (obs) -> (Observable<Map<String, Object>>) func.invoke(obs);
    }

    @SuppressWarnings("unchecked")
    public static Func1<Map<String, Object>, Map<String, Object>> getExtrapolationFn(String query) {
        IFn func = (IFn) queryToExtrapolationFn.invoke(query);
        return (datum) -> (Map<String, Object>) func.invoke(datum);
    }

    @SuppressWarnings("unchecked")
    public static Func1<Observable<Map<String, Object>>, Observable<Map<String, Object>>> getOrderBy(String query) {
        IFn func = (IFn) queryToOrderBy.invoke(query);
        return obs -> (Observable<Map<String, Object>>) func.invoke(obs);
    }


    //    public static List<Long> getWindow(String query) {
    //        clojure.lang.PersistentVector result = (clojure.lang.PersistentVector)queryToWindow.invoke(query);
    //        Long window = (Long)result.nth(0);
    //        Long shift = (Long)result.nth(1);
    //        return Arrays.asList(window, shift);
    //    }

    public static Long getLimit(String query) {
        return (Long) queryToLimit.invoke(query);
    }

    //
    // Helper Functions
    //

    /**
     * A predicate which indicates whether or not the MQL parser considers query to be a valid query.
     *
     * @param query A String representing the MQL query.
     *
     * @return A boolean indicating whether or not the query successfully parses.
     */
    public static Boolean parses(String query) {
        return (Boolean) parses.invoke(query);
    }

    /**
     * A convenience function allowing a caller to determine what went wrong if a call to #parses(String query) returns
     * false.
     *
     * @param query A String representing the MQL query.
     *
     * @return A String representing the parse error for an MQL query, null if no parse error occurred.
     */
    public static String getParseError(String query) {
        return (String) getParseError.invoke(query);
    }

    /**
     * A helper which converts bare true/false queries to MQL.
     *
     * @param criterion A Mantis Query (old query language) query.
     *
     * @return A valid MQL query string assuming the input was valid.
     */
    public static String transformLegacyQuery(String criterion) {
        return criterion.toLowerCase().equals("true") ? "select * where true" :
            criterion.toLowerCase().equals("false") ? "select * where false" :
                criterion;
    }

    public static void main(String[] args) {
        System.out.println(MQL.makeQuery("abc", "select * from stream where true"));
    }
}

