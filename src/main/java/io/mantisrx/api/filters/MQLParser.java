/*
 * Copyright 2020 Netflix, Inc.
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

package io.mantisrx.api.filters;

import io.mantisrx.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.zuul.filters.http.HttpSyncEndpoint;
import com.netflix.zuul.message.http.HttpRequestMessage;
import com.netflix.zuul.message.http.HttpResponseMessage;
import com.netflix.zuul.message.http.HttpResponseMessageImpl;
import io.mantisrx.mql.shaded.clojure.java.api.Clojure;
import io.mantisrx.mql.shaded.clojure.lang.IFn;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.Charset;


@Slf4j
public class MQLParser extends HttpSyncEndpoint {

    private static IFn require = Clojure.var("io.mantisrx.mql.shaded.clojure.core", "require");
    static {
        require.invoke(Clojure.read("io.mantisrx.mql.core"));
        require.invoke(Clojure.read("io.mantisrx.mql.jvm.interfaces.server"));
        require.invoke(Clojure.read("io.mantisrx.mql.jvm.interfaces.core"));
    }
    private static IFn parses = Clojure.var("io.mantisrx.mql.jvm.interfaces.core", "parses?");
    private static IFn getParseError = Clojure.var("io.mantisrx.mql.jvm.interfaces.core", "get-parse-error");

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public @Value class MQLParseResult {
        private boolean success;
        private String criterion;
        private String message;
    }

    @Override
    public HttpResponseMessage apply(HttpRequestMessage input) {
        String query = input.getQueryParams().getFirst("criterion");

        boolean parses = parses(query);
        String parseError = getParseError(query);
        MQLParseResult result = new MQLParseResult(parses, query, parses ? "" : parseError);

        try {
            HttpResponseMessage response = new HttpResponseMessageImpl(input.getContext(), input, 200);
            response.setBody(objectMapper.writeValueAsBytes(result));
            return response;

        } catch (JsonProcessingException ex) {
            HttpResponseMessage response = new HttpResponseMessageImpl(input.getContext(), input, 500);
            response.setBody(getErrorResponse(ex.getMessage()).getBytes(Charset.defaultCharset()));
            return response;
        }
    }

    /**
     * A predicate which indicates whether or not the MQL parser considers query to be a valid query.
     * @param query A String representing the MQL query.
     * @return A boolean indicating whether or not the query successfully parses.
     */
    public static Boolean parses(String query) {
        return (Boolean) parses.invoke(query);
    }

    /**
     * A convenience function allowing a caller to determine what went wrong if a call to #parses(String query) returns
     * false.
     * @param query A String representing the MQL query.
     * @return A String representing the parse error for an MQL query, null if no parse error occurred.
     */
    public static String getParseError(String query) {
        return (String) getParseError.invoke(query);
    }

    private String getErrorResponse(String exceptionMessage) {
        StringBuilder sb = new StringBuilder(50);
        sb.append("{\"success\": false, \"messages\": \"");
        sb.append(exceptionMessage);
        sb.append("\"}");
        return sb.toString();
    }
}
