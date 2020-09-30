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

import static com.mantisrx.common.utils.MantisSourceJobConstants.CRITERION_PARAM_NAME;
import static com.mantisrx.common.utils.MantisSourceJobConstants.SUBSCRIPTION_ID_PARAM_NAME;

import java.util.List;
import java.util.Map;

import io.mantisrx.runtime.Context;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.functions.Func2;


/**
 * This is a callback that is invoked when a new client connects to this sink of this job.
 * The callback is used to extract useful query parameters the user may have set on the GET request such as
 * the clientId, subscriptionId and the criterion.
 * The clientId identifies a group of connections belonging to the same consumer. Data is sent round-robin amongst
 * all clients with the same clientId
 * The subscriptionId tracks this particular client.
 * The criterion is a valid MQL query. It indicates what data this client is interested in.
 */
@Slf4j
public class QueryRequestPreProcessor implements Func2<Map<String, List<String>>, Context, Void> {

    public QueryRequestPreProcessor() { }

    @Override
    public Void call(Map<String, List<String>> queryParams, Context context) {

        log.info("QueryRequestPreProcessor:queryParams: {}", queryParams);

        if (queryParams != null) {

            if (queryParams.containsKey(SUBSCRIPTION_ID_PARAM_NAME) && queryParams.containsKey(CRITERION_PARAM_NAME)) {
                final String subId = queryParams.get(SUBSCRIPTION_ID_PARAM_NAME).get(0);
                final String query = queryParams.get(CRITERION_PARAM_NAME).get(0);
                final String clientId = queryParams.get("clientId").get(0);

                if (subId != null && query != null) {
                    try {
                        log.info("Registering query {}", query);
                        if (clientId != null && !clientId.isEmpty()) {
                            registerQuery(clientId + "_" + subId, query);
                        } else {
                            registerQuery(subId, query);
                        }

                    } catch (Throwable t) {
                        log.error("Error registering query", t);
                    }
                }
            }
        }
        return null;
    }

    private static synchronized void registerQuery(String subId, String query) {
        QueryRefCountMap.INSTANCE.addQuery(subId, query);
    }
}
