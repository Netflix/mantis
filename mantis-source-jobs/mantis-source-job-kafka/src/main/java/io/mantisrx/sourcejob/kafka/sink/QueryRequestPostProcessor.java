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

import static io.mantisrx.sourcejob.kafka.core.utils.SourceJobConstants.CRITERION_PARAM_NAME;
import static io.mantisrx.sourcejob.kafka.core.utils.SourceJobConstants.SUBSCRIPTION_ID_PARAM_NAME;

import java.util.List;
import java.util.Map;

import io.mantisrx.runtime.Context;
import org.apache.log4j.Logger;
import rx.functions.Func2;


public class QueryRequestPostProcessor implements Func2<Map<String, List<String>>, Context, Void> {

    private static Logger logger = Logger.getLogger(QueryRequestPostProcessor.class);

    public QueryRequestPostProcessor() { }

    @Override
    public Void call(Map<String, List<String>> queryParams, Context context) {

        logger.info("RequestPostProcessor:queryParams: " + queryParams);

        if (queryParams != null) {

            if (queryParams.containsKey(SUBSCRIPTION_ID_PARAM_NAME) && queryParams.containsKey(CRITERION_PARAM_NAME)) {
                final String subId = queryParams.get(SUBSCRIPTION_ID_PARAM_NAME).get(0);
                final String query = queryParams.get(CRITERION_PARAM_NAME).get(0);
                final String clientId = queryParams.get("clientId").get(0);

                if (subId != null && query != null) {
                    try {
                        if (clientId != null && !clientId.isEmpty()) {
                            deregisterQuery(clientId + "_" + subId);
                        } else {
                            deregisterQuery(subId);
                        }
                        // TODO - DynamicGauge.set("activeQueries", BasicTagList.of("mantisJobId", context.getJobId(),
                        //  "mantisJobName",context.getWorkerInfo().getJobName()), (double) MQLQueryManager.getInstance().getRegisteredQueries().size());
                    } catch (Throwable t) {
                        logger.error("Error propagating unsubscription notification", t);
                    }
                }
            }
        }
        return null;
    }

    private void deregisterQuery(String subId) {
        QueryRefCountMap.INSTANCE.removeQuery(subId);
    }
}
