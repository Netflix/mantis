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
import org.apache.log4j.Logger;
import rx.functions.Func2;


/**
 * This is a callback that is invoked after a client connected to the sink of this job disconnects. This is used
 * to cleanup the queries the client had registered.
 */
@Slf4j
public class QueryRequestPostProcessor implements Func2<Map<String, List<String>>, Context, Void> {

    public QueryRequestPostProcessor() { }

    @Override
    public Void call(Map<String, List<String>> queryParams, Context context) {
        log.info("RequestPostProcessor:queryParams: " + queryParams);

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
                    } catch (Throwable t) {
                        log.error("Error propagating unsubscription notification", t);
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
