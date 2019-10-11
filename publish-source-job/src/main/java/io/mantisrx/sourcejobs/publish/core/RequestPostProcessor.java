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

package io.mantisrx.sourcejobs.publish.core;


import java.util.List;
import java.util.Map;

import com.mantisrx.common.utils.MantisSourceJobConstants;
import io.mantisrx.connector.publish.core.QueryRegistry;
import io.mantisrx.runtime.Context;
import org.apache.log4j.Logger;
import rx.functions.Func2;


public class RequestPostProcessor implements Func2<Map<String, List<String>>, Context, Void> {

    private static final Logger LOGGER = Logger.getLogger(RequestPostProcessor.class);

    private final QueryRegistry queryRegistry;

    public RequestPostProcessor(QueryRegistry queryRegistry) {
        this.queryRegistry = queryRegistry;
    }

    @Override
    public Void call(Map<String, List<String>> queryParams, Context context) {
        LOGGER.info("RequestPostProcessor:queryParams: " + queryParams);

        if (queryParams != null) {
            if (queryParams.containsKey(MantisSourceJobConstants.SUBSCRIPTION_ID_PARAM_NAME) && queryParams.containsKey(MantisSourceJobConstants.CRITERION_PARAM_NAME)) {
                final String subId = queryParams.get(MantisSourceJobConstants.SUBSCRIPTION_ID_PARAM_NAME).get(0);
                final String query = queryParams.get(MantisSourceJobConstants.CRITERION_PARAM_NAME).get(0);
                String targetApp = queryParams.containsKey(MantisSourceJobConstants.TARGET_APP_NAME_KEY) ? queryParams.get(MantisSourceJobConstants.TARGET_APP_NAME_KEY).get(0) : QueryRegistry.ANY;

                if (subId != null && query != null) {
                    queryRegistry.deregisterQuery(targetApp, subId, query);
                }
            }
        }

        return null;
    }
}
