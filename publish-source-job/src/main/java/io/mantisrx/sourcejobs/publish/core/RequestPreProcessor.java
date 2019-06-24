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


import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.mantisrx.common.utils.MantisSourceJobConstants;
import io.mantisrx.connectors.publish.core.QueryRegistry;
import io.mantisrx.runtime.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.functions.Func2;


public class RequestPreProcessor implements Func2<Map<String, List<String>>, Context, Void> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RequestPreProcessor.class);

    private final QueryRegistry queryRegistry;
    private final Map<String, String> emptyMap = new HashMap<String, String>();

    public RequestPreProcessor(QueryRegistry queryRegistry) {
        this.queryRegistry = queryRegistry;
    }

    @Override
    public Void call(Map<String, List<String>> queryParams, Context context) {
        LOGGER.info("RequestPreProcessor:queryParams: " + queryParams);

        if (queryParams != null) {
            if (queryParams.containsKey(MantisSourceJobConstants.SUBSCRIPTION_ID_PARAM_NAME) && queryParams.containsKey(MantisSourceJobConstants.CRITERION_PARAM_NAME)) {
                final String subId = queryParams.get(MantisSourceJobConstants.SUBSCRIPTION_ID_PARAM_NAME).get(0);
                final String query = queryParams.get(MantisSourceJobConstants.CRITERION_PARAM_NAME).get(0);
                String targetApp = queryParams.containsKey(MantisSourceJobConstants.TARGET_APP_NAME_KEY) ? queryParams.get(MantisSourceJobConstants.TARGET_APP_NAME_KEY).get(0) : QueryRegistry.ANY;

                if (subId != null && query != null) {
                    queryRegistry.registerQuery(targetApp, subId, query, emptyMap, false);
                }
            }
        }

        return null;
    }
}
