/*
 * Copyright 2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.mantisrx.connectors.publish.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.mantisrx.common.utils.MantisSourceJobConstants;
import org.apache.log4j.Logger;
import rx.functions.Func1;


public class EventFilter implements Func1<Map<String, List<String>>, Func1<String, Boolean>> {

    private static final Logger LOGGER = Logger.getLogger(EventFilter.class);

    private final String clientId;

    public EventFilter(String clientId) {
        ObjectUtils.checkNotNull("clientId", clientId);
        this.clientId = clientId;
    }

    @Override
    public Func1<String, Boolean> call(Map<String, List<String>> parameters) {
        Func1<String, Boolean> filter = t1 -> true;

        if (parameters != null) {
            if (parameters.containsKey(MantisSourceJobConstants.FILTER_PARAM_NAME)) {
                String filterBy = parameters.get(MantisSourceJobConstants.FILTER_PARAM_NAME).get(0);

                List<String> terms = convertCommaSeparatedEventsToList(filterBy);
                LOGGER.info("terms: " + terms);
                // Create filter function based on parameter value.
                filter = new SourceEventFilter(terms);
            } else if (parameters.containsKey(MantisSourceJobConstants.SUBSCRIPTION_ID_PARAM_NAME)) {
                String subId = parameters.get(MantisSourceJobConstants.SUBSCRIPTION_ID_PARAM_NAME).get(0);
                List<String> terms = new ArrayList<String>();
                terms.add(clientId + "_" + subId);
                filter = new SourceEventFilter(terms);
            }

            return filter;
        }

        return filter;
    }

    private List<String> convertCommaSeparatedEventsToList(String filterBy) {
        List<String> terms = new ArrayList<>();

        if (filterBy != null && !filterBy.isEmpty()) {
            terms = Arrays.asList(filterBy.split("\\s*,\\s*"));
        }

        return terms;
    }


    private static class SourceEventFilter implements Func1<String, Boolean> {

        private List<String> terms;

        public SourceEventFilter(List<String> terms) {
            this.terms = terms;
            LOGGER.info("Initiated with terms" + terms);
        }

        @Override
        public Boolean call(String data) {
            boolean match = true;

            if (data != null && !data.isEmpty()) {
                for (String term : terms) {
                    if (!data.contains(term)) {
                        match = false;
                        break;
                    }
                }
            } else {
                match = false;
            }
            return match;
        }
    }
}
