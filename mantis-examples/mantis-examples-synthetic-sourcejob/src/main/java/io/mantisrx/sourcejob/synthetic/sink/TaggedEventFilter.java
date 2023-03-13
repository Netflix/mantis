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

import static com.mantisrx.common.utils.MantisSourceJobConstants.CLIENT_ID_PARAMETER_NAME;
import static com.mantisrx.common.utils.MantisSourceJobConstants.SUBSCRIPTION_ID_PARAM_NAME;

import io.mantisrx.common.MantisProperties;
import io.mantisrx.sourcejob.synthetic.core.TaggedData;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import rx.functions.Func1;


/**
 * This is a predicate that decides what data to send to the downstream client. The data is tagged with the clientId
 * and subscriptionId of the intended recipient.
 */
@Slf4j
public class TaggedEventFilter implements Func1<Map<String, List<String>>, Func1<TaggedData, Boolean>> {

    @Override
    public Func1<TaggedData, Boolean> call(Map<String, List<String>> parameters) {
        Func1<TaggedData, Boolean> filter = t1 -> true;
        if (parameters != null) {
            if (parameters.containsKey(SUBSCRIPTION_ID_PARAM_NAME)) {
                String subId = parameters.get(SUBSCRIPTION_ID_PARAM_NAME).get(0);
                String clientId = parameters.get(CLIENT_ID_PARAMETER_NAME).get(0);
                List<String> terms = new ArrayList<String>();
                if (clientId != null && !clientId.isEmpty()) {
                    terms.add(clientId + "_" + subId);
                } else {
                    terms.add(subId);
                }
                filter = new SourceEventFilter(terms);
            }
            return filter;
        }
        return filter;
    }

    private static class SourceEventFilter implements Func1<TaggedData, Boolean> {

        private String jobId = "UNKNOWN";
        private String jobName = "UNKNOWN";
        private List<String> terms;

        SourceEventFilter(List<String> terms) {
            this.terms = terms;
            String jId = MantisProperties.getProperty("JOB_ID");
            if (jId != null && !jId.isEmpty()) {
                jobId = jId;
            }
            String jName = MantisProperties.getProperty("JOB_NAME");
            if (jName != null && !jName.isEmpty()) {
                jobName = jName;
            }
            log.info("Created SourceEventFilter! for subId " + terms.toString() + " in Job : " + jobName + " with Id " + jobId);
        }

        @Override
        public Boolean call(TaggedData data) {

            boolean match = true;
            for (String term : terms) {
                if (!data.matchesClient(term)) {
                    match = false;
                    break;
                }
            }
            return match;
        }
    }
}
