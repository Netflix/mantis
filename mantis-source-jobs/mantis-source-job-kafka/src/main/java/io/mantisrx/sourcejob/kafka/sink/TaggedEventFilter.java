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

import io.mantisrx.common.MantisProperties;
import io.mantisrx.sourcejob.kafka.core.TaggedData;
import io.mantisrx.sourcejob.kafka.core.utils.SourceJobConstants;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import rx.functions.Func1;


public class TaggedEventFilter implements Func1<Map<String, List<String>>, Func1<TaggedData, Boolean>> {

    private static Logger logger = Logger.getLogger(TaggedEventFilter.class);

    @Override
    public Func1<TaggedData, Boolean> call(Map<String, List<String>> parameters) {
        Func1<TaggedData, Boolean> filter = t1 -> true;
        if (parameters != null) {
            if (parameters.containsKey(SourceJobConstants.SUBSCRIPTION_ID_PARAM_NAME)) {
                String subId = parameters.get(SourceJobConstants.SUBSCRIPTION_ID_PARAM_NAME).get(0);
                String clientId = parameters.get(SourceJobConstants.CLIENT_ID_PARAMETER_NAME).get(0);
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
            logger.info("Created SourceEventFilter! for subId " + terms.toString() + " in Job : " + jobName + " with Id " + jobId);
        }

        @Override
        public Boolean call(TaggedData data) {
            //      DynamicCounter.increment("SourceEventFilterCount", "kind","processed","mantisJobId",jobId,"subId",terms.toString());
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
