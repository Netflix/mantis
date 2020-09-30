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

package com.netflix.mantis.samples.stage;

import java.util.ArrayList;
import java.util.List;

import com.netflix.mantis.samples.proto.RequestEvent;
import io.mantisrx.common.MantisGroup;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.ScalarToGroup;
import io.mantisrx.runtime.computation.ToGroupComputation;
import io.mantisrx.runtime.parameter.ParameterDefinition;
import io.mantisrx.runtime.parameter.type.StringParameter;
import io.mantisrx.runtime.parameter.validator.Validators;
import lombok.extern.slf4j.Slf4j;
import rx.Observable;


/**
 * This is the first stage of this 3 stage job. It is at the head of the computation DAG
 * This stage converts Scalar Events to Grouped Events {@link ToGroupComputation}. The grouped events are then
 * send to the next stage of the Mantis Job in a way such that all events belonging to a particular group will
 * land on the same worker of the next stage.
 *
 * It receives a stream of {@link RequestEvent} and groups them by either the path or the IP address
 * based on the parameters passed by the user.
 */
@Slf4j
public class GroupByStage implements ToGroupComputation<RequestEvent, String, RequestEvent> {

    private static final String GROUPBY_FIELD_PARAM = "groupByField";
    private boolean groupByPath = true;
    @Override
    public Observable<MantisGroup<String, RequestEvent>> call(Context context, Observable<RequestEvent> requestEventO) {
        return requestEventO
                .map((requestEvent) -> {
                    if(groupByPath) {
                        return new MantisGroup<>(requestEvent.getRequestPath(), requestEvent);
                    } else {
                        return new MantisGroup<>(requestEvent.getIpAddress(), requestEvent);
                    }
                });
    }

    @Override
    public void init(Context context) {
        String groupByField = (String)context.getParameters().get(GROUPBY_FIELD_PARAM,"path");
        groupByPath = groupByField.equalsIgnoreCase("path") ? true : false;
    }

    /**
     * Here we declare stage specific parameters.
     * @return
     */
    public static List<ParameterDefinition<?>> getParameters() {
        List<ParameterDefinition<?>> params = new ArrayList<>();

        // Group by field
        params.add(new StringParameter()
                .name(GROUPBY_FIELD_PARAM)
                .description("The key to group events by")
                .validator(Validators.notNullOrEmpty())
                .defaultValue("path")
                .build())	;

        return params;
    }

    public static ScalarToGroup.Config<RequestEvent, String, RequestEvent> config(){
        return new ScalarToGroup.Config<RequestEvent, String, RequestEvent>()
                .description("Group event data by path/ip")
                .concurrentInput() // signifies events can be processed parallely
                .withParameters(getParameters())
                .codec(RequestEvent.requestEventCodec());
    }

}
