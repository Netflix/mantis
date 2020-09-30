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

package io.mantisrx.sourcejob.synthetic.source;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.parameter.ParameterDefinition;
import io.mantisrx.runtime.parameter.type.IntParameter;
import io.mantisrx.runtime.parameter.validator.Validators;
import io.mantisrx.runtime.source.Index;
import io.mantisrx.runtime.source.Source;
import io.mantisrx.sourcejob.synthetic.proto.RequestEvent;
import lombok.extern.slf4j.Slf4j;
import net.andreinc.mockneat.MockNeat;
import rx.Observable;


/**
 * Generates random set of RequestEvents at a preconfigured interval.
 */
@Slf4j
public class SyntheticSource implements Source<String> {

    private static final String DATA_GENERATION_RATE_MSEC_PARAM = "dataGenerationRate";
    private MockNeat mockDataGenerator;
    private int dataGenerateRateMsec = 250;

    @Override
    public Observable<Observable<String>> call(Context context, Index index) {
        return Observable.just(Observable
                .interval(dataGenerateRateMsec, TimeUnit.MILLISECONDS)
                .map((tick) -> generateEvent())
                .map((event) -> event.toJsonString())
                .filter(Objects::nonNull)
                .doOnNext((event) -> {
                    log.debug("Generated Event {}", event);
                }));
    }

    @Override
    public void init(Context context, Index index) {
        mockDataGenerator = MockNeat.threadLocal();
        dataGenerateRateMsec = (int)context.getParameters().get(DATA_GENERATION_RATE_MSEC_PARAM,250);
    }
    @Override
    public List<ParameterDefinition<?>> getParameters() {
        List<ParameterDefinition<?>> params = new ArrayList<>();
        params.add(new IntParameter()
                .name(DATA_GENERATION_RATE_MSEC_PARAM)
                .description("Rate at which to generate data")
                .validator(Validators.range(100,1000000))
                .defaultValue(250)
                .build());

        return params;
    }

    private RequestEvent generateEvent() {

        String path = mockDataGenerator.probabilites(String.class)
                .add(0.1, "/login")
                .add(0.2, "/genre/horror")
                .add(0.5, "/genre/comedy")
                .add(0.2, "/mylist")
                .get();

        String deviceType = mockDataGenerator.probabilites(String.class)
                .add(0.1, "ps4")
                .add(0.1, "xbox")
                .add(0.2, "browser")
                .add(0.3, "ios")
                .add(0.3, "android")
                .get();
        String userId = mockDataGenerator.strings().size(10).get();
        int status = mockDataGenerator.probabilites(Integer.class)
                                .add(0.1,500)
                                .add(0.7,200)
                                .add(0.2,500)
                                .get();


        String country = mockDataGenerator.countries().names().get();
        return RequestEvent.builder()
                .status(status)
                .uri(path)
                .country(country)
                .userId(userId)
                .deviceType(deviceType)
                .build();
    }

}
