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

package com.netflix.control.clutch;

import com.google.common.util.concurrent.AtomicDouble;
import com.netflix.control.IActuator;
import com.netflix.control.controllers.ControlLoop;
import io.vavr.Tuple;
import org.junit.Test;
import rx.Observable;

import static org.mockito.Mockito.*;

import java.util.concurrent.TimeUnit;


public class ControlLoopTest {



    @Test public void shouldRemainInSteadyState() {

        IActuator actuator = mock(IActuator.class);

        ClutchConfiguration config = ClutchConfiguration.builder()
                .cooldownInterval(10L)
                .cooldownUnits(TimeUnit.MILLISECONDS)
                .kd(0.01)
                .kp(0.01)
                .kd(0.01)
                .maxSize(10)
                .minSize(3)
                .rope(Tuple.of(25.0, 0.0))
                .setPoint(0.6)
                .build();

        // define return value for method getUniqueId()
        //when(test.getUniqueId()).thenReturn(43);

        ControlLoop loop = new ControlLoop(config, actuator, 5.0, new AtomicDouble(1.0));

        // TODO: Run some fake data through and verify behavior.
    }
}
