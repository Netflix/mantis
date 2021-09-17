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

package io.mantisrx.runtime.descriptor;

import org.junit.Test;

import static org.junit.Assert.*;

public class SchedulingInfoTest {
    @Test
    public void shouldRequireInheritInstanceCheck() {
        SchedulingInfo.Builder builder = new SchedulingInfo.Builder().numberOfStages(1);
        builder.addStage(
                new StageSchedulingInfo.Builder().setNumberOfInstances(1).setInheritInstanceCount(true).build());
        assertTrue(builder.build().requireInheritInstanceCheck());

        // test multiple stages
        builder = new SchedulingInfo.Builder().numberOfStages(3);
        builder.addStage(new StageSchedulingInfo.Builder().setNumberOfInstances(1).build())
                .addStage(new StageSchedulingInfo.Builder().setNumberOfInstances(2).setInheritInstanceCount(true).build())
                .addStage(new StageSchedulingInfo.Builder().setNumberOfInstances(2).setInheritInstanceCount(false).build());
        assertTrue(builder.build().requireInheritInstanceCheck());
    }

    @Test
    public void shouldNotRequireInheritInstanceCheck() {
        SchedulingInfo.Builder builder = new SchedulingInfo.Builder().numberOfStages(1);
        builder.addStage(
                new StageSchedulingInfo.Builder().setNumberOfInstances(1).setInheritInstanceCount(false).build());
        assertFalse(builder.build().requireInheritInstanceCheck());

        // test default setting
        builder = new SchedulingInfo.Builder().numberOfStages(1);
        builder.addStage(
                new StageSchedulingInfo.Builder().setNumberOfInstances(1).build());
        assertFalse(builder.build().requireInheritInstanceCheck());

        // test multiple stages
        builder = new SchedulingInfo.Builder().numberOfStages(3);
        builder.addStage(new StageSchedulingInfo.Builder().setNumberOfInstances(1).build())
                .addStage(new StageSchedulingInfo.Builder().setNumberOfInstances(2).setInheritInstanceCount(false).build())
                .addStage(new StageSchedulingInfo.Builder().setNumberOfInstances(2).setInheritInstanceCount(false).build());
        assertFalse(builder.build().requireInheritInstanceCheck());
    }
}
