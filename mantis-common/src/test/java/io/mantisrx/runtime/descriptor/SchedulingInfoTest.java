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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.junit.Assert.*;

public class SchedulingInfoTest {
    @Test
    public void shouldRequireInheritInstanceCheck() {
        SchedulingInfo.Builder builder = new SchedulingInfo.Builder().numberOfStages(1);
        builder.addStage(
                StageSchedulingInfo.builder().numberOfInstances(1).scalable(true).build());
        builder.addDeploymentStrategy(
                DeploymentStrategy.builder()
                        .stage(1,
                                new StageDeploymentStrategy.StageDeploymentStrategyBuilder().inheritInstanceCount(true).build())
                        .build());

        SchedulingInfo res = builder.build();
        assertTrue(res.requireInheritInstanceCheck());
        assertTrue(res.requireInheritInstanceCheck(1));
        assertFalse(res.requireInheritInstanceCheck(2));
        assertTrue(res.forStage(1).getScalable());

        // test multiple stages
        builder = new SchedulingInfo.Builder().numberOfStages(3);
        builder.addStage(StageSchedulingInfo.builder().numberOfInstances(1).build())
                .addStage(StageSchedulingInfo.builder().numberOfInstances(2).build())
                .addStage(StageSchedulingInfo.builder().numberOfInstances(2).build());

        builder.addDeploymentStrategy(
                DeploymentStrategy.builder()
                        .stage(2,
                                new StageDeploymentStrategy.StageDeploymentStrategyBuilder().inheritInstanceCount(true).build())
                        .stage(3,
                                new StageDeploymentStrategy.StageDeploymentStrategyBuilder().inheritInstanceCount(false).build())
                        .build());
        res = builder.build();
        assertTrue(res.requireInheritInstanceCheck());
        assertTrue(res.requireInheritInstanceCheck(2));
        assertFalse(res.requireInheritInstanceCheck(1));
        assertFalse(res.requireInheritInstanceCheck(3));
    }

    @Test
    public void shouldNotRequireInheritInstanceCheck() {
        SchedulingInfo.Builder builder = new SchedulingInfo.Builder().numberOfStages(1);
        builder.addStage(
                StageSchedulingInfo.builder().numberOfInstances(1).build());
        SchedulingInfo res = builder.build();
        assertFalse(res.requireInheritInstanceCheck(1));
        assertFalse(res.requireInheritInstanceCheck(2));
        assertFalse(res.requireInheritInstanceCheck());

        // test default setting
        builder = new SchedulingInfo.Builder().numberOfStages(1);
        builder.addStage(
                StageSchedulingInfo.builder().numberOfInstances(1).build());
        builder.addDeploymentStrategy(
                DeploymentStrategy.builder().build());
        res = builder.build();
        assertFalse(res.requireInheritInstanceCheck(1));
        assertFalse(res.requireInheritInstanceCheck(2));
        assertFalse(res.requireInheritInstanceCheck());

        // test multiple stages
        builder = new SchedulingInfo.Builder().numberOfStages(3);
        builder.addStage(StageSchedulingInfo.builder().numberOfInstances(1).build())
                .addStage(StageSchedulingInfo.builder().numberOfInstances(2).build())
                .addStage(StageSchedulingInfo.builder().numberOfInstances(2).build());
        builder.addDeploymentStrategy(
                DeploymentStrategy.builder()
                        .stage(1,
                                StageDeploymentStrategy.builder().build())
                        .stage(3,
                                StageDeploymentStrategy.builder().inheritInstanceCount(false).build())
                        .build());
        res = builder.build();
        assertFalse(res.requireInheritInstanceCheck(1));
        assertFalse(res.requireInheritInstanceCheck(2));
        assertFalse(res.requireInheritInstanceCheck());
    }

    @Test
    public void buildWithInheritInstanceTest() {
        Map<Integer, StageSchedulingInfo> givenStages = new HashMap<>();
        givenStages.put(1, StageSchedulingInfo.builder().numberOfInstances(1).build());
        SchedulingInfo.Builder builder = new SchedulingInfo.Builder()
                .createWithInstanceInheritance(
                        givenStages,
                        i -> empty(),
                        i -> false,
                        false);
        SchedulingInfo info = builder.build();
        assertEquals(1, info.getStages().size());
        assertEquals(1, info.forStage(1).getNumberOfInstances());

        givenStages.put(2, StageSchedulingInfo.builder().numberOfInstances(2).build());
        builder = new SchedulingInfo.Builder()
                .createWithInstanceInheritance(
                        givenStages,
                        i -> empty(),
                        i -> false,
                        false);
        info = builder.build();
        assertEquals(2, info.getStages().size());
        assertEquals(1, info.forStage(1).getNumberOfInstances());
        assertEquals(2, info.forStage(2).getNumberOfInstances());

        // test valid existing count + no inherit flag
        builder = new SchedulingInfo.Builder()
                .createWithInstanceInheritance(
                        givenStages,
                        i -> of(9),
                        i -> false,
                        false);
        info = builder.build();
        assertEquals(2, info.getStages().size());
        assertEquals(1, info.forStage(1).getNumberOfInstances());
        assertEquals(2, info.forStage(2).getNumberOfInstances());

        // test invalid existing count + inherit flag
        builder = new SchedulingInfo.Builder()
                .createWithInstanceInheritance(
                        givenStages,
                        i -> empty(),
                        i -> true,
                        false);
        info = builder.build();
        assertEquals(2, info.getStages().size());
        assertEquals(1, info.forStage(1).getNumberOfInstances());
        assertEquals(2, info.forStage(2).getNumberOfInstances());

        // test invalid existing count + force inherit flag
        builder = new SchedulingInfo.Builder()
                .createWithInstanceInheritance(
                        givenStages,
                        i -> empty(),
                        i -> false,
                        true);
        info = builder.build();
        assertEquals(2, info.getStages().size());
        assertEquals(1, info.forStage(1).getNumberOfInstances());
        assertEquals(2, info.forStage(2).getNumberOfInstances());

        // test valid existing count + inherit flag
        builder = new SchedulingInfo.Builder()
                .createWithInstanceInheritance(
                        givenStages,
                        i -> of(9),
                        i -> i == 1,
                        false);
        info = builder.build();
        assertEquals(2, info.getStages().size());
        assertEquals(9, info.forStage(1).getNumberOfInstances());
        assertEquals(2, info.forStage(2).getNumberOfInstances());

        // test valid existing count + force inherit flag
        builder = new SchedulingInfo.Builder()
                .createWithInstanceInheritance(
                        givenStages,
                        i -> of(9),
                        i -> false,
                        true);
        info = builder.build();
        assertEquals(2, info.getStages().size());
        assertEquals(9, info.forStage(1).getNumberOfInstances());
        assertEquals(9, info.forStage(2).getNumberOfInstances());

        // test valid existing count + both inherit flag
        builder = new SchedulingInfo.Builder()
                .createWithInstanceInheritance(
                        givenStages,
                        i -> of(9),
                        i -> true,
                        true);
        info = builder.build();
        assertEquals(2, info.getStages().size());
        assertEquals(9, info.forStage(1).getNumberOfInstances());
        assertEquals(9, info.forStage(2).getNumberOfInstances());

        // test job master
        givenStages.put(0, StageSchedulingInfo.builder().numberOfInstances(1).build());
        builder = new SchedulingInfo.Builder()
                .createWithInstanceInheritance(
                        givenStages,
                        i -> of(9),
                        i -> true,
                        true);
        info = builder.build();
        assertEquals(3, info.getStages().size());
        assertEquals(9, info.forStage(0).getNumberOfInstances());
        assertEquals(9, info.forStage(1).getNumberOfInstances());
        assertEquals(9, info.forStage(2).getNumberOfInstances());

    }
}
