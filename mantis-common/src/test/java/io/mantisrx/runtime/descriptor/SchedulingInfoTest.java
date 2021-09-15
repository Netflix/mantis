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
