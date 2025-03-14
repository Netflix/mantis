package io.mantisrx.master.jobcluster.scaler;

import io.mantisrx.master.jobcluster.proto.JobClusterScalerRuleProto;
import io.mantisrx.runtime.descriptor.StageScalingPolicy;

import java.util.*;

import io.mantisrx.runtime.descriptor.JobScalingRule;
import io.mantisrx.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import io.mantisrx.shaded.com.google.common.base.Strings;
import io.mantisrx.shaded.com.google.common.collect.ImmutableList;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import org.junit.Test;

import static org.junit.Assert.*;

public class JobClusterScalerRuleDataImplWritableTest {

    static Map<StageScalingPolicy.ScalingReason, StageScalingPolicy.Strategy> smap = new HashMap<>();
    static StageScalingPolicy scalingPolicy;
    static ObjectMapper mapper = new ObjectMapper();

    private JobClusterScalerRuleProto.CreateScalerRuleRequest createDummyRequest(String jobClusterName, int desireSize) {
        smap.put(StageScalingPolicy.ScalingReason.CPU, new StageScalingPolicy.Strategy(StageScalingPolicy.ScalingReason.CPU, 0.5, 0.75, null));
        smap.put(StageScalingPolicy.ScalingReason.DataDrop, new StageScalingPolicy.Strategy(StageScalingPolicy.ScalingReason.DataDrop, 0.0, 2.0, null));
        scalingPolicy = new StageScalingPolicy(1, 1, 2, 1, 1, 60, smap, false);


        JobScalingRule.ScalerConfig scalerConfig =
            JobScalingRule.ScalerConfig.builder()
                .type("standard")
                .stageConfigMap(ImmutableMap.of("1", JobScalingRule.StageScalerConfig.builder()
                        .desireSize(desireSize)
                        .scalingPolicy(scalingPolicy)
                        .build()))
                .build();

        JobScalingRule.TriggerConfig triggerConfig =
            JobScalingRule.TriggerConfig.builder()
                .triggerType("cron")
                .scheduleCron("0 0 * * *")
                .scheduleDuration("PT1H")
                .customTrigger("none")
                .build();

        Map<String, String> metadata = new HashMap<>();
        metadata.put("key", "value");

        return JobClusterScalerRuleProto.CreateScalerRuleRequest.builder()
            .jobClusterName(jobClusterName)
            .scalerConfig(scalerConfig)
            .triggerConfig(triggerConfig)
            .metadata(metadata)
            .build();
    }

    @Test
    public void testMergeAddsNewRuleAndIncrementsRuleId() throws JsonProcessingException {
        // Create an initial data instance using the provided "of" method.
        String clusterName = "testCluster";
        IJobClusterScalerRuleData data = JobClusterScalerRuleDataImplWritable.of(clusterName);

        // Before merge: no rules and lastRuleIdNumber is 0.
        List<JobScalingRule> protoRulesBefore = data.getProtoRules();
        assertNotNull(protoRulesBefore);
        assertTrue(protoRulesBefore.isEmpty());

        // Create a dummy scaler rule request.
        JobClusterScalerRuleProto.CreateScalerRuleRequest req = createDummyRequest(clusterName, 10);

        // Merge the new rule.
        IJobClusterScalerRuleData mergedData = data.merge(req);

        String jsonMergedData = mapper.writeValueAsString(mergedData);
        assertFalse(Strings.isNullOrEmpty(jsonMergedData));
        JobClusterScalerRuleDataImplWritable deserRules = mapper.readValue(
            jsonMergedData, JobClusterScalerRuleDataImplWritable.class);
        assertEquals(mergedData, deserRules);

        // Validate that the new instance has lastRuleIdNumber incremented.
        assertTrue(mergedData instanceof JobClusterScalerRuleDataImplWritable);
        JobClusterScalerRuleDataImplWritable writable =
            (JobClusterScalerRuleDataImplWritable) mergedData;
        assertEquals(clusterName, writable.getJobClusterName());
        assertEquals(1, writable.getLastRuleIdNumber());
        assertFalse(writable.isDisabled());
        assertEquals(1, writable.getScalerRules().size());

        // Check that the ruleId in the new rule is "1"
        JobClusterScalerRule newRule = writable.getScalerRules().get(0);
        assertEquals("1", newRule.getRule().getRuleId());

        // Also test that toProtoRules returns the correct data.
        List<JobScalingRule> protoRulesAfter = mergedData.getProtoRules();
        assertNotNull(protoRulesAfter);
        assertEquals(1, protoRulesAfter.size());
    }

    @Test
    public void testDeleteRemovesExistingRuleAndKeepsOthersUnchanged() {
        // Create an initial data instance with two rules.
        String clusterName = "testCluster";
        IJobClusterScalerRuleData data = JobClusterScalerRuleDataImplWritable.of(clusterName);
        JobClusterScalerRuleProto.CreateScalerRuleRequest req1 = createDummyRequest(clusterName, 10);
        JobClusterScalerRuleProto.CreateScalerRuleRequest req2 = createDummyRequest(clusterName, 20);

        IJobClusterScalerRuleData dataAfterFirstMerge = data.merge(req1); // adds rule with id "1"
        IJobClusterScalerRuleData dataAfterSecondMerge = dataAfterFirstMerge.merge(req2); // adds rule with id "2"

        JobClusterScalerRuleDataImplWritable writable =
            (JobClusterScalerRuleDataImplWritable) dataAfterSecondMerge;
        assertEquals(2, writable.getScalerRules().size());

        // Delete the rule with id "1"
        IJobClusterScalerRuleData afterDelete = dataAfterSecondMerge.delete("1");
        JobClusterScalerRuleDataImplWritable writableAfterDelete =
            (JobClusterScalerRuleDataImplWritable) afterDelete;
        List<JobClusterScalerRule> remainingRules = writableAfterDelete.getScalerRules();
        assertEquals(1, remainingRules.size());
        // The remaining rule should have ruleId "2"
        assertEquals("2", remainingRules.get(0).getRule().getRuleId());

        // Deleting a non-existing rule should return the same instance.
        IJobClusterScalerRuleData unchanged = afterDelete.delete("non-existing");
        assertSame(afterDelete, unchanged);
    }

    @Test
    public void testToProtoRulesConversion() {
        // Create an instance with one merged rule.
        String clusterName = "testCluster";
        IJobClusterScalerRuleData data = JobClusterScalerRuleDataImplWritable.of(clusterName);
        JobClusterScalerRuleProto.CreateScalerRuleRequest req = createDummyRequest(clusterName, 15);
        IJobClusterScalerRuleData mergedData = data.merge(req);

        // Convert to proto and verify that the returned list is not null and has expected size.
        List<JobScalingRule> protoRules = mergedData.getProtoRules();
        assertNotNull(protoRules);
        assertEquals(1, protoRules.size());

        JobScalingRule protoRule = protoRules.get(0);
        // Validate that the protoRule contains the expected fields (scalerConfig, triggerConfig, metadata)
        assertNotNull(protoRule.getScalerConfig());
        assertEquals("standard", protoRule.getScalerConfig().getType());
        assertEquals((Integer)15, protoRule.getScalerConfig().getStageConfigMap().get("1").getDesireSize());
        assertEquals(scalingPolicy, protoRule.getScalerConfig().getStageConfigMap().get("1").getScalingPolicy());

        assertNotNull(protoRule.getTriggerConfig());
        assertEquals("cron", protoRule.getTriggerConfig().getTriggerType());
        assertEquals("0 0 * * *", protoRule.getTriggerConfig().getScheduleCron());
        assertEquals("PT1H", protoRule.getTriggerConfig().getScheduleDuration());
        assertEquals("none", protoRule.getTriggerConfig().getCustomTrigger());
        assertEquals("1", protoRule.getRuleId());

        assertNotNull(protoRule.getMetadata());
        assertEquals("value", protoRule.getMetadata().get("key"));
    }
}
