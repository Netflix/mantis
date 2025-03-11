package io.mantisrx.runtime.descriptor;


import io.mantisrx.shaded.com.fasterxml.jackson.databind.DeserializationFeature;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.*;


public class JobScalingRuleTest {

    private final ObjectMapper objectMapper = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Test
    public void jobScalingRuleSerialization() throws Exception {
        JobScalingRule.ScalerConfig scalerConfig = JobScalingRule.ScalerConfig.builder()
                .type("standard")
                .stageConfigMap(
                    Collections.singletonMap("1", JobScalingRule.StageScalerConfig.builder().desireSize(10).build()))
                .build();

        JobScalingRule.TriggerConfig triggerConfig = JobScalingRule.TriggerConfig.builder()
                .triggerType(JobScalingRule.TRIGGER_TYPE_SCHEDULE)
                .scheduleCron("0 0 * * *")
                .scheduleDuration("1h")
                .customTrigger(null)
                .build();

        JobScalingRule jobScalingRule = JobScalingRule.builder()
                .ruleId("1")
                .scalerConfig(scalerConfig)
                .triggerConfig(triggerConfig)
                .metadata(Collections.singletonMap("key", "value"))
                .build();

        String json = objectMapper.writeValueAsString(jobScalingRule);
        assertNotNull(json);
    }

    @Test
    public void jobScalingRuleDeserialization() throws Exception {
        String json = "{\"ruleId\":\"1\",\"scalerConfig\":{\"type\":\"standard\",\"scalingPolicies\":[],\"stageDesireSize\":{\"1\":10}},\"triggerConfig\":{\"triggerType\":\"schedule\",\"scheduleCron\":\"0 0 * * *\",\"scheduleDuration\":\"1h\",\"customTrigger\":null},\"metadata\":{\"key\":\"value\"}}";

        JobScalingRule jobScalingRule = objectMapper.readValue(json, JobScalingRule.class);
        assertNotNull(jobScalingRule);
        assertEquals("1", jobScalingRule.getRuleId());
        assertEquals("standard", jobScalingRule.getScalerConfig().getType());
        assertEquals(JobScalingRule.TRIGGER_TYPE_SCHEDULE, jobScalingRule.getTriggerConfig().getTriggerType());
        assertEquals("0 0 * * *", jobScalingRule.getTriggerConfig().getScheduleCron());
        assertEquals("1h", jobScalingRule.getTriggerConfig().getScheduleDuration());
        assertEquals("value", jobScalingRule.getMetadata().get("key"));
    }

    @Test
    public void jobScalingRuleDeserializationWithUnknownProperties() throws Exception {
        String json = "{\"ruleId\":\"1\",\"scalerConfig\":{\"type\":\"standard\",\"scalingPolicies\":[],\"stageDesireSize\":{\"1\":10},\"unknownProperty\":\"value\"},\"triggerConfig\":{\"triggerType\":\"schedule\",\"scheduleCron\":\"0 0 * * *\",\"scheduleDuration\":\"1h\",\"customTrigger\":null,\"unknownProperty\":\"value\"},\"metadata\":{\"key\":\"value\"},\"unknownProperty\":\"value\"}";

        JobScalingRule jobScalingRule = objectMapper.readValue(json, JobScalingRule.class);
        assertNotNull(jobScalingRule);
        assertEquals("1", jobScalingRule.getRuleId());
        assertEquals("standard", jobScalingRule.getScalerConfig().getType());
        assertEquals(JobScalingRule.TRIGGER_TYPE_SCHEDULE, jobScalingRule.getTriggerConfig().getTriggerType());
        assertEquals("0 0 * * *", jobScalingRule.getTriggerConfig().getScheduleCron());
        assertEquals("1h", jobScalingRule.getTriggerConfig().getScheduleDuration());
        assertEquals("value", jobScalingRule.getMetadata().get("key"));
    }

    @Test
    public void jobScalingRuleSerializationWithEmptyScalingPolicies() throws Exception {
        JobScalingRule.ScalerConfig scalerConfig = JobScalingRule.ScalerConfig.builder()
                .type("standard")
                .stageConfigMap(
                    Collections.singletonMap("1", JobScalingRule.StageScalerConfig.builder().desireSize(10).build()))
                .build();

        JobScalingRule.TriggerConfig triggerConfig = JobScalingRule.TriggerConfig.builder()
                .triggerType(JobScalingRule.TRIGGER_TYPE_SCHEDULE)
                .scheduleCron("0 0 * * *")
                .scheduleDuration("1h")
                .customTrigger(null)
                .build();

        JobScalingRule jobScalingRule = JobScalingRule.builder()
                .ruleId("1")
                .scalerConfig(scalerConfig)
                .triggerConfig(triggerConfig)
                .metadata(Collections.singletonMap("key", "value"))
                .build();

        String json = objectMapper.writeValueAsString(jobScalingRule);
        assertNotNull(json);

        JobScalingRule deserializedJobScalingRule = objectMapper.readValue(json, JobScalingRule.class);
        assertNotNull(deserializedJobScalingRule);
        assertEquals("1", deserializedJobScalingRule.getRuleId());
        assertEquals("standard", deserializedJobScalingRule.getScalerConfig().getType());
        assertEquals(1, deserializedJobScalingRule.getScalerConfig().getStageConfigMap().size());
        assertEquals((Integer)10,
            deserializedJobScalingRule.getScalerConfig().getStageConfigMap().get("1").getDesireSize());
        assertNull(deserializedJobScalingRule.getScalerConfig().getStageConfigMap().get("1").getScalingPolicy());

        assertEquals(JobScalingRule.TRIGGER_TYPE_SCHEDULE,
            deserializedJobScalingRule.getTriggerConfig().getTriggerType());
        assertEquals("0 0 * * *", deserializedJobScalingRule.getTriggerConfig().getScheduleCron());
        assertEquals("1h", deserializedJobScalingRule.getTriggerConfig().getScheduleDuration());
        assertEquals("value", deserializedJobScalingRule.getMetadata().get("key"));
    }
}
