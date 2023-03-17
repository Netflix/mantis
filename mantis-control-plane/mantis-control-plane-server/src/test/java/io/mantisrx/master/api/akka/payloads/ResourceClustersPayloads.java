/*
 * Copyright 2022 Netflix, Inc.
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

package io.mantisrx.master.api.akka.payloads;


import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import java.util.Map;

public class ResourceClustersPayloads {
    public static final String CLUSTER_ID = "mantisResourceClusterUT1";

    public static final String RESOURCE_CLUSTER_CREATE =
        PayloadUtils.getStringFromResource("testpayloads/ResourceClusterCreate.json");

    public static final String RESOURCE_CLUSTER_SCALE_RESULT =
        PayloadUtils.getStringFromResource("testpayloads/ResourceClusterScaleResult.json");

    public static final String RESOURCE_CLUSTER_SCALE_RULES_CREATE =
        PayloadUtils.getStringFromResource("testpayloads/ResourceClusterScaleRulesCreate.json");

    public static final String RESOURCE_CLUSTER_SINGLE_SCALE_RULE_CREATE =
        PayloadUtils.getStringFromResource("testpayloads/ResourceClusterScaleRuleCreate.json");

    public static final String RESOURCE_CLUSTER_SCALE_RULES_RESULT =
        PayloadUtils.getStringFromResource("testpayloads/ResourceClusterScaleRulesResult.json");

    public static final String RESOURCE_CLUSTER_SKU_SCALE = "{\"clusterId\":{\"resourceID\": "
        + "\"mantisResourceClusterUT1\"},"
            + "\"skuId\":{\"resourceID\": \"small\"},\"region\":\"us-east-1\",\"envType\":\"Prod\","
        + "\"desireSize\":11}\n";

    public static final String RESOURCE_CLUSTER_DISABLE_TASK_EXECUTORS_PAYLOAD = "" +
        "{\n" +
        "  \"expirationDurationInHours\": 19,\n" +
        "  \"attributes\": {\n" +
        "    \"attr1\": \"attr1\"\n" +
        "  }\n" +
        "}";

    public static final Map<String, String> RESOURCE_CLUSTER_DISABLE_TASK_EXECUTORS_ATTRS =
        ImmutableMap.of("attr1", "attr1");
}
