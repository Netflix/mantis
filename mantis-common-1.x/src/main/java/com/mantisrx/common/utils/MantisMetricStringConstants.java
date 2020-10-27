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

package com.mantisrx.common.utils;

import io.reactivx.mantis.operators.DropOperator;


public class MantisMetricStringConstants {

    public static final String INCOMING = "incoming";
    public static final String DROP_OPERATOR_INCOMING_METRIC_GROUP = String.format("%s_%s", DropOperator.METRIC_GROUP, INCOMING);
    public static final String GROUP_ID_TAG = "groupId";
}
