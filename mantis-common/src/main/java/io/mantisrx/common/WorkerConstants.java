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

package io.mantisrx.common;

import java.util.regex.Pattern;

public class WorkerConstants {
    public static final String WORKER_CONTAINER_DEFINITION_ID = "MANTIS_WORKER_CONTAINER_DEFINITION_ID";
    public static final String WORKER_TASK_ATTRIBUTE_ENV_KEY = "MANTIS_WORKER_CONTAINER_ATTRIBUTE";
    // TODO(fdichiara): make this configurable.
    public static final String AUTO_SCALE_GROUP_KEY = "NETFLIX_AUTO_SCALE_GROUP";
    public static final String MANTIS_WORKER_CONTAINER_GENERATION = "MANTIS_WORKER_CONTAINER_GENERATION";
    public static final Pattern MANTIS_SCHEDULING_ATTRIBUTE_PATTERN = Pattern.compile("^(?i)MANTIS_SCHEDULING_ATTRIBUTE_(.*)$");
    public static final String MANTIS_CONTAINER_SIZE_NAME_KEY = "MANTIS_WORKER_CONTAINER_SIZE_NAME";
}
