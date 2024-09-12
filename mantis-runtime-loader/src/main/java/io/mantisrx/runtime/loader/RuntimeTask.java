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

package io.mantisrx.runtime.loader;

import io.mantisrx.shaded.com.google.common.util.concurrent.Service;
import org.apache.flink.util.UserCodeClassLoader;

/**
 * Interface to load core runtime work load from external host (TaskExecutor).
 * [Note] To avoid dependency conflicts in TaskExecutors running in custom structure
 * e.g. SpringBoot based runtime, adding dependency here shall be carefully reviewed.
 * Do not have any shared Mantis library that will be used on both TaskExecutor and implementations of
 * RuntimeTask in here and use primitive types in general.
 */
public interface RuntimeTask extends Service {
    void initialize(
        String executeStageRequestString,
        String workerConfigurationString,
        UserCodeClassLoader userCodeClassLoader);

    String getWorkerId();

}
