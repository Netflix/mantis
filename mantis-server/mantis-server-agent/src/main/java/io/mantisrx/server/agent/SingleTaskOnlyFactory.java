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

package io.mantisrx.server.agent;

import io.mantisrx.runtime.loader.ClassLoaderHandle;
import io.mantisrx.runtime.loader.RuntimeTask;
import io.mantisrx.runtime.loader.TaskFactory;
import io.mantisrx.server.core.ExecuteStageRequest;
import java.util.ServiceLoader;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.util.UserCodeClassLoader;

/**
 * This factory is used when there is only 1 RuntimeTask implementation.
 */
@Slf4j
public class SingleTaskOnlyFactory implements TaskFactory {

    @Override
    public RuntimeTask getRuntimeTaskInstance(ExecuteStageRequest request, ClassLoader cl) {
        ServiceLoader<RuntimeTask> loader = ServiceLoader.load(RuntimeTask.class, cl);
        return loader.iterator().next();
    }

    @Override
    public UserCodeClassLoader getUserCodeClassLoader(
        ExecuteStageRequest request,
        ClassLoaderHandle classLoaderHandle) {
        try {
            return classLoaderHandle.createUserCodeClassloader(request);
        } catch (Exception ex) {
            log.error("Failed to submit task, request: {}", request, ex);
            throw new RuntimeException(ex);
        }
    }
}
