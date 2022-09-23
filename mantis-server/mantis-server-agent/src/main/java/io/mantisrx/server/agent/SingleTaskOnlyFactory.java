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

import io.mantisrx.server.core.ExecuteStageRequest;
import io.mantisrx.server.master.client.ClassLoaderHandle;
import io.mantisrx.server.master.client.ITask;
import io.mantisrx.server.master.client.TaskFactory;
import java.util.ServiceLoader;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.util.UserCodeClassLoader;

@Slf4j
public class SingleTaskOnlyFactory implements TaskFactory {

    @Override
    public ITask getITaskInstance(ExecuteStageRequest request, ClassLoader cl) {
        ServiceLoader<ITask> loader = ServiceLoader.load(ITask.class, cl);
        // This factory is used when only 1 task implementation provided by mantis-server-worker.
        return loader.iterator().next();
    }

    @Override
    public UserCodeClassLoader getUserCodeClassLoader(
        ExecuteStageRequest request,
        ClassLoaderHandle classLoaderHandle) {
        try {
            UserCodeClassLoader userCodeClassLoader = ClassLoaderHandle.createUserCodeClassloader(
                request, classLoaderHandle);
            return userCodeClassLoader;
        } catch (Exception ex) {
            log.error("Failed to submit task, request: {}", request, ex);
            throw new RuntimeException(ex);
        }
    }
}
