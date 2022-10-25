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

package io.mantisrx.runtime.loader.config;

import java.io.IOException;

/**
 * Abstraction useful for collecting metrics about the node on which the worker task is running.
 * The metrics that are collected from the node are available via the metrics port, which is used
 * by the jobmanager node then for auto-scaling.
 */
public interface MetricsCollector {
    Usage get() throws IOException;
}
