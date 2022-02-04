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

package io.mantisrx.publish.api;

public class StreamType {

    /**
     * Default Stream name for emitting events.
     */
	public static final String DEFAULT_EVENT_STREAM = "__default__";

    /**
     * Stream name for emitting request events.
     */
    public static final String REQUEST_EVENT_STREAM = "requestEventStream";

    /**
     * Stream name for emitting log4j events by using the Mantis Log4J appender.
     */
    public static final String LOG_EVENT_STREAM = "logEventStream";
}
