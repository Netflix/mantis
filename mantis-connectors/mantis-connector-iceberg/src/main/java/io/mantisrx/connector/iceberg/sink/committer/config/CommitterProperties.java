/*
 * Copyright 2020 Netflix, Inc.
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

package io.mantisrx.connector.iceberg.sink.committer.config;

/**
 * Property key names and default values for an Iceberg Committer.
 */
public class CommitterProperties {

    private CommitterProperties() {
    }

    /**
     * Iceberg committer frequency by time in milliseconds.
     */
    public static final String COMMIT_FREQUENCY_MS = "commitFrequencyMs";
    public static final String WATERMARK_PROPERTY_KEY = "watermarkPropertyKey";
    // TODO: Change to long.
    public static final String COMMIT_FREQUENCY_MS_DEFAULT = "300000";    // 5 min
    public static final String COMMIT_FREQUENCY_DESCRIPTION =
            String.format("Iceberg Committer frequency by time in milliseconds (default: %s)",
                    COMMIT_FREQUENCY_MS_DEFAULT);

    public static final String WATERMARK_PROPERTY_DESCRIPTION =
            "Property key name for watermark value (default: null)";
}
