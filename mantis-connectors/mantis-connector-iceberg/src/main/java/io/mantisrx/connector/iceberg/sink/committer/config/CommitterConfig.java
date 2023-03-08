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

import static io.mantisrx.connector.iceberg.sink.committer.config.CommitterProperties.COMMIT_FREQUENCY_MS;
import static io.mantisrx.connector.iceberg.sink.committer.config.CommitterProperties.WATERMARK_PROPERTY_KEY;

import io.mantisrx.connector.iceberg.sink.config.SinkConfig;
import io.mantisrx.runtime.parameter.Parameters;
import javax.annotation.Nullable;

/**
 * Config for controlling Iceberg Committer semantics.
 */
public class CommitterConfig extends SinkConfig {

    private final long commitFrequencyMs;

    @Nullable
    private final String watermarkPropertyKey;

    /**
     * Creates an instance from {@link Parameters} derived from the current Mantis Stage's {@code Context}.
     */
    public CommitterConfig(Parameters parameters) {
        super(parameters);
        this.commitFrequencyMs = Long.parseLong((String) parameters.get(COMMIT_FREQUENCY_MS));
        this.watermarkPropertyKey = (String) parameters.get(WATERMARK_PROPERTY_KEY, null);
    }

    /**
     * Returns a long representing Iceberg committer frequency by time (milliseconds).
     */
    public long getCommitFrequencyMs() {
        return commitFrequencyMs;
    }

    @Nullable
    public String getWatermarkPropertyKey() {
        return watermarkPropertyKey;
    }
}
