/*
 *
 * Copyright 2020 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.mantisrx.common.metrics.spectator;

import java.util.Arrays;
import java.util.Collections;

import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Tag;


public class MetricId {

    private final String metricGroup;
    private final String metricName;
    private final Iterable<Tag> tags;

    public MetricId(final String metricGroup, final String metricName) {
        this(metricGroup, metricName, Collections.emptyList());
    }

    public MetricId(final String metricGroup,
                    final String metricName,
                    final Iterable<Tag> tags) {
        this.metricGroup = metricGroup;
        this.metricName = metricName;
        this.tags = tags;
    }

    public MetricId(final String metricGroup,
                    final String metricName,
                    final Tag... tags) {
        this.metricGroup = metricGroup;
        this.metricName = metricName;
        this.tags = Arrays.asList(tags);
    }

    public String metricGroup() {
        return metricGroup;
    }

    public String metricName() {
        return metricName;
    }

    public Iterable<Tag> tags() {
        return tags;
    }

    public Id getSpectatorId(final Registry registry) {
        return registry.createId(String.format("%s_%s", metricGroup, metricName), tags);
    }

    public String metricNameWithTags() {
        if (tags.iterator().hasNext()) {
            StringBuilder buf = new StringBuilder();
            buf.append(metricName);
            for (Tag t : tags) {
                buf.append(':').append(t.key()).append('=').append(t.value());
            }
            return buf.toString();
        } else {
            return metricName;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MetricId metricId = (MetricId) o;

        if (!metricGroup.equals(metricId.metricGroup)) return false;
        if (!metricName.equals(metricId.metricName)) return false;
        return tags.equals(metricId.tags);

    }

    @Override
    public int hashCode() {
        int result = metricGroup.hashCode();
        result = 31 * result + metricName.hashCode();
        result = 31 * result + tags.hashCode();
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MetricId{");
        sb.append("metricGroup='").append(metricGroup).append('\'');
        sb.append(", metricName='").append(metricName).append('\'');
        sb.append(", tags=").append(tags);
        sb.append('}');
        return sb.toString();
    }
}
