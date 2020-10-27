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

import com.netflix.spectator.api.Tag;


public class MetricGroupId {

    private final String name;
    private final Iterable<Tag> tags;
    private final String id;

    public MetricGroupId(final String name) {
        this(name, Collections.emptyList());
    }

    public MetricGroupId(final String name,
                         final Iterable<Tag> tags) {
        this.name = name;
        this.tags = tags;
        this.id = createId(name, tags);
    }

    public MetricGroupId(final String name,
                         final Tag... tags) {
        this(name, Arrays.asList(tags));
    }

    private String createId(final String name, final Iterable<Tag> tags) {
        StringBuilder buf = new StringBuilder();
        buf.append(name);
        for (Tag t : tags) {
            buf.append(':').append(t.key()).append('=').append(t.value());
        }
        return buf.toString();
    }

    public String name() {
        return name;
    }

    public Iterable<Tag> tags() {
        return tags;
    }

    public String id() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MetricGroupId that = (MetricGroupId) o;

        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        if (tags != null ? !tags.equals(that.tags) : that.tags != null) return false;
        return id != null ? id.equals(that.id) : that.id == null;

    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (tags != null ? tags.hashCode() : 0);
        result = 31 * result + (id != null ? id.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MetricGroupId{");
        sb.append("name='").append(name).append('\'');
        sb.append(", tags=").append(tags);
        sb.append(", id='").append(id).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
