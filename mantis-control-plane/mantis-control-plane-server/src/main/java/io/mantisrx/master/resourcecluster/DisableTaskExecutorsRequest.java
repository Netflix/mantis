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

package io.mantisrx.master.resourcecluster;

import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorRegistration;
import io.mantisrx.shaded.com.google.common.io.BaseEncoding;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Map;
import java.util.TreeMap;
import javax.annotation.Nullable;
import lombok.Value;

@Value
public class DisableTaskExecutorsRequest {
    Map<String, String> attributes;

    ClusterID clusterID;

    Instant expiry;

    boolean isExpired(Instant now) {
        return expiry.compareTo(now) <= 0;
    }

    boolean targetsSameTaskExecutorsAs(DisableTaskExecutorsRequest another) {
        return this.attributes.entrySet().containsAll(another.attributes.entrySet());
    }

    boolean covers(@Nullable TaskExecutorRegistration registration) {
        return registration != null && registration.containsAttributes(this.attributes);
    }

    public String getHash() {
        try {
            MessageDigest messageDigest = MessageDigest.getInstance("MD5");
            messageDigest.update(clusterID.getResourceID().getBytes(StandardCharsets.UTF_8));
            TreeMap<String, String> clone = new TreeMap<>(attributes);
            clone.forEach((key, value) -> {
                messageDigest.update(key.getBytes(StandardCharsets.UTF_8));
                messageDigest.update(value.getBytes(StandardCharsets.UTF_8));
            });
            return BaseEncoding.base16().encode(messageDigest.digest());
        } catch (NoSuchAlgorithmException exception) {
            // don't expect this to happen
            // let's just throw a runtime exception in this case
            throw new RuntimeException(exception);
        }
    }
}
