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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import org.junit.Test;

public class DisableTaskExecutorsRequestTest {
    private static final DisableTaskExecutorsRequest R1 =
        new DisableTaskExecutorsRequest(ImmutableMap.of("attr1", "attr1"), ClusterID.of("cluster1"), Instant.now(), Optional.empty());
    private static final DisableTaskExecutorsRequest R2 =
        new DisableTaskExecutorsRequest(ImmutableMap.of("attr2", "attr2"), ClusterID.of("cluster1"), Instant.now(), Optional.empty());
    private static final DisableTaskExecutorsRequest R3 =
        new DisableTaskExecutorsRequest(ImmutableMap.of("attr1", "attr1"), ClusterID.of("cluster2"), Instant.now(), Optional.empty());
    private static final DisableTaskExecutorsRequest R4 =
        new DisableTaskExecutorsRequest(ImmutableMap.of("attr1", "attr1"), ClusterID.of("cluster1"), Instant.now().plus(Duration.ofDays(1)), Optional.empty());

    @Test
    public void checkIfDifferentRequestsHaveDifferentHashes() {
        assertNotEquals(R1.getHash(), R2.getHash());
    }

    @Test
    public void checkIfDifferentClustersHaveDifferentHashes() {
        assertNotEquals(R1.getHash(), R3.getHash());
    }

    @Test
    public void checkIfSimilarRequestsHaveSameHashes() {
        assertEquals(R1.getHash(), R4.getHash());
    }

    // TODO(fdichiara): add tests with new field.
}
