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

package io.mantisrx.server.master.domain;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Optional;
import org.junit.jupiter.api.Test;

public class JobIdTest {
    @Test
    public void testJobId() {
        final JobId jobId = new JobId("clustername", 10);
        final String idString1 = jobId.toString();
        final Optional<JobId> fromId = JobId.fromId(idString1);
        assert(fromId.isPresent());
        assertEquals(jobId, fromId.get());

        final String idString2 = jobId.getId();
        final Optional<JobId> fromId2 = JobId.fromId(idString2);
        assert(fromId2.isPresent());
        assertEquals(jobId, fromId2.get());
    }
}
