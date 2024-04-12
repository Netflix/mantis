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

package io.mantisrx.server.master.scheduler;

import io.mantisrx.runtime.MantisJobDurationType;
import io.mantisrx.server.core.domain.JobMetadata;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.core.scheduler.SchedulingConstraints;
import java.util.Objects;
import lombok.Getter;

public class ScheduleRequest {
    @Getter private final WorkerId workerId;
    @Getter private final int stageNum;
    @Getter private final JobMetadata jobMetadata;
    @Getter private final MantisJobDurationType durationType;
    @Getter private final SchedulingConstraints schedulingConstraints;
    @Getter private final long readyAt;

    public ScheduleRequest(final WorkerId workerId,
                           final int stageNum,
                           final JobMetadata jobMetadata,
                           final MantisJobDurationType durationType,
                           final SchedulingConstraints schedulingConstraints,
                           final long readyAt) {
        this.workerId = workerId;
        this.stageNum = stageNum;
        this.jobMetadata = jobMetadata;
        this.durationType = durationType;
        this.schedulingConstraints = schedulingConstraints;

        this.readyAt = readyAt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ScheduleRequest that = (ScheduleRequest) o;

        return Objects.equals(workerId, that.workerId);
    }

    @Override
    public int hashCode() {
        return workerId != null ? workerId.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "ScheduleRequest{" +
                "workerId=" + workerId +
                ", readyAt=" + readyAt +
                '}';
    }
}
