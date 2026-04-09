/*
 * Copyright 2026 Netflix, Inc.
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

package io.mantisrx.master.jobcluster.proto;

import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonInclude;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonSubTypes;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@EqualsAndHashCode(callSuper = true)
@ToString
@JsonInclude(JsonInclude.Include.ALWAYS)
public class HealthCheckResponse extends BaseResponse {

    private final boolean isHealthy;
    private final FailureReason failureReason;

    @JsonCreator
    public HealthCheckResponse(
            @JsonProperty("requestId") long requestId,
            @JsonProperty("responseCode") ResponseCode responseCode,
            @JsonProperty("message") String message,
            @JsonProperty("isHealthy") boolean isHealthy,
            @JsonProperty("failureReason") FailureReason failureReason) {
        super(requestId, responseCode, message);
        this.isHealthy = isHealthy;
        this.failureReason = failureReason;
    }

    public static HealthCheckResponse healthy(long requestId) {
        return new HealthCheckResponse(
                requestId, ResponseCode.SUCCESS, "OK", true, null);
    }

    public static HealthCheckResponse unhealthyWorkers(long requestId, List<FailedWorker> failedWorkers) {
        return new HealthCheckResponse(
                requestId, ResponseCode.SERVER_ERROR, "unhealthy workers", false,
                new WorkerFailure(failedWorkers));
    }

    public static HealthCheckResponse unhealthyAlerts(long requestId, List<String> alerts) {
        return new HealthCheckResponse(
                requestId, ResponseCode.SERVER_ERROR, "alerts firing", false,
                new AlertFailure(alerts));
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
    @JsonSubTypes({
            @JsonSubTypes.Type(value = WorkerFailure.class, name = "workerStatus"),
            @JsonSubTypes.Type(value = AlertFailure.class, name = "alertsFiring")
    })
    public sealed interface FailureReason permits WorkerFailure, AlertFailure {}

    public record WorkerFailure(List<FailedWorker> failedWorkers) implements FailureReason {}

    public record AlertFailure(List<String> alerts) implements FailureReason {}

    public record FailedWorker(int workerIndex, int workerNumber, String state) {}
}
