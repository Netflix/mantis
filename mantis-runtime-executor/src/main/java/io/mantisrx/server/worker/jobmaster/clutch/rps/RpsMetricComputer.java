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

package io.mantisrx.server.worker.jobmaster.clutch.rps;

import com.netflix.control.clutch.Clutch;
import com.netflix.control.clutch.ClutchConfiguration;
import com.netflix.control.clutch.IRpsMetricComputer;
import java.util.Map;

public class RpsMetricComputer implements IRpsMetricComputer {
    public Double apply(ClutchConfiguration config, Map<Clutch.Metric, Double> metrics) {
        double rps = metrics.get(Clutch.Metric.RPS);
        double lag = metrics.get(Clutch.Metric.LAG);
        double sourceDrops = metrics.get(Clutch.Metric.SOURCEJOB_DROP);
        double drops = metrics.get(Clutch.Metric.DROPS) / 100.0 * rps;
        return rps + lag + sourceDrops + drops;
    }
}
