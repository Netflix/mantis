/*
 * Copyright 2024 Netflix, Inc.
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

package io.mantisrx.control.clutch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.yahoo.sketches.quantiles.UpdateDoublesSketch;
import io.mantisrx.control.clutch.metrics.IClutchMetricsRegistry;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.assertj.core.data.Offset;
import org.junit.Test;
import rx.Observable;

public class ClutchConfiguratorTest {



    @Test public void shouldCorrectlyBoundValues() {
        assertThat(ClutchConfigurator.bound(0, 10, 50)).isEqualTo(10, Offset.offset(0.001));
        assertThat(ClutchConfigurator.bound(0, 10, -10.0)).isEqualTo(0, Offset.offset(0.001));
        assertThat(ClutchConfigurator.bound(0, 10, 5.2)).isEqualTo(5.2, Offset.offset(0.001));
    }

    @Test public void shouldProduceValeusInSaneRange() {

        ThreadLocalRandom random = ThreadLocalRandom.current();

        UpdateDoublesSketch sketch = UpdateDoublesSketch.builder().build(1024);
        for (int i = 0; i < 21; ++i) {
            sketch.update(random.nextDouble(8.3, 75.0));
        }

        assertThat(sketch.getQuantile(0.99)).isLessThan(76.0);
    }

    @Test public void shouldGetConfigWithoutException() {
        ClutchConfigurator configurator = new ClutchConfigurator(new IClutchMetricsRegistry() {}, 1, 2, Observable.interval(1, TimeUnit.DAYS));
        configurator.getSketch(Clutch.Metric.CPU).update(70.0);
        assertNotNull(configurator.getConfig());
    }

    @Test
    public void testPercentileCalculation() {

        DescriptiveStatistics stats = new DescriptiveStatistics(100);
        UpdateDoublesSketch sketch = UpdateDoublesSketch.builder().build(1024);
        for (int i = 0; i < 200; ++i) {
            sketch.update(i);
            stats.addValue(i);
        }
        assertEquals(sketch.getQuantile(0.8), 160, 0);
        assertEquals(stats.getPercentile(80), 180, 1);
    }
    // TODO: What guarantees do I want to make about the configurator?
}
