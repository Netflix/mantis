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

package io.mantisrx.runtime.executor;

import java.util.Iterator;

import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.Job;
import io.mantisrx.runtime.StageConfig;
import io.reactivex.mantis.remote.observable.RxMetrics;
import junit.framework.Assert;
import org.junit.Test;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.subjects.BehaviorSubject;


public class StageExecutorsSingleStageTest {

    @SuppressWarnings("rawtypes")
    @Test
    public void testSingleStageJob() {

        Action0 noOpAction = new Action0() {
            @Override
            public void call() {}
        };

        Action1<Throwable> noOpError = new Action1<Throwable>() {
            @Override
            public void call(Throwable t) {}
        };
        TestJobSingleStage provider = new TestJobSingleStage();
        Job job = provider.getJobInstance();
        PortSelector portSelector = new PortSelectorInRange(8000, 9000);
        StageConfig<?, ?> stage = (StageConfig<?, ?>) job.getStages().get(0);
        BehaviorSubject<Integer> workersInStageOneObservable = BehaviorSubject.create(1);
        StageExecutors.executeSingleStageJob(job.getSource(), stage,
                job.getSink(), portSelector, new RxMetrics(), new Context(),
                noOpAction, 0, workersInStageOneObservable, null, null, noOpAction, noOpError);

        Iterator<Integer> iter = provider.getItemsWritten().iterator();
        Assert.assertEquals(0, iter.next().intValue());
        Assert.assertEquals(1, iter.next().intValue());
        Assert.assertEquals(4, iter.next().intValue());
        Assert.assertEquals(9, iter.next().intValue());

    }
}
