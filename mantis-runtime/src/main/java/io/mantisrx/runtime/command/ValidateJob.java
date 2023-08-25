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

package io.mantisrx.runtime.command;

import io.mantisrx.runtime.Job;
import io.mantisrx.runtime.SinkHolder;
import io.mantisrx.runtime.SourceHolder;
import io.mantisrx.runtime.StageConfig;
import java.util.List;


public class ValidateJob implements Command {

    @SuppressWarnings("rawtypes")
    private final Job job;

    @SuppressWarnings("rawtypes")
    public ValidateJob(Job job) {
        this.job = job;
    }

    @SuppressWarnings( {"rawtypes", "unchecked"})
    @Override
    public void execute() throws CommandException {
        if (job == null) {
            throw new InvalidJobException("job reference cannot be null");
        }

        SourceHolder source = job.getSource();
        if (source == null || source.getSourceFunction() == null) {
            throw new InvalidJobException("A job requires a source");
        }

        List<StageConfig<?, ?>> stages = job.getStages();
        if (stages == null || stages.isEmpty()) {
            throw new InvalidJobException("A job requires at least one stage");
        }

        int i = 0;
        for (StageConfig<?, ?> stage : stages) {
            if (stage == null) {
                throw new InvalidJobException("Stage cannot be null, for stage number: " + i);
            }
            i++;
        }

        SinkHolder sink = job.getSink();
        if (sink == null || sink.getSinkAction() == null) {
            throw new InvalidJobException("A job requires a sink");
        }
    }


}
