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

package io.mantisrx.runtime.core;

import io.mantisrx.runtime.*;
import io.mantisrx.runtime.computation.*;
import io.mantisrx.runtime.sink.SelfDocumentingSink;
import io.mantisrx.runtime.sink.Sink;
import io.mantisrx.runtime.sink.Sinks;
import io.mantisrx.runtime.source.Source;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import rx.Observable;

@Slf4j
public class MantisJobBuilder {
  private SourceHolder sourceHolder;
  private Stages currentStage;
  private Config jobConfig;
  private CompositeScalarFunction compositeScalarFn;

  MantisJobBuilder() {}

  Job buildJob() {
    ScalarStages<Object> stage = ((ScalarStages) currentStage);
      Config<Object> jobConfig = stage.sink(Sinks.eagerSubscribe(new SelfDocumentingSink<Object>() {
          @Override
          public Metadata metadata() {
              return new Metadata.Builder().description("eager print sink").build();
          }

          @Override
          public void close() {
          }

          @Override
          public void call(Context context, PortRequest portRequest, Observable<Object> objectObservable) {
              objectObservable.subscribe(o -> log.info("sink output {}", o));
          }
      }));
    return jobConfig.create();
  }
  public MantisJobBuilder addStage(Source source) {
    this.sourceHolder = MantisJob.source(source);
    return this;
  }

  public MantisJobBuilder addStage(Computation compFn, Object config) {
    if (this.sourceHolder == null) {
      throw new IllegalArgumentException(
              "SourceHolder not currently set. Uninitialized MantisJob configuration");
    }
    if (config instanceof ScalarToScalar.Config) {
      if (compositeScalarFn == null) {
        compositeScalarFn = new CompositeScalarFunction<>();
      }
      compositeScalarFn.addScalarFn((ScalarComputation) compFn, ((ScalarToScalar.Config<?, ?>) config));
    } else {
      if (compositeScalarFn != null && compositeScalarFn.size() > 0) {
        if (currentStage == null) {
          this.currentStage = addInitStage(compositeScalarFn, compositeScalarFn.getConfig());
        } else {
          this.currentStage = addMoreStages(compositeScalarFn, compositeScalarFn.getConfig());
        }
        this.compositeScalarFn = null;
      }
      this.currentStage = addMoreStages(compFn, config);
    }
    return this;
  }

  public MantisJobBuilder addStage(Sink sink) {
    ScalarStages scalarStages = (ScalarStages) this.currentStage;
    this.jobConfig = scalarStages.sink(sink);
    return this;
  }

  public MantisJobBuilder addStage(SelfDocumentingSink sink) {
    ScalarStages scalarStages = (ScalarStages) this.currentStage;
    this.jobConfig = scalarStages.sink(sink);
    return this;
  }

  private Stages addInitStage(Computation compFn, Object config) {
    if (config instanceof ScalarToScalar.Config) {
      return this.sourceHolder.stage((ScalarComputation) compFn, (ScalarToScalar.Config) config);
    } else if (config instanceof ScalarToKey.Config) {
      return this.sourceHolder.stage((ToKeyComputation) compFn, (ScalarToKey.Config) config);
    }
    return this.sourceHolder.stage((ToGroupComputation) compFn, (ScalarToGroup.Config) config);
  }

  private Stages addMoreStages(Computation compFn, Object config) {
    if (this.currentStage instanceof ScalarStages) {
      ScalarStages scalarStages = (ScalarStages) this.currentStage;
      if (config instanceof ScalarToScalar.Config) {
        ScalarComputation scalarFn = (ScalarComputation) compFn;
        return scalarStages.stage(scalarFn, (ScalarToScalar.Config) config);
      } else if (config instanceof ScalarToKey.Config) {
        return scalarStages.stage((ToKeyComputation) compFn, (ScalarToKey.Config) config);
      }
      return scalarStages.stage((ToGroupComputation) compFn, (ScalarToGroup.Config) config);
    }
    KeyedStages keyedStages = (KeyedStages) this.currentStage;
    if (config instanceof KeyToScalar.Config) {
      return keyedStages.stage((ToScalarComputation) compFn, (KeyToScalar.Config) config);
    } else if (config instanceof GroupToScalar.Config) {
      return keyedStages.stage((GroupToScalarComputation) compFn, (GroupToScalar.Config) config);
    } else if (config instanceof KeyToKey.Config) {
      return keyedStages.stage((KeyComputation) compFn, (KeyToKey.Config) config);
    }
    return keyedStages.stage((GroupComputation) compFn, (GroupToGroup.Config) config);
  }

  private static class CompositeScalarFunction<T> extends BeamScalarComputation<T, T> {
    public final List<ScalarComputation<T, T>> scalarFns = new ArrayList<>();
    public ScalarToScalar.Config<T,T> config;

    public CompositeScalarFunction() {
      super(null);
    }

    public void addScalarFn(ScalarComputation<T, T> fn, ScalarToScalar.Config<T, T> config) {
      this.scalarFns.add(fn);
      this.config = config;
    }

    @Override
    public String stringifyTransform() {
      return scalarFns.stream()
              .map(fn -> (fn instanceof BeamScalarComputation)
               ? ((BeamScalarComputation<T, T>) fn).stringifyTransform() :
                String.format("scalarfn.class.%s",fn.getClass().getName()))
              .collect(Collectors.joining("\n"));
    }

    @Override
    public void init(Context context) {
      this.scalarFns.forEach(x -> x.init(context));
    }

    @Override
    public Observable<T> call(Context context, Observable<T> tObservable) {
      Observable<T> result = tObservable;
      for (ScalarComputation<T,T> fn : this.scalarFns) {
        result = fn.call(context, result);
      }
      return result;
    }

    public int size() {
      return scalarFns.size();
    }

    public ScalarToScalar.Config<T,T> getConfig() {
      return config.description(String.format("scalar stage for beam composite %s", stringifyTransform()));
    }
  }
}
