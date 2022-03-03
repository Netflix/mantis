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
package io.mantisrx.server.worker;

import io.mantisrx.common.codec.Codecs;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.Job;
import io.mantisrx.runtime.MantisJob;
import io.mantisrx.runtime.MantisJobProvider;
import io.mantisrx.runtime.Metadata;
import io.mantisrx.runtime.PortRequest;
import io.mantisrx.runtime.ScalarToScalar;
import io.mantisrx.runtime.executor.LocalJobExecutorNetworked;
import io.mantisrx.runtime.sink.Sink;
import io.mantisrx.runtime.source.Index;
import io.mantisrx.runtime.source.Source;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import rx.Observable;
import rx.Observer;
import rx.Subscription;

@RequiredArgsConstructor
@Slf4j
public class WordCountJobProvider extends MantisJobProvider<String> {
  private final CompletableFuture<Void> futureNotifier;
  private final Collection<String> output;

  @Override
  public Job<String> getJobInstance() {
    return MantisJob
        .source(new IlliadSource())
        // Simply echoes the tweet
        .stage((context, dataO) -> dataO
                // Tokenize
                .flatMap((text) -> Observable.from(tokenize(text)))
                // On a hopping window of 10 seconds
                .window(10, TimeUnit.SECONDS)
                .flatMap((wordCountPairObservable) -> wordCountPairObservable
                    // count how many times a word appears
                    .groupBy(WordCountPair::getWord)
                    .flatMap((groupO) -> groupO.reduce(0, (cnt, wordCntPair) -> cnt + 1)
                        .map((cnt) -> new WordCountPair(groupO.getKey(), cnt))))
                .map(WordCountPair::toString),
            stageConfig())
        // Reuse built in sink that eagerly subscribes and delivers data over SSE
        .sink(new Sink<String>() {
          private Subscription subscription;

          @Override
          public void close() {
            if (subscription != null) {
              subscription.unsubscribe();
            }
          }

          @Override
          public void call(Context context, PortRequest portRequest,
              Observable<String> stringObservable) {
            subscription = stringObservable.subscribe(
                new Observer<String>() {
                  @Override
                  public void onCompleted() {
                    futureNotifier.complete(null);
                  }

                  @Override
                  public void onError(Throwable e) {
                    futureNotifier.completeExceptionally(e);
                  }

                  @Override
                  public void onNext(String s) {
                    output.add(s);
                  }
                });
          }
        })
        .metadata(new Metadata.Builder()
            .name("WordCount")
            .description("Reads Homer's The Illiad faster than we can.")
            .build())
        .create();
  }

  ScalarToScalar.Config<String, String> stageConfig() {
    return new ScalarToScalar.Config<String, String>()
        .codec(Codecs.string());
  }

  private List<WordCountPair> tokenize(String text) {
    StringTokenizer tokenizer = new StringTokenizer(text);
    List<WordCountPair> wordCountPairs = new ArrayList<>();
    while(tokenizer.hasMoreTokens()) {
      String word = tokenizer.nextToken().replaceAll("\\s*", "").toLowerCase();
      wordCountPairs.add(new WordCountPair(word,1));
    }
    return wordCountPairs;
  }

  @Data
  static class WordCountPair {

    private final String word;
    private final int count;
  }

  class IlliadSource implements Source<String> {

    @Override
    public Observable<Observable<String>> call(Context context, Index index) {
      return Observable.interval(1, TimeUnit.SECONDS)
          .takeUntil(l -> l < 1)
          .map(__ -> {
            try {
              Path path = Paths.get(getClass().getClassLoader()
                  .getResource("illiad.txt").toURI());
              return Observable.from(() -> {
                    try {
                      return Files.lines(path).iterator();
                    } catch (IOException ex) {
                      log.error("IOException while reading illiad.txt from resources", ex);
                    }
                    return Stream.<String>empty().iterator();
                  }
              );
            } catch (URISyntaxException ex) {
              log.error("URISyntaxException while loading illiad.txt from resources.", ex);
            }
            return Observable.empty();
          });
    }

    @Override
    public void close() throws IOException {

    }
  }

  public static void main(String[] args) throws Exception {
    CompletableFuture<Void> futureNotifier = new CompletableFuture<>();
    ArrayList<String> output = new ArrayList<>();
    LocalJobExecutorNetworked.execute(new WordCountJobProvider(futureNotifier, output).getJobInstance());
    futureNotifier.get();
    for (String s : output) {
      log.info(s);
    }
  }
}
