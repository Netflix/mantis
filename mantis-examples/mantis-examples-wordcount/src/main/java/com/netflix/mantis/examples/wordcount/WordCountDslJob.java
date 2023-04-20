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

package com.netflix.mantis.examples.wordcount;

import com.netflix.mantis.examples.core.WordCountPair;
import com.netflix.mantis.examples.wordcount.sources.IlliadSource;
import io.mantisrx.runtime.Job;
import io.mantisrx.runtime.MantisJobProvider;
import io.mantisrx.runtime.Metadata;
import io.mantisrx.runtime.core.MantisStream;
import io.mantisrx.runtime.core.WindowSpec;
import io.mantisrx.runtime.core.functions.SimpleReduceFunction;
import io.mantisrx.runtime.core.sinks.ObservableSinkImpl;
import io.mantisrx.runtime.core.sources.ObservableSourceImpl;
import io.mantisrx.runtime.executor.LocalJobExecutorNetworked;
import io.mantisrx.runtime.sink.Sinks;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import lombok.extern.slf4j.Slf4j;


/**
 * This sample demonstrates ingesting data from a text file and counting the number of occurrences of words within a 10
 * sec hopping window.
 * Run the main method of this class and then look for a the SSE port in the output
 * E.g
 * <code> Serving modern HTTP SSE server sink on port: 8650 </code>
 * You can curl this port <code> curl localhost:8650</code> to view the output of the job.
 *
 * To run via gradle
 * /gradlew :mantis-examples-wordcount:execute
 */
@Slf4j
public class WordCountDslJob extends MantisJobProvider<String> {

    @Override
    public Job<String> getJobInstance() {
        return MantisStream.create(null)
            .source(new ObservableSourceImpl<>(new IlliadSource()))
            .flatMap(this::tokenize)
            .map(x -> {
                // this guards against TooLongFrameException for some reason, need to investigate!
                try {
                    Thread.sleep(0, 10000);
                } catch (InterruptedException ignored) {
                }
                return x;
            })
            .keyBy(WordCountPair::getWord)
            .window(WindowSpec.timed(Duration.ofSeconds(10)))
            .reduce((SimpleReduceFunction<WordCountPair>) (acc, item) -> {
                if (acc.getWord() != null && !acc.getWord().isEmpty() && !acc.getWord().equals(item.getWord())) {
                    log.warn("keys dont match: acc ({}) vs item ({})", acc.getWord(), item.getWord());
                }
                return new WordCountPair(acc.getWord(), acc.getCount() + item.getCount());
            })
            .map(WordCountPair::toString)
            // Reuse built in sink that eagerly subscribes and delivers data over SSE
            .sink(new ObservableSinkImpl<>(Sinks.eagerSubscribe(Sinks.sse((String data) -> data))))
            .metadata(new Metadata.Builder()
                .name("WordCount")
                .description("Reads Homer's The Illiad faster than we can.")
                .build())
            .create();
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


    public static void main(String[] args) {
        LocalJobExecutorNetworked.execute(new WordCountDslJob().getJobInstance());
    }
}
