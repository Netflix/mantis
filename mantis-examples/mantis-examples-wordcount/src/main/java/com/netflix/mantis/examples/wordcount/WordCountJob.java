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

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;

import com.netflix.mantis.examples.config.StageConfigs;
import com.netflix.mantis.examples.core.WordCountPair;
import com.netflix.mantis.examples.wordcount.sources.IlliadSource;
import io.mantisrx.runtime.Job;
import io.mantisrx.runtime.MantisJob;
import io.mantisrx.runtime.MantisJobProvider;
import io.mantisrx.runtime.Metadata;
import io.mantisrx.runtime.executor.LocalJobExecutorNetworked;
import io.mantisrx.runtime.sink.Sinks;
import lombok.extern.slf4j.Slf4j;
import rx.Observable;


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
public class WordCountJob extends MantisJobProvider<String> {

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
                                .map(WordCountPair::toString)
                        , StageConfigs.scalarToScalarConfig())
                // Reuse built in sink that eagerly subscribes and delivers data over SSE
                .sink(Sinks.eagerSubscribe(Sinks.sse((String data) -> data)))
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
        LocalJobExecutorNetworked.execute(new WordCountJob().getJobInstance());
    }
}
