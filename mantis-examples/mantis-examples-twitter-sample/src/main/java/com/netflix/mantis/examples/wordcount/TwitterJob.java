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

import com.mantisrx.common.utils.JsonUtility;
import com.netflix.mantis.examples.config.StageConfigs;
import com.netflix.mantis.examples.core.WordCountPair;
import com.netflix.mantis.examples.wordcount.sources.TwitterSource;
import io.mantisrx.runtime.Job;
import io.mantisrx.runtime.MantisJob;
import io.mantisrx.runtime.MantisJobProvider;
import io.mantisrx.runtime.Metadata;
import io.mantisrx.runtime.executor.LocalJobExecutorNetworked;
import io.mantisrx.runtime.parameter.Parameter;
import io.mantisrx.runtime.sink.Sinks;
import lombok.extern.slf4j.Slf4j;
import rx.Observable;


/**
 * This sample demonstrates connecting to a twitter feed and counting the number of occurrences of words within a 10
 * sec hopping window.
 * Run the main method of this class and then look for a the SSE port in the output
 * E.g
 * <code> Serving modern HTTP SSE server sink on port: 8650 </code>
 * You can curl this port <code> curl localhost:8650</code> to view the output of the job.
 *
 * To run via gradle
 * ../gradlew execute --args='consumerKey consumerSecret token tokensecret'
 */
@Slf4j
public class TwitterJob extends MantisJobProvider<String> {

    @Override
    public Job<String> getJobInstance() {
        return MantisJob
                .source(new TwitterSource())
                // Simply echoes the tweet
                .stage((context, dataO) -> dataO
                        .map(JsonUtility::jsonToMap)
                        // filter out english tweets
                        .filter((eventMap) -> {
                            if(eventMap.containsKey("lang") && eventMap.containsKey("text")) {
                                String lang = (String)eventMap.get("lang");
                                return "en".equalsIgnoreCase(lang);
                            }
                            return false;
                        }).map((eventMap) -> (String)eventMap.get("text"))
                        // tokenize the tweets into words
                        .flatMap((text) -> Observable.from(tokenize(text)))
                        // On a hopping window of 10 seconds
                        .window(10, TimeUnit.SECONDS)
                        .flatMap((wordCountPairObservable) -> wordCountPairObservable
                                // count how many times a word appears
                                .groupBy(WordCountPair::getWord)
                                .flatMap((groupO) -> groupO.reduce(0, (cnt, wordCntPair) -> cnt + 1)
                                        .map((cnt) -> new WordCountPair(groupO.getKey(), cnt))))
                                .map(WordCountPair::toString)
                                .doOnNext((cnt) -> log.info(cnt))
                        , StageConfigs.scalarToScalarConfig())
                // Reuse built in sink that eagerly subscribes and delivers data over SSE
                .sink(Sinks.eagerSubscribe(Sinks.sse((String data) -> data)))
                .metadata(new Metadata.Builder()
                        .name("TwitterSample")
                        .description("Connects to a Twitter feed")
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

        String consumerKey = null;
        String consumerSecret = null;
        String token = null;
        String tokenSecret = null;
        if(args.length != 4) {
            System.out.println("Usage: java com.netflix.mantis.examples.TwitterJob <consumerKey> <consumerSecret> <token> <tokenSecret");
            System.exit(0);
        } else {
            consumerKey = args[0].trim();
            consumerSecret = args[1].trim();
            token = args[2].trim();
            tokenSecret = args[3].trim();
        }

        LocalJobExecutorNetworked.execute(new TwitterJob().getJobInstance(),
                new Parameter(TwitterSource.CONSUMER_KEY_PARAM,consumerKey),
                new Parameter(TwitterSource.CONSUMER_SECRET_PARAM, consumerSecret),
                new Parameter(TwitterSource.TOKEN_PARAM, token),
                new Parameter(TwitterSource.TOKEN_SECRET_PARAM, tokenSecret)

        );
    }
}
