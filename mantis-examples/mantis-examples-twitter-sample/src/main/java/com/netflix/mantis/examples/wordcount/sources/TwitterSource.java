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

package com.netflix.mantis.examples.wordcount.sources;

import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;
import com.netflix.mantis.examples.core.ObservableQueue;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.parameter.ParameterDefinition;
import io.mantisrx.runtime.parameter.type.StringParameter;
import io.mantisrx.runtime.parameter.validator.Validators;
import io.mantisrx.runtime.source.Index;
import io.mantisrx.runtime.source.Source;
import rx.Observable;


/**
 * A Mantis Source that wraps an underlying Twitter source based on the HorseBirdClient.
 */
public class TwitterSource implements Source<String> {

    public static final String CONSUMER_KEY_PARAM = "consumerKey";

    public static final String CONSUMER_SECRET_PARAM = "consumerSecret";

    public static final String TOKEN_PARAM = "token";

    public static final String TOKEN_SECRET_PARAM = "tokenSecret";

    public static final String TERMS_PARAM = "terms";

    private final ObservableQueue<String> twitterObservable = new ObservableQueue<>();

    private transient BasicClient client;

    @Override
    public Observable<Observable<String>> call(Context context, Index index) {
        return Observable.just(twitterObservable.observe());
    }

    /**
     * Define parameters required by this source.
     *
     * @return
     */
    @Override
    public List<ParameterDefinition<?>> getParameters() {
        List<ParameterDefinition<?>> params = Lists.newArrayList();

        // Consumer key
        params.add(new StringParameter()
                .name(CONSUMER_KEY_PARAM)
                .description("twitter consumer key")
                .validator(Validators.notNullOrEmpty())
                .required()
                .build());

        params.add(new StringParameter()
                .name(CONSUMER_SECRET_PARAM)
                .description("twitter consumer secret")
                .validator(Validators.notNullOrEmpty())
                .required()
                .build());

        params.add(new StringParameter()
                .name(TOKEN_PARAM)
                .description("twitter token")
                .validator(Validators.notNullOrEmpty())
                .required()
                .build());

        params.add(new StringParameter()
                .name(TOKEN_SECRET_PARAM)
                .description("twitter token secret")
                .validator(Validators.notNullOrEmpty())
                .required()
                .build());

        params.add(new StringParameter()
                .name(TERMS_PARAM)
                .description("terms to follow")
                .validator(Validators.notNullOrEmpty())
                .defaultValue("Netflix,Dark")
                .build());

        return params;

    }

    /**
     * Init method is called only once during initialization. It is the ideal place to perform one time
     * configuration actions.
     *
     * @param context Provides access to Mantis system information like JobId, Job parameters etc
     * @param index   This provides access to the unique workerIndex assigned to this container. It also provides
     *                the total number of workers of this job.
     */
    @Override
    public void init(Context context, Index index) {
        String consumerKey = (String) context.getParameters().get(CONSUMER_KEY_PARAM);
        String consumerSecret = (String) context.getParameters().get(CONSUMER_SECRET_PARAM);
        String token = (String) context.getParameters().get(TOKEN_PARAM);
        String tokenSecret = (String) context.getParameters().get(TOKEN_SECRET_PARAM);

        String terms = (String) context.getParameters().get(TERMS_PARAM);

        Authentication auth = new OAuth1(consumerKey,
                consumerSecret,
                token,
                tokenSecret);

        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();

        String[] termArray = terms.split(",");

        List<String> termsList = Arrays.asList(termArray);

        endpoint.trackTerms(termsList);

        client = new ClientBuilder()
                .name("twitter-source")
                .hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new StringDelimitedProcessor(twitterObservable))
                .build();

        client.connect();
    }
}
