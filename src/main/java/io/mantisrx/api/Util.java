/**
 * Copyright 2018 Netflix, Inc.
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
package io.mantisrx.api;

import com.google.common.base.Strings;
import io.netty.handler.codec.http.QueryStringDecoder;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import rx.Observable;
import rx.functions.Func1;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static io.mantisrx.api.Constants.*;

@UtilityClass
@Slf4j
public class Util {
    private static final int defaultNumRetries = 2;

    public static boolean startsWithAnyOf(final String target, List<String> prefixes) {
        for (String prefix : prefixes) {
            if (target.startsWith(prefix)) {
                return  true;
            }
        }
        return false;
    }

    //
    // Regions
    //

    public static String getLocalRegion() {
        return System.getenv("EC2_REGION");
    }

    public static boolean isAllRegion(String region) {
        return region != null && region.trim().equalsIgnoreCase("all");
    }

    //
    // Query Params
    //

    public static String[] getTaglist(String uri, String id) {
        return getTaglist(uri, id, null);
    }

    public static String[] getTaglist(String uri, String id, String region) {
        QueryStringDecoder queryStringDecoder = new QueryStringDecoder(uri);
        Map<String, List<String>> queryParameters = queryStringDecoder.parameters();
        boolean isClientIdSet = false;

        final List<String> tags = new LinkedList<>();
        if (queryParameters != null) {
            List<String> tagVals = queryParameters.get(TagsParamName);
            if (tagVals != null) {
                for (String s : tagVals) {
                    StringTokenizer tokenizer = new StringTokenizer(s, TagNameValDelimiter);
                    if (tokenizer.countTokens() == 2) {
                        String s1 = tokenizer.nextToken();
                        String s2 = tokenizer.nextToken();
                        if (s1 != null && !s1.isEmpty() && s2 != null && !s2.isEmpty()) {
                            tags.add(s1);
                            tags.add(s2);
                            if (ClientIdTagName.equals(s1)) {
                                isClientIdSet = true;
                            }
                        }
                    }
                }
            }
            tagVals = queryParameters.get(ClientIdTagName);
            if (!isClientIdSet && tagVals != null && !tagVals.isEmpty()) {
                tags.add(ClientIdTagName);
                tags.add(tagVals.get(0));
            }
        }

        tags.add("SessionId");
        tags.add(id);

        tags.add("urlPath");
        tags.add(queryStringDecoder.path());

        if (!Strings.isNullOrEmpty(region)) {
            tags.add("region");
            tags.add(region);
        }

        return tags.toArray(new String[]{});
    }

    //
    // Retries
    //

    public static Func1<Observable<? extends Throwable>, Observable<?>> getRetryFunc(final Logger logger, String name) {
        return getRetryFunc(logger, name, defaultNumRetries);
    }

    public static Func1<Observable<? extends Throwable>, Observable<?>> getRetryFunc(final Logger logger, String name, final int retries) {
        final int limit = retries == Integer.MAX_VALUE ? retries : retries + 1;
        return attempts -> attempts
                .zipWith(Observable.range(1, limit), (t1, integer) -> {
                    logger.warn("Caught exception connecting for {}.", name, t1);
                    return new ImmutablePair<Throwable, Integer>(t1, integer);
                })
                .flatMap(pair -> {
                    Throwable t = pair.left;
                    int retryIter = pair.right;
                    long delay = Math.round(Math.pow(2, retryIter));

                    if (retryIter > retries) {
                        logger.error("Exceeded maximum retries ({}) for {} with exception: {}", retries, name, t.getMessage(), t);
                        return Observable.error(new Exception("Timeout after " + retries + " retries"));
                    }
                    logger.info("Retrying connection to {} after sleeping for {} seconds.", name, delay, t);
                    return Observable.timer(delay, TimeUnit.SECONDS);
                });
    }
}
