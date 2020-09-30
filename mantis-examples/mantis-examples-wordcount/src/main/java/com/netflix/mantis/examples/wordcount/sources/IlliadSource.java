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

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.source.Index;
import io.mantisrx.runtime.source.Source;
import lombok.extern.log4j.Log4j;
import rx.Observable;


/**
 * Ignore the contents of this file for the tutorial. The purpose is just to generate a stream of interesting data
 * on which we can word count.
 */
@Log4j
public class IlliadSource implements Source<String> {

    @Override
    public Observable<Observable<String>> call(Context context, Index index) {
        return Observable.interval(10, TimeUnit.SECONDS)
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
}
