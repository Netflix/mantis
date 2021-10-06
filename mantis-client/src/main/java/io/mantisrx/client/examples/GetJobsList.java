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

package io.mantisrx.client.examples;

import com.sampullara.cli.Args;
import com.sampullara.cli.Argument;
import io.mantisrx.client.MantisClient;
import io.mantisrx.runtime.MantisJobState;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


public class GetJobsList {

    @Argument(alias = "p", description = "Specify a configuration file", required = true)
    private static String propFile = "";

    @Argument(alias = "n", description = "Job name for getting list", required = true)
    private static String jobName;

    public static void main(String[] args) {
        try {
            Args.parse(GetJobsList.class, args);
        } catch (IllegalArgumentException e) {
            Args.usage(GetJobsList.class);
            System.exit(1);
        }
        System.out.println("propfile=" + propFile);
        Properties properties = new Properties();
        try (InputStream inputStream = new FileInputStream(propFile)) {
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        final MantisClient mantisClient = new MantisClient(properties);
        String json = mantisClient
                .getJobsOfNamedJob(jobName, MantisJobState.MetaState.Active)
                .toBlocking()
                .first();
        System.out.println("json: " + json);
    }
}
