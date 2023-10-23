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
import java.io.File;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LoadValidateCreateZip implements Command {

    private final String jobZipFile;
    private final String artifactName;
    private final String version;
    private final String outputLocation;
    private final boolean readyForJobMaster;

    public LoadValidateCreateZip(final String jobZipFile, final String artifactName,
                                 final String version, final String outputLocation,
                                 final boolean readyForJobMaster) {
        this.jobZipFile = jobZipFile;
        this.outputLocation = outputLocation;
        this.version = version;
        this.artifactName = artifactName;
        this.readyForJobMaster = readyForJobMaster;
    }

    public static void main(String[] args) throws CommandException {

        if (args.length < 4) {
            System.err.println("usage: zipFile artifactName version outputLocation");
            System.exit(1);
        } else {
            // print all the args
            log.info("args: ");
            for (String arg : args) {
                log.info(arg);
            }
        }

        String jobZipFile = args[0];
        String name = args[1];
        String version = args[2];
        String outputLocation = args[3];
        boolean readyForJobMaster = false;
        if (args.length == 5) {
            readyForJobMaster = Boolean.parseBoolean(args[4]);
        }

        try {
            new LoadValidateCreateZip(jobZipFile, name, version, outputLocation, readyForJobMaster).execute();
        } catch (Exception e) {
            // print stack trace
            log.error("Failed with the following exception: ", e);
            System.exit(1);
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void execute() throws CommandException {
        ReadJobFromZip readCommand = new ReadJobFromZip(jobZipFile, artifactName, version);
        readCommand.execute();
        Job job = readCommand.getJob();
        new ValidateJob(job).execute();
        File jobDescriptor = new File(outputLocation + "/" + artifactName + "-" + version + ".json");
        new CreateJobDescriptorFile(job, jobDescriptor, version, artifactName, readyForJobMaster).execute();
    }
}
