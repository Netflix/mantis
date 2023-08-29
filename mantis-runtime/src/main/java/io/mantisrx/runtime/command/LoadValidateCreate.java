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


public class LoadValidateCreate implements Command {

    private final String jobJarFile;
    private final String artifactName;
    private final String version;
    private final String project;
    private final String outputLocation;

    public LoadValidateCreate(String jobJarFile, String artifactName,
                              String version, String outputLocation,
                              String project) {
        this.jobJarFile = jobJarFile;
        this.outputLocation = outputLocation;
        this.version = version;
        this.artifactName = artifactName;
        this.project = project;
    }

    public static void main(String[] args) throws CommandException {

        if (args.length < 4) {
            System.err.println("usage: jarFile name version outputLocation project");
            System.exit(1);
        }

        String jobJarFile = args[0];
        String name = args[1];
        String version = args[2];
        String outputLocation = args[3];

        new LoadValidateCreate(jobJarFile, name, version, outputLocation, name).execute();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void execute() throws CommandException {
        ReadJobFromJar readCommand = new ReadJobFromJar(jobJarFile);
        readCommand.execute();
        Job job = readCommand.getJob();
        new ValidateJob(job).execute();
        File jobDescriptor = new File(outputLocation + "/" + artifactName + "-" + version + ".json");
        new CreateJobDescriptorFile(job, jobDescriptor, version, project).execute();
        new CreateZipFile(new File(outputLocation + "/" + artifactName + "-" + version + ".mantis")
                , new File(jobJarFile), jobDescriptor).execute();
    }
}
