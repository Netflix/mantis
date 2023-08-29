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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LoadValidateCreateDir implements Command {

    private static final Logger logger = LoggerFactory.getLogger(LoadValidateCreateDir.class);

    private final String jarPath;

    public LoadValidateCreateDir(String jarPath) {
        this.jarPath = jarPath;

    }

    public static void main(String[] args) throws CommandException {

        if (args.length < 1) {
            System.err.println("usage: directory to scan");
            System.exit(1);
        }

        String classpath = System.getProperty("java.class.path");
        System.out.println(classpath);
        // loop through each file in directory and process it
        String jarPath = args[0];
        new LoadValidateCreateDir(jarPath).execute();

        System.exit(0);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void execute() throws CommandException {


        File dir = new File(jarPath);
        File[] directoryListing = dir.listFiles();
        if (directoryListing != null) {
            for (File child : directoryListing) {

                System.out.println("================================");

                // Absolute Path
                System.out.println("Absolute Path:" + child.getAbsolutePath());

                // File name only
                System.out.println("File name only:" + child.getName());

                // JSON file name
                File fileLoop = new File(child.getAbsolutePath());


                System.out.println("Dirname: " + fileLoop.getParent());
                System.out.println("Basename: " + fileLoop.getName());

                String fileBase = fileLoop.getName().substring(0, fileLoop.getName().lastIndexOf("."));
                String fileExtension = fileLoop.getName().substring(fileLoop.getName().lastIndexOf(".") + 1);
                String jsonFile = fileBase + ".json";
                String fileVersion = fileLoop.getName().substring(fileLoop.getName().lastIndexOf("-") + 1, fileLoop.getName().lastIndexOf("."));

                System.out.println("fileBase: " + fileBase);
                System.out.println("fileExtension: " + fileExtension);
                System.out.println("jsonFile: " + jsonFile);
                System.out.println("fileVersion: " + fileVersion);

                try {
                    ReadJobFromJar readCommand = new ReadJobFromJar(child.getAbsolutePath());
                    readCommand.execute();
                    Job job = readCommand.getJob();
                    new ValidateJob(job).execute();
                    File jobDescriptor = new File(fileLoop.getParent() + "/" + jsonFile);
                    new CreateJobDescriptorFile(job, jobDescriptor, fileVersion, fileBase).execute();
                } catch (Exception e) {
                    logger.error("Got an error " + e.getMessage());
                    System.exit(1);
                }

                System.out.println("================================");

            }
        } else {
            // Handle the case where dir is not really a directory.
            // Checking dir.isDirectory() above would not be sufficient
            // to avoid race conditions with another process that deletes
            // directories.
            System.out.println("not a dir");
            System.exit(1);
        }

    }
}
