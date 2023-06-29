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
import io.mantisrx.runtime.MantisJobProvider;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ReadJobFromZip implements Command {

    private static final Logger logger = LoggerFactory.getLogger(ReadJobFromZip.class);
    private final String jobZipFile;
    private final String artifactName;
    private final String version;
    @SuppressWarnings("rawtypes")
    private Job job;

    public ReadJobFromZip(String jobZipFile, String artifactName,
                          String version) {
        this.jobZipFile = jobZipFile;
        this.artifactName = artifactName;
        this.version = version;
    }

    @SuppressWarnings("rawtypes")
    public Job getJob() {
        return job;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void execute() throws CommandException {

        try {
            ZipFile zipFile = new ZipFile(jobZipFile);
            final Enumeration<? extends ZipEntry> entries = zipFile.entries();
            String jobProviderFilename = "io.mantisrx.runtime.MantisJobProvider";

            boolean jobProviderFound = false;

            while (entries.hasMoreElements()) {
                ZipEntry zipEntry = entries.nextElement();
                if (!zipEntry.isDirectory() && zipEntry.getName().matches("^.*" + jobProviderFilename + "$")) {
                    jobProviderFound = true;
                    final InputStream mainClassInputStream = zipFile.getInputStream(zipEntry);
                    final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(mainClassInputStream));
                    final String jobProviderClassName = bufferedReader.readLine();
                    logger.info("loading MantisJobProvider from " + jobProviderClassName);
                    final Class<?> clazz = Class.forName(jobProviderClassName);
                    final MantisJobProvider jobProvider = (MantisJobProvider)  clazz.getDeclaredConstructor().newInstance();
                    job = jobProvider.getJobInstance();
                    break;
                }
            }
            if (!jobProviderFound) {
                throw new ReadJobFromZipException("no entries in ZipFile matching jobProvider " + jobProviderFilename);
            }
            if (job == null) {
                throw new ReadJobFromZipException("failed to load job from classname in " + jobProviderFilename);
            }

        } catch (IOException | IllegalAccessException | ClassNotFoundException | InstantiationException e) {
            throw new ReadJobFromZipException(e);
        } catch (InvocationTargetException | NoSuchMethodException e) {
            e.printStackTrace();
        }

    }

}
