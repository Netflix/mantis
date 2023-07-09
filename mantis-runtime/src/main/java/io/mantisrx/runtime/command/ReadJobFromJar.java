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
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.ServiceLoader;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;


public class ReadJobFromJar implements Command {

    private final String jobJarFile;
    @SuppressWarnings("rawtypes")
    private Job job;

    public ReadJobFromJar(String jobJarFile) {
        this.jobJarFile = jobJarFile;
    }

    @SuppressWarnings("rawtypes")
    public Job getJob() {
        return job;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void execute() throws CommandException {

        try {
            // check jar file for service file
            JarFile jf = new JarFile(jobJarFile);
            Enumeration<JarEntry> en = jf.entries();
            String serviceFile = "META-INF/services/io.mantisrx.runtime.MantisJobProvider";
            boolean serviceFileFound = false;
            while (en.hasMoreElements()) {
                JarEntry entry = en.nextElement();
                if (serviceFile.equals(entry.getName())) {
                    serviceFileFound = true;
                }
            }
            jf.close();
            if (!serviceFileFound) {
                throw new ReadJobFromJarException("Service file not found in jar at location: META-INF/services/io.mantisrx.runtime.MantisJobProvider");
            }

            // put jar into classpath to get MantisJob instance
            // Note, hard coded to file protocol
            ClassLoader cl = URLClassLoader.newInstance(new URL[] {new URL("file://" + jobJarFile)},
                    Thread.currentThread().getContextClassLoader());
            int providerCount = 0;
            ServiceLoader<MantisJobProvider> provider = ServiceLoader.load(MantisJobProvider.class, cl);
            for (MantisJobProvider jobProvider : provider) {
                job = jobProvider.getJobInstance();
                providerCount++;
            }
            if (providerCount != 1) {
                throw new ReadJobFromJarException("Provider count not equal to 1 for MantisJobProvider, count: " + providerCount + ".  Either no "
                        + "entry exists in META-INF/services/io.mantisrx.runtime.MantisJobProvider, or more than one entry exists."
                );
            }

        } catch (IOException e) {
            //MalformedURLException is Subclass of the IOException
            throw new ReadJobFromJarException(e);
        }

    }

}
