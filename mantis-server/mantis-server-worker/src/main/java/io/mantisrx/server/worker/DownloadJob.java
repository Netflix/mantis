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

package io.mantisrx.server.worker;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DownloadJob {

    private static final Logger logger = LoggerFactory.getLogger(DownloadJob.class);
    private URL jobArtifactUrl;
    private String jobName;
    private String locationToStore;

    public DownloadJob(
            URL jobArtifactUrl, String jobName,
            String locationToStore) {
        this.jobArtifactUrl = jobArtifactUrl;
        this.locationToStore = locationToStore;
        this.jobName = jobName;
    }

    public static void main(String[] args) throws MalformedURLException {

        if (args.length < 3) {
            System.err.println("usage: job_artifact_url job_name location_to_store");
            System.exit(1);
        }

        logger.info("parameters, jobArtifactUrl: " + args[0]);
        logger.info("parameters, jobName: " + args[1]);
        logger.info("parameters, locationToStore: " + args[2]);

        new DownloadJob(new URL(args[0]), args[1], args[2]).execute();
    }

    public void execute() {

        String jobJarFile = jobArtifactUrl.getFile();
        String jarName = jobJarFile.substring(jobJarFile.lastIndexOf('/') + 1);
        Path path = Paths.get(locationToStore, jobName,
                "lib");

        logger.info("Started writing job to tmp directory: " + path);
        // download file to /tmp, then add file location
        try (InputStream is = jobArtifactUrl.openStream()) {
            Files.createDirectories(path);
            try (OutputStream os = Files.newOutputStream(Paths.get(path.toString(), jarName))) {
                byte[] bytes = new byte[2048];
                int read = 0;
                while ((read = is.read(bytes)) >= 0) {
                    os.write(bytes, 0, read);
                }
            }
        } catch (IOException e1) {
            logger.error("Failed to write job to local store at path: " + path, e1);
            throw new RuntimeException(e1);
        }
        logger.info("Finished writing job to tmp directory: " + path);
    }
}
