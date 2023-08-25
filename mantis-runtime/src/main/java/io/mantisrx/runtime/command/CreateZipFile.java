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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;


public class CreateZipFile implements Command {

    private final File jobJarFile;
    private final File jobDescriptor;
    private final File zipFileName;

    public CreateZipFile(File zipFileName,
                         File jobJarFile, File jobDescriptor) {
        this.zipFileName = zipFileName;
        this.jobJarFile = jobJarFile;
        this.jobDescriptor = jobDescriptor;
    }

    private void readBytesFromFile(File file, ZipOutputStream os) throws CommandException {
        BufferedInputStream is = null;
        try {
            is = new BufferedInputStream(Files.newInputStream(Paths.get(file.toURI())));
            byte[] in = new byte[1024];
            int bytesRead;
            while ((bytesRead = is.read(in)) > 0) {
                os.write(in, 0, bytesRead);
            }
        } catch (IOException e) {
            throw new CommandException(e);
        } finally {
            try {
                if (is != null) {
                    is.close();
                }
            } catch (IOException e) {
                throw new CommandException(e);
            }
        }
    }

    @Override
    public void execute() throws CommandException {
        ZipOutputStream out = null;
        try {
            out = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(zipFileName)));
            ZipEntry jobJarEntry = new ZipEntry(jobJarFile.getName());
            out.putNextEntry(jobJarEntry);
            readBytesFromFile(jobJarFile, out);
            out.closeEntry();
            ZipEntry jobDescriptorEntry = new ZipEntry(jobDescriptor.getName());
            out.putNextEntry(jobDescriptorEntry);
            readBytesFromFile(jobDescriptor, out);
            out.closeEntry();
        } catch (IOException e) {
            throw new CommandException(e);
        } finally {
            if (out != null) {
                try {
                    out.close();
                } catch (IOException e) {
                    throw new CommandException(e);
                }
            }
        }
    }
}
