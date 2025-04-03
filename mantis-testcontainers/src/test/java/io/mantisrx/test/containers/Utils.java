package io.mantisrx.test.containers;

import io.mantisrx.shaded.org.apache.zookeeper.Op;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Utils {
    public static String getStringFromResource(String fileName) {
        InputStream inputStream =
            TestContainerHelloWorld.class.getClassLoader().getResourceAsStream(fileName);
        StringBuilder sb = new StringBuilder();
        try (InputStreamReader streamReader =
                 new InputStreamReader(inputStream, StandardCharsets.UTF_8);
             BufferedReader reader = new BufferedReader(streamReader)) {

            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
        } catch (IOException e) {
            System.out.println("Failed to load resource: " + fileName + ". " + e);
            throw new RuntimeException("Failed to load resource: " + fileName, e);
        }

        return sb.toString();
    }

    public static String getBuildVersion() {
        try (InputStream input = TestContainerHelloWorld.class.getClassLoader().getResourceAsStream(
            "version.properties")) {
            if (input == null) {
                throw new RuntimeException("failed to get build version file.");
            }
            Properties prop = new Properties();
            prop.load(input);
            return prop.getProperty("version");
        } catch (IOException ex) {
            ex.printStackTrace();
            throw new RuntimeException("failed to get build version in test: ", ex);
        }
    }

    private static final String workerIDRegEx = "\"jobCluster\":\"(.*?)\",\"jobId\":\"(.*?)\",\"workerIndex\":(\\d+),\"workerNum\":(\\d+)";
    private static final Pattern workerIdPattern = Pattern.compile(workerIDRegEx);
    public static Optional<String> getWorkerId(String workerIdResponse) {
        Matcher matcher = workerIdPattern.matcher(workerIdResponse);

        // Check if the pattern matches
        if (matcher.find()) {
            // Extract the fields
            String jobCluster = matcher.group(1);
            String jobId = matcher.group(2);
            int workerIndex = Integer.parseInt(matcher.group(3));
            int workerNum = Integer.parseInt(matcher.group(4));

            return Optional.of(jobId + "-" + workerIndex + "-" + workerNum);
        } else {
            return Optional.empty();
        }
    }

}
