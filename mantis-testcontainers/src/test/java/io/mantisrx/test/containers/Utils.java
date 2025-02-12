package io.mantisrx.test.containers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

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

}
