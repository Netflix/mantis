/*
 *
 * Copyright 2020 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.mantisrx.common.compression;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import io.mantisrx.common.MantisServerSentEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;


public class CompressionUtils {

    public static final String MANTIS_SSE_DELIMITER = "$$$";
    public static final byte[] MANTIS_SSE_DELIMITER_BINARY = MANTIS_SSE_DELIMITER.getBytes();
    private static final Logger logger = LoggerFactory.getLogger(CompressionUtils.class);

    public static String compressAndBase64Encode(List<String> events, boolean useSnappy) {
        return compressAndBase64Encode(events, useSnappy, MANTIS_SSE_DELIMITER_BINARY);
    }

    public static String compressAndBase64Encode(List<String> events, boolean useSnappy, byte[] delimiter) {
        if (!events.isEmpty()) {

            StringBuilder sb = new StringBuilder();
            for (String event : events) {
                sb.append(event);
                sb.append(delimiter);
            }
            try {
                byte[] compressedBytes;
                if (useSnappy) {
                    compressedBytes = snappyCompressData(sb.toString());
                } else {
                    compressedBytes = gzipCompressData(sb.toString());
                }
                String encodedData = Base64.getEncoder().encodeToString(compressedBytes);
                if (logger.isDebugEnabled()) {
                    logger.debug("Encoded Data --> " + encodedData);
                }

                return encodedData;
            } catch (UnsupportedEncodingException e) {
                logger.warn("Error encoding messages:" + e.getMessage());

            } catch (IOException e) {
                logger.warn("Error encoding messages2:" + e.getMessage());

            }
        }
        return null;
    }


    public static String compressAndBase64Encode(List<String> events) {
        return compressAndBase64Encode(events, false);
    }

    public static byte[] compressAndBase64EncodeBytes(List<List<byte[]>> nestedEvents, boolean useSnappy) {
        return compressAndBase64EncodeBytes(nestedEvents, useSnappy, MANTIS_SSE_DELIMITER_BINARY);
    }

    public static byte[] compressAndBase64EncodeBytes(List<List<byte[]>> nestedEvents, boolean useSnappy, byte[] delimiter) {
        if (!nestedEvents.isEmpty()) {

            ByteBuffer buffer = ByteBuffer.allocate(getTotalByteSize(nestedEvents, delimiter));

            for (List<byte[]> outerList : nestedEvents) {
                for (byte[] event : outerList) {
                    buffer.put(event);
                    buffer.put(delimiter);
                }
            }

            try {
                byte[] compressedBytes;
                if (useSnappy) {
                    compressedBytes = snappyCompressData(buffer.array());
                } else {
                    compressedBytes = gzipCompressData(buffer.array());
                }
                String encodedData = Base64.getEncoder().encodeToString(compressedBytes);
                if (logger.isDebugEnabled()) { logger.debug("Encoded Data --> " + encodedData); }

                return encodedData.getBytes(StandardCharsets.UTF_8);
            } catch (UnsupportedEncodingException e) {
                logger.warn("Error encoding messages:" + e.getMessage());

            } catch (IOException e) {
                logger.warn("Error encoding messages2:" + e.getMessage());

            }
        }
        return null;
    }

    @Deprecated
    public static byte[] compressAndBase64EncodeBytes(List<List<byte[]>> nestedEvents) {
        if (!nestedEvents.isEmpty()) {

            ByteBuffer buffer = ByteBuffer.allocate(getTotalByteSize(nestedEvents, MANTIS_SSE_DELIMITER_BINARY));


            for (List<byte[]> outerList : nestedEvents) {
                for (byte[] event : outerList) {
                    buffer.put(event);
                    buffer.put(MANTIS_SSE_DELIMITER_BINARY);
                }
            }

            try {

                byte[] compressedBytes = gzipCompressData(buffer.array());
                String encodedData = Base64.getEncoder().encodeToString(compressedBytes);
                if (logger.isDebugEnabled()) { logger.debug("Encoded Data --> " + encodedData); }

                return encodedData.getBytes(StandardCharsets.UTF_8);
            } catch (UnsupportedEncodingException e) {
                logger.warn("Error encoding messages:" + e.getMessage());

            } catch (IOException e) {
                logger.warn("Error encoding messages2:" + e.getMessage());

            }
        }
        return null;
    }


    private static int getTotalByteSize(List<List<byte[]>> nestedEvents, byte[] delimiter) {
        int size = 0;
        int count = 0;
        for (List<byte[]> outerList : nestedEvents) {
            for (byte[] event : outerList) {
                count++;
                size += event.length;
            }
        }

        return size + count * delimiter.length;

    }


    public static List<MantisServerSentEvent> decompressAndBase64Decode_old(String encodedString, boolean isCompressedBinary) {
        encodedString = encodedString.trim();
        //	System.out.println("Inside client decompress Current thread -->" + Thread.currentThread().getName());
        if (!encodedString.isEmpty() && isCompressedBinary && !encodedString.startsWith("ping") && !encodedString.startsWith("{")) {
            if (logger.isDebugEnabled()) { logger.debug("decoding " + encodedString); }
            byte[] decoded = Base64.getDecoder().decode(encodedString);
            GZIPInputStream gis;
            try {
                gis = new GZIPInputStream(new ByteArrayInputStream(decoded));
                BufferedReader bf = new BufferedReader(new InputStreamReader(gis, StandardCharsets.UTF_8));
                String outStr = "";
                String line;
                while ((line = bf.readLine()) != null) {
                    outStr += line;
                }

                String[] toks = outStr.split("\\$\\$\\$");
                List<MantisServerSentEvent> msseList = new ArrayList<>();
                for (String tok : toks) {
                    msseList.add(new MantisServerSentEvent(tok));
                }
                //return Arrays.asList(new MantisServerSentEvent(toks));
                return msseList;

            } catch (IOException e) {
                logger.error(e.getMessage());
            }
            return new ArrayList<MantisServerSentEvent>();
        } else {
            List<MantisServerSentEvent> s = new ArrayList<MantisServerSentEvent>();
            s.add(new MantisServerSentEvent(encodedString));
            return s;
        }
    }

    static List<MantisServerSentEvent> tokenize(BufferedReader br) throws IOException {
        return tokenize(br, MANTIS_SSE_DELIMITER);
    }

    static List<MantisServerSentEvent> tokenize(BufferedReader bf, String delimiter) throws IOException {
        StringBuilder sb = new StringBuilder();
        String line;
        List<MantisServerSentEvent> msseList = new ArrayList<>();
        final int delimiterLength = delimiter.length();
        char[] delimiterArray = delimiter.toCharArray();

        int delimiterCount = 0;
        while ((line = bf.readLine()) != null) {
            for (int i = 0; i < line.length(); i++) {
                if (line.charAt(i) != delimiterArray[delimiterCount]) {
                    if (delimiterCount > 0) {
                        for (int j = 0; j < delimiterCount; ++j) {
                            sb.append(delimiterArray[j]);
                        }
                        delimiterCount = 0;
                    }
                    sb.append(line.charAt(i));
                } else {
                    delimiterCount++;
                }

                if (delimiterCount == delimiterLength) {
                    msseList.add(new MantisServerSentEvent(sb.toString()));
                    delimiterCount = 0;
                    sb = new StringBuilder();
                }
            }
        }

        // We have a trailing event.
        if (sb.length() > 0) {
            // We had a partial delimiter match which was not in the builder.
            if (delimiterCount > 0) {
                for (int j = 0; j < delimiterCount; ++j) {
                    sb.append(delimiter.charAt(j));
                }
            }
            msseList.add(new MantisServerSentEvent(sb.toString()));
        }
        return msseList;
    }

    static List<MantisServerSentEvent> tokenize_1(BufferedReader bf) throws IOException {
        StringBuilder sb = new StringBuilder();
        String line;
        List<MantisServerSentEvent> msseList = new ArrayList<>();

        String outStr = "";
        while ((line = bf.readLine()) != null) {
            sb.append(line);
        }
        int i = 0;
        outStr = sb.toString();

        sb = new StringBuilder();
        while (i < outStr.length()) {

            while (outStr.charAt(i) != '$') {
                sb.append(outStr.charAt(i));
                i++;
            }

            if (i + 3 < outStr.length()) {
                if (outStr.charAt(i) == '$' && outStr.charAt(i + 1) == '$' && outStr.charAt(i + 2) == '$') {
                    i += 3;
                    msseList.add(new MantisServerSentEvent(sb.toString()));
                    sb = new StringBuilder();
                }
            } else {
                sb.append(outStr.charAt(i));
                i++;
            }
        }

        return msseList;
    }

    static List<MantisServerSentEvent> tokenize_2(BufferedReader bf) throws IOException {
        StringBuilder sb = new StringBuilder();
        String line;
        List<MantisServerSentEvent> msseList = new ArrayList<>();

        String outStr = "";
        while ((line = bf.readLine()) != null) {
            sb.append(line);
        }

        outStr = sb.toString();

        String[] toks = outStr.split("\\$\\$\\$");

        for (String tok : toks) {
            msseList.add(new MantisServerSentEvent(tok));
        }
        return msseList;
    }

    public static List<MantisServerSentEvent> decompressAndBase64Decode(String encodedString,
                                                                        boolean isCompressedBinary,
                                                                        boolean useSnappy) {
        return decompressAndBase64Decode(encodedString, isCompressedBinary, useSnappy, null);
    }

    public static List<MantisServerSentEvent> decompressAndBase64Decode(String encodedString,
                                                                        boolean isCompressedBinary,
                                                                        boolean useSnappy,
                                                                        String delimiter) {
        encodedString = encodedString.trim();
        //	System.out.println("Inside client decompress Current thread -->" + Thread.currentThread().getName());
        if (!encodedString.isEmpty() && isCompressedBinary && !encodedString.startsWith("ping") && !encodedString.startsWith("{")) {
            if (logger.isDebugEnabled()) { logger.debug("decoding " + encodedString); }
            byte[] decoded = Base64.getDecoder().decode(encodedString);

            try {

                if (useSnappy) {
                    return delimiter == null
                            ? tokenize(snappyDecompress(decoded))
                            : tokenize(snappyDecompress(decoded), delimiter);
                } else {
                    return delimiter == null
                            ? tokenize(gzipDecompress(decoded))
                            : tokenize(gzipDecompress(decoded), delimiter);
                }

            } catch (IOException e) {
                logger.error(e.getMessage());
            }
            return new ArrayList<MantisServerSentEvent>();
        } else {
            List<MantisServerSentEvent> s = new ArrayList<MantisServerSentEvent>();
            s.add(new MantisServerSentEvent(encodedString));
            return s;
        }
    }

    @Deprecated
    public static List<MantisServerSentEvent> decompressAndBase64Decode(String encodedString, boolean isCompressedBinary) {
        return decompressAndBase64Decode(encodedString, isCompressedBinary, false);
    }

    static byte[] snappyCompressData(String data) throws IOException {
        return Snappy.compress(data);
    }

    static byte[] snappyCompressData(byte[] data) throws IOException {
        return Snappy.compress(data);
    }

    /*protected*/
    static byte[] gzipCompressData(String data) throws IOException {
        ByteArrayOutputStream obj = new ByteArrayOutputStream();
        GZIPOutputStream gzip = new GZIPOutputStream(obj);
        gzip.write(data.getBytes(StandardCharsets.UTF_8));
        gzip.close();
        byte[] compressedBytes = obj.toByteArray();
        return compressedBytes;
    }

    /*protected*/
    static byte[] gzipCompressData(byte[] data) throws IOException {
        ByteArrayOutputStream obj = new ByteArrayOutputStream();
        GZIPOutputStream gzip = new GZIPOutputStream(obj);
        gzip.write(data);
        gzip.close();
        byte[] compressedBytes = obj.toByteArray();
        return compressedBytes;
    }

    static BufferedReader snappyDecompress(byte[] data) throws IOException {
        byte[] decompressed = Snappy.uncompress(data);
        ByteArrayInputStream bais = new ByteArrayInputStream(decompressed);
        BufferedReader bf = new BufferedReader(new InputStreamReader(bais, StandardCharsets.UTF_8));
        return bf;
    }

    static BufferedReader gzipDecompress(byte[] data) throws IOException {
        GZIPInputStream gis = new GZIPInputStream(new ByteArrayInputStream(data));
        BufferedReader bf = new BufferedReader(new InputStreamReader(gis, StandardCharsets.UTF_8));
        return bf;
    }

    public static void main(String[] args) {
        String d = "{\"ip\":\"50.112.119.64\",\"count\":27}$$${\\\"ip\\\":\\\"50.112.119.64\\\",\\\"count\\\":27}";

        String e1 = "{\"ip\":\"11.112.119.64\",\"count\":27}";
        String e2 = "{\"ip\":\"22.111.112.62\",\"count\":27}";
        String e3 = "{\"ip\":\"33.222.112.62\",\"count\":27}";

        List<String> events = new ArrayList<>();
        events.add(e1);
        events.add(e2);
        events.add(e3);

        String encodedString = CompressionUtils.compressAndBase64Encode(events);

        List<MantisServerSentEvent> orig = CompressionUtils.decompressAndBase64Decode(encodedString, true);
        for (MantisServerSentEvent event : orig) {
            System.out.println("event -> " + event);
        }


        //		String d2 = "blah1$$$blah3$$$blah4";
        //		System.out.println("pos " + d2.indexOf("$$$"));
        //		String [] toks = d.split("\\$\\$\\$");
        //
        //		System.out.println("toks len" + toks.length);
        //		for(int i=0; i<toks.length; i++) {
        //			System.out.println("t -> " + toks[i]);
        //		}


    }

}
