package io.reactivx.common.compression;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import io.mantisrx.runtime.common.compression.CompressionUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;


public class CompressionUtilsBenchmark {

    private static final Random random = new Random();

    //
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.MINUTES)
    @Warmup(iterations = 10, time = 3, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 50, time = 3, timeUnit = TimeUnit.SECONDS)
    @Threads(1)
    public void testBasicStringSplit(Blackhole blackhole, MyState state) throws IOException {
        BufferedReader bf = new BufferedReader(new StringReader(state.eventListStr));

        StringBuilder sb = new StringBuilder();
        String line;
        List<String> msseList = new ArrayList<>();
        int dollarCnt = 0;
        while ((line = bf.readLine()) != null) {
            for (int i = 0; i < line.length(); i++) {
                if (dollarCnt == 3) {
                    msseList.add(sb.toString());
                    dollarCnt = 0;
                    sb = new StringBuilder();
                }
                if (line.charAt(i) != '$') {
                    sb.append(line.charAt(i));
                } else {
                    dollarCnt++;
                }
            }

        }
        blackhole.consume(msseList);
        //blackhole.consume(state.eventListStr.split("$$"));
        //state.sum = state.a + state.b;
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @Warmup(iterations = 10, time = 3, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 50, time = 3, timeUnit = TimeUnit.SECONDS)
    @Threads(1)
    public void testBuiltInStringSplit(Blackhole blackhole, MyState state) throws IOException {
        BufferedReader bf = new BufferedReader(new StringReader(state.eventListStr));
        String line;
        List<String> msseList = new ArrayList<>();

        StringBuilder outStrB = new StringBuilder();
        while ((line = bf.readLine()) != null) {
            outStrB.append(line);
        }
        String[] toks = outStrB.toString().split("\\$\\$\\$");

        for (String tok : toks) {
            msseList.add(tok);
        }

        blackhole.consume(msseList);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @Warmup(iterations = 10, time = 3, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 50, time = 3, timeUnit = TimeUnit.SECONDS)
    @Threads(1)
    public void testSnappyCompress(Blackhole blackhole, MyState state) throws IOException {
        blackhole.consume(CompressionUtils.compressAndBase64Encode(state.eventList, true));
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @Warmup(iterations = 10, time = 3, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 50, time = 3, timeUnit = TimeUnit.SECONDS)
    @Threads(1)
    public void testSnappyDeCompress(Blackhole blackhole, MyState state) throws IOException {
        blackhole.consume(CompressionUtils.decompressAndBase64Decode(state.snappyCompressed, true, true));
    }
    //

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @Warmup(iterations = 10, time = 3, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 50, time = 3, timeUnit = TimeUnit.SECONDS)
    @Threads(1)
    public void testGzipCompress(Blackhole blackhole, MyState state) throws IOException {
        blackhole.consume(CompressionUtils.compressAndBase64Encode(state.eventList));
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @Warmup(iterations = 10, time = 3, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 50, time = 3, timeUnit = TimeUnit.SECONDS)
    @Threads(1)
    public void testGzipDeCompress(Blackhole blackhole, MyState state) throws IOException {
        blackhole.consume(CompressionUtils.decompressAndBase64Decode(state.gzipCompressed, true));
    }

    public static class RandomString {

        private static final char[] symbols;

        static {
            StringBuilder tmp = new StringBuilder();
            for (char ch = '0'; ch <= '9'; ++ch)
                tmp.append(ch);
            for (char ch = 'a'; ch <= 'z'; ++ch)
                tmp.append(ch);
            symbols = tmp.toString().toCharArray();
        }


        private final char[] buf;

        public RandomString(int length) {
            if (length < 1)
                throw new IllegalArgumentException("length < 1: " + length);
            buf = new char[length];
        }

        public String nextString() {
            for (int idx = 0; idx < buf.length; ++idx)
                buf[idx] = symbols[random.nextInt(symbols.length)];
            return new String(buf);
        }
    }

    @State(Scope.Thread)
    public static class MyState {

        public String eventListStr;
        public List<String> eventList = new ArrayList<>();
        public String snappyCompressed;
        public String gzipCompressed;

        @Setup(Level.Trial)
        public void doSetup() {
            RandomString rs = new RandomString(200);
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < random.nextInt(50); i++) {
                String s = rs.nextString();
                eventList.add(s);
                sb.append(s);
                sb.append("$$$");
            }

            snappyCompressed = CompressionUtils.compressAndBase64Encode(eventList, true);
            gzipCompressed = CompressionUtils.compressAndBase64Encode(eventList);
            eventListStr = sb.toString();

            System.out.println("Do Setup");
        }

        @TearDown(Level.Trial)
        public void doTearDown() {
            System.out.println("Do TearDown");
        }


    }
}
