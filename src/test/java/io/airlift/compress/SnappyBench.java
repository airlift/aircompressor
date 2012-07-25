/*
 * Copyright (C) 2011 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
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
package io.airlift.compress;

import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.common.primitives.Longs;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static io.airlift.compress.BenchmarkDriver.JAVA_BLOCK;
import static io.airlift.compress.BenchmarkDriver.JAVA_STREAM;
import static io.airlift.compress.BenchmarkDriver.JNI_BLOCK;
import static io.airlift.compress.BenchmarkDriver.JNI_STREAM;

/**
 * Port of the micro-benchmarks for  Snappy.
 * <p/>
 * Make sure to run these with the server version of hot spot.  I use the following configuration:
 * <pre>
 * {@code
 *   -Dorg.xerial.snappy.lib.name=libsnappyjava.jnilib -server -XX:+UseCompressedOops -Xms128M -Xmx128M -XX:+UseConcMarkSweepGC
 * }
 * </pre>
 */
public class SnappyBench
{
    private static final int NUMBER_OF_RUNS = 5;
    private static final int CALIBRATE_ITERATIONS = 100;
    private static final int WARM_UP_SECONDS = 45;
    private static final int SECONDS_PER_RUN = 1;

    public static void main(String[] args)
    {
        System.err.printf("Running micro-benchmarks.\n");

        SnappyBench snappyBench = new SnappyBench();

        // verify implementation with a round trip for every input
        snappyBench.verify();

        // warm up the code paths so hot spot optimizes the code
        snappyBench.warmUp();

        // Easy to use individual tests
//        for (int i = 0; i < 100; i++) {
//            snappyBench.runUncompress(TestData.txt1);
//            snappyBench.runUncompress(TestData.txt2);
//            snappyBench.runUncompress(TestData.txt3);
//            snappyBench.runUncompress(TestData.txt4);
//            snappyBench.runUncompress(TestData.sum);
//            snappyBench.runUncompress(TestData.lsp);
//            snappyBench.runUncompress(TestData.man);
//            snappyBench.runUncompress(TestData.c);
//            snappyBench.runUncompress(TestData.cp);
//        }

        snappyBench.runCompress("Block Compress", JNI_BLOCK, JAVA_BLOCK);
        snappyBench.runUncompress("Block Uncompress", JNI_BLOCK, JAVA_BLOCK);
        snappyBench.runRoundTrip("Block Round Trip", JNI_BLOCK, JAVA_BLOCK);

        snappyBench.runCompress("Stream Compress (no checksum)", JNI_STREAM, JAVA_STREAM);
        snappyBench.runUncompress("Stream Uncompress (no checksum)", JNI_STREAM, JAVA_STREAM);
        snappyBench.runRoundTrip("Stream RoundTrip (no checksum)", JNI_STREAM, JAVA_STREAM);
    }

    public void verify()
    {
        for (TestData testData : TestData.values()) {
            byte[] contents = testData.getContents();
            byte[] compressed = new byte[Snappy.maxCompressedLength(contents.length)];
            int compressedSize = Snappy.compress(contents, 0, contents.length, compressed, 0);

            byte[] uncompressed = new byte[contents.length];

            Snappy.uncompress(compressed, 0, compressedSize, uncompressed, 0);
            if (!Arrays.equals(uncompressed, testData.getContents())) {
                throw new AssertionError("Failed for " + testData);
            }

            Arrays.fill(uncompressed, (byte) 0);
            compressed = Arrays.copyOf(compressed, compressedSize);
            Snappy.uncompress(compressed, 0, compressedSize, uncompressed, 0);
            if (!Arrays.equals(uncompressed, testData.getContents())) {
                throw new AssertionError("Failed for " + testData);
            }


        }

        for (TestData testData : TestData.values()) {
            try {
                byte[] contents = testData.getContents();

                ByteArrayOutputStream rawOut = new ByteArrayOutputStream(Snappy.maxCompressedLength(contents.length));
                SnappyOutputStream out = new SnappyOutputStream(rawOut);
                out.write(contents);
                out.close();

                SnappyInputStream in = new SnappyInputStream(new ByteArrayInputStream(rawOut.toByteArray()));
                byte[] uncompressed = ByteStreams.toByteArray(in);

                if (!Arrays.equals(uncompressed, testData.getContents())) {
                    throw new AssertionError("Failed for " + testData);
                }
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    public void warmUp()
    {
        // Warm up the code
        {
            long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(WARM_UP_SECONDS);
            do {
                for (TestData testData : TestData.values()) {
                    benchmarkCompress(testData, JAVA_BLOCK, 100);
                }
            } while (System.nanoTime() < end);
            end = System.nanoTime() + TimeUnit.SECONDS.toNanos(WARM_UP_SECONDS);
            do {
                for (TestData testData : TestData.values()) {
                    benchmarkUncompress(testData, JAVA_BLOCK, 100);
                }
            } while (System.nanoTime() < end);
            end = System.nanoTime() + TimeUnit.SECONDS.toNanos(WARM_UP_SECONDS);
            do {
                for (TestData testData : TestData.values()) {
                    benchmarkCompress(testData, JAVA_STREAM, 100);
                }
            } while (System.nanoTime() < end);
            end = System.nanoTime() + TimeUnit.SECONDS.toNanos(WARM_UP_SECONDS);
            do {
                for (TestData testData : TestData.values()) {
                    benchmarkUncompress(testData, JAVA_STREAM, 100);
                }
            } while (System.nanoTime() < end);
        }

    }

    private static void printHeader(String benchmarkTitle)
    {
        System.err.println();
        System.err.println();
        System.err.println("### " +benchmarkTitle);
        System.err.println("<pre><code>");
        System.err.printf("%-8s %8s %9s %9s %11s %11s %7s\n",
                "",
                "",
                "JNI",
                "Java",
                "JNI",
                "Java",
                "");
        System.err.printf("%-8s %8s %9s %9s %11s %11s %7s\n",
                "Input",
                "Size",
                "Compress",
                "Compress",
                "Throughput",
                "Throughput",
                "Change");
        System.err.printf("---------------------------------------------------------------------\n");
    }

    private static void printFooter()
    {
        System.err.println("</code></pre>");
    }

    public void runCompress(String benchmarkTitle, BenchmarkDriver oldDriver, BenchmarkDriver newDriver)
    {
        printHeader(benchmarkTitle);
        for (TestData testData : TestData.values()) {
            runCompress(testData, oldDriver, newDriver);
        }
        printFooter();
    }

    private void runCompress(TestData testData, BenchmarkDriver oldDriver, BenchmarkDriver newDriver)
    {
        long iterations = calibrateIterations(testData, oldDriver, true);

        long oldBytesPerSecond = benchmarkCompress(testData, oldDriver, iterations);
        long newBytesPerSecond = benchmarkCompress(testData, newDriver, iterations);

        // results
        String oldHumanReadableSpeed = toHumanReadableSpeed(oldBytesPerSecond);
        String newHumanReadableSpeed = toHumanReadableSpeed(newBytesPerSecond);
        double improvement = 100.0d * (newBytesPerSecond - oldBytesPerSecond) / oldBytesPerSecond;

        System.err.printf(
                "%-8s %8d %8.1f%% %8.1f%% %11s %11s %+6.1f%%  %s\n",
                testData,
                testData.size(),
                oldDriver.getCompressionRatio(testData) * 100.0,
                newDriver.getCompressionRatio(testData) * 100.0,
                oldHumanReadableSpeed,
                newHumanReadableSpeed,
                improvement,
                testData.getInfo());
    }

    private long benchmarkCompress(TestData testData, BenchmarkDriver driver, long iterations)
    {
        long[] firstBenchmarkRuns = new long[NUMBER_OF_RUNS];
        for (int run = 0; run < NUMBER_OF_RUNS; ++run) {
            firstBenchmarkRuns[run] = driver.compress(testData, iterations);
        }
        long firstMedianTimeInNanos = getMedianValue(firstBenchmarkRuns);
        return (long) (1.0 * iterations * testData.size() / nanosToSeconds(firstMedianTimeInNanos));
    }

    public void runUncompress(String benchmarkTitle, BenchmarkDriver oldDriver, BenchmarkDriver newDriver)
    {
        printHeader(benchmarkTitle);
        for (TestData testData : TestData.values()) {
            runUncompress(testData, oldDriver, newDriver);
        }
        printFooter();
    }

    private void runUncompress(TestData testData, BenchmarkDriver oldDriver, BenchmarkDriver newDriver)
    {
        long iterations = calibrateIterations(testData, oldDriver, false);

        long oldBytesPerSecond = benchmarkUncompress(testData, oldDriver, iterations);
        long newBytesPerSecond = benchmarkUncompress(testData, newDriver, iterations);

        // results
        String newHumanReadableSpeed = toHumanReadableSpeed(newBytesPerSecond);
        String oldHumanReadableSpeed = toHumanReadableSpeed(oldBytesPerSecond);
        double improvement = 100.0d * (newBytesPerSecond - oldBytesPerSecond) / oldBytesPerSecond;

        System.err.printf(
                "%-8s %8d %8.1f%% %8.1f%% %11s %11s %+6.1f%%  %s\n",
                testData,
                testData.size(),
                oldDriver.getCompressionRatio(testData) * 100.0,
                newDriver.getCompressionRatio(testData) * 100.0,
                oldHumanReadableSpeed,
                newHumanReadableSpeed,
                improvement,
                testData.getInfo());
    }

    private long benchmarkUncompress(TestData testData, BenchmarkDriver driver, long iterations)
    {
        long[] jniBenchmarkRuns = new long[NUMBER_OF_RUNS];
        for (int run = 0; run < NUMBER_OF_RUNS; ++run) {
            jniBenchmarkRuns[run] = driver.uncompress(testData, iterations);
        }
        long jniMedianTimeInNanos = getMedianValue(jniBenchmarkRuns);
        return (long) (1.0 * iterations * testData.size() / nanosToSeconds(jniMedianTimeInNanos));
    }

    public void runRoundTrip(String benchmarkTitle, BenchmarkDriver oldDriver, BenchmarkDriver newDriver)
    {
        printHeader(benchmarkTitle);
        for (TestData testData : TestData.values()) {
            runRoundTrip(testData, oldDriver, newDriver);
        }
        printFooter();
    }

    private void runRoundTrip(TestData testData, BenchmarkDriver oldDriver, BenchmarkDriver newDriver)
    {
        long iterations = calibrateIterations(testData, oldDriver, true);

        long oldBytesPerSecond = benchmarkRoundTrip(testData, oldDriver, iterations);
        long newBytesPerSecond = benchmarkRoundTrip(testData, newDriver, iterations);

        // results
        String newHumanReadableSpeed = toHumanReadableSpeed(newBytesPerSecond);
        String oldHumanReadableSpeed = toHumanReadableSpeed(oldBytesPerSecond);
        double improvement = 100.0d * (newBytesPerSecond - oldBytesPerSecond) / oldBytesPerSecond;

        System.err.printf(
                "%-8s %8d %8.1f%% %8.1f%% %11s %11s %+6.1f%%  %s\n",
                testData,
                testData.size(),
                oldDriver.getCompressionRatio(testData) * 100.0,
                newDriver.getCompressionRatio(testData) * 100.0,
                oldHumanReadableSpeed,
                newHumanReadableSpeed,
                improvement,
                testData.getInfo());
    }

    private long benchmarkRoundTrip(TestData testData, BenchmarkDriver driver, long iterations)
    {
        long[] jniBenchmarkRuns = new long[NUMBER_OF_RUNS];
        for (int run = 0; run < NUMBER_OF_RUNS; ++run) {
            jniBenchmarkRuns[run] = driver.roundTrip(testData, iterations);
        }
        long jniMedianTimeInNanos = getMedianValue(jniBenchmarkRuns);
        return (long) (1.0 * iterations * testData.size() / nanosToSeconds(jniMedianTimeInNanos));
    }

    private long calibrateIterations(TestData testData, BenchmarkDriver driver, boolean compression)
    {
        // Run a few iterations first to find out approximately how fast
        // the benchmark is.
        long start = System.nanoTime();
        if (compression) {
            driver.compress(testData, CALIBRATE_ITERATIONS);
        }
        else {
            driver.uncompress(testData, CALIBRATE_ITERATIONS);
        }
        long timeInNanos = System.nanoTime() - start;

        // Let each test case run for about 200ms, but at least as many
        // as we used to calibrate.
        // Run five times and pick the median.
        long iterations = 0;
        if (timeInNanos > 0) {
            double iterationsPerSecond = CALIBRATE_ITERATIONS / nanosToSeconds(timeInNanos);
            iterations = (long) (SECONDS_PER_RUN * iterationsPerSecond);
        }
        iterations = Math.max(iterations, CALIBRATE_ITERATIONS);
        return iterations;
    }

    private double nanosToSeconds(long nanos)
    {
        return 1.0 * nanos / TimeUnit.SECONDS.toNanos(1);
    }

    private String toHumanReadableSpeed(long bytesPerSecond)
    {
        String humanReadableSpeed;
        if (bytesPerSecond < 1024) {
            humanReadableSpeed = format("%dB/s", bytesPerSecond);
        }
        else if (bytesPerSecond < 1024 * 1024) {
            humanReadableSpeed = format("%.1fkB/s", bytesPerSecond / 1024.0f);
        }
        else if (bytesPerSecond < 1024 * 1024 * 1024) {
            humanReadableSpeed = format("%.1fMB/s", bytesPerSecond / (1024.0f * 1024.0f));
        }
        else {
            humanReadableSpeed = format("%.1fGB/s", bytesPerSecond / (1024.0f * 1024.0f * 1024.0f));
        }
        return humanReadableSpeed;
    }

    private long getMedianValue(long[] benchmarkRuns)
    {
        ArrayList<Long> list = new ArrayList<Long>(Longs.asList(benchmarkRuns));
        Collections.sort(list);
        return list.get(benchmarkRuns.length / 2);
    }

    @SuppressWarnings({"UnusedDeclaration"})
    public enum TestData {
        html("html"),
        urls("urls.10K"),
        jpg("house.jpg", false),
        pdf("mapreduce-osdi-1.pdf"),
        html4("html_x_4"),
        cp("cp.html"),
        c("fields.c"),
        lsp("grammar.lsp"),
        xls("kennedy.xls"),
        txt1("alice29.txt"),
        txt2("asyoulik.txt"),
        txt3("lcet10.txt"),
        txt4("plrabn12.txt"),
        bin("ptt5"),
        sum("sum"),
        man("xargs.1"),
        pb("geo.protodata"),
        gaviota("kppkn.gtb");


        private final String fileName;
        private final boolean compressibleData;
        private final byte[] contents;
        private final byte[] compressed;

        TestData(String fileName)
        {
            this(fileName, true);
        }

        TestData(String fileName, boolean compressibleData)
        {
            this.fileName = fileName;
            this.compressibleData = compressibleData;
            try {
                contents = Files.toByteArray(new File("testdata", fileName));
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }


            // Read the file and create buffers out side of timing
            byte[] compressed = new byte[Snappy.maxCompressedLength(contents.length)];
            int compressedSize;
            try {
                compressedSize = org.xerial.snappy.Snappy.compress(contents, 0, contents.length, compressed, 0);
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
            this.compressed = Arrays.copyOf(compressed, compressedSize);
        }

        public String getFileName()
        {
            return fileName;
        }

        public boolean isCompressibleData()
        {
            return compressibleData;
        }

        public String getInfo()
        {
            if (compressibleData) {
                return name();
            } else {
                return name() + " (not compressible)";
            }
        }

        public byte[] getContents()
        {
            return Arrays.copyOf(contents, contents.length);
        }

        public int size()
        {
            return contents.length;
        }

        public byte[] getCompressed()
        {
            return Arrays.copyOf(compressed, compressed.length);
        }

        public int compressedSize()
        {
            return compressed.length;
        }
    }
}
