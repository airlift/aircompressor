package org.iq80.snappy;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.google.common.primitives.Longs;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static org.iq80.snappy.SnappyBench.TestSuite.Compress;
import static org.iq80.snappy.SnappyBench.TestSuite.Uncompress;

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

    public static void main(String[] args)
    {
        System.err.printf("Running micro-benchmarks.\n");

        System.err.printf("%-18s %10s %10s %10s %10s\n",
                "Benchmark",
                "Size",
                "Time(ns)",
                "Iterations",
                "Throughput");
        System.err.printf("--------------------------------------------------------------\n");

        SnappyBench snappyBench = new SnappyBench();

        // verify implementation with a round trip for every input
        snappyBench.verify();

        // warm up the code paths so hotspot inlines the code
        snappyBench.warmUp();

        // Easy to use individual tests
//        TestSuite testSuite = Uncompress;
//        for (int i = 0; i < 100; i++) {
//            snappyBench.run(testSuite, "jpg", 0);
//
//            snappyBench.run(testSuite, "lsp", 0);
////            snappyBench.run(testSuite, "bin", 0);
//            snappyBench.run(testSuite, "man", 0);
//            snappyBench.run(testSuite, "c", 0);
//            snappyBench.run(testSuite, "cp", 0);
//
//            // ok
////            snappyBench.run(testSuite, "xls", 0);
//
//            // bad
////            snappyBench.run(testSuite, "txt1", 0);
////            snappyBench.run(testSuite, "txt2", 0);
////            snappyBench.run(testSuite, "txt3", 0);
//            snappyBench.run(testSuite, "txt4", 0);
//            snappyBench.run(testSuite, "sum", 0);
//        }

//        snappyBench.run(Compress);
        snappyBench.run(Uncompress);

    }

    public static enum TestSuite
    {
        Compress, Uncompress
    }

    public static final Map<String, String> files = ImmutableMap.<String, String>builder()
            .put("html", "html")
            .put("urls", "urls.10K")
            .put("jpg", "house.jpg")
            .put("pdf", "mapreduce-osdi-1.pdf")
            .put("html4", "html_x_4")
            .put("cp", "cp.html")
            .put("c", "fields.c")
            .put("lsp", "grammar.lsp")
            .put("xls", "kennedy.xls")
            .put("txt1", "alice29.txt")
            .put("txt2", "asyoulik.txt")
            .put("txt3", "lcet10.txt")
            .put("txt4", "plrabn12.txt")
            .put("bin", "ptt5")
            .put("sum", "sum")
            .put("man", "xargs.1")
            .put("pb", "geo.protodata")
            .put("gaviota", "kppkn.gtb")
            .build();

    private boolean benchmarkRunning;
    private long benchmarkStart;
    private long benchmarkRealTimeUsec;
    private String benchmarkLabel;
    private long benchmarkBytesProcessed;

    public void verify()
    {
        for (String testName : files.keySet()) {
            byte[] contents = readTestDataFile(files.get(testName));
            byte[] compressed = new byte[Snappy.maxCompressedLength(contents.length)];
            int compressedSize = Snappy.compress(contents, 0, contents.length, compressed, 0);

            byte[] uncompressed = new byte[contents.length];

            Snappy.uncompress(compressed, 0, compressedSize, uncompressed, 0);
            if (!Arrays.equals(uncompressed, contents)) {
                throw new AssertionError("Failed for " + testName);
            }

            Arrays.fill(uncompressed, (byte) 0);
            compressed = Arrays.copyOf(compressed, compressedSize);
            Snappy.uncompress(compressed, 0, compressedSize, uncompressed, 0);
            if (!Arrays.equals(uncompressed, contents)) {
                throw new AssertionError("Failed for " + testName);
            }
        }
    }

    public void warmUp()
    {
        // Warm up the code
        {
            long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(WARM_UP_SECONDS);
//            do {
//                for (String testName : files.keySet()) {
//                    benchmarkCompress(100, testName, false);
//                }
//            } while (System.nanoTime() < end);
//            end = System.nanoTime() + TimeUnit.SECONDS.toNanos(WARM_UP_SECONDS);
            do {
                for (String testName : files.keySet()) {
                    benchmarkUncompress(100, testName, false);
                }
            } while (System.nanoTime() < end);
        }

    }

    public void run(TestSuite testSuite)
    {
        int testNumber = 0;
        for (String testName : files.keySet()) {
            for (boolean useJni : ImmutableList.of(false, true)) {
                run(testSuite, testName, testNumber, useJni);
            }
            System.err.println();
            testNumber++;
        }
    }

    private void run(TestSuite testSuite, String testName, int testNumber)
    {
        run(testSuite, testName, testNumber, false);
        run(testSuite, testName, testNumber, true);
        System.err.println();
    }

    private void run(TestSuite testSuite, String testName, int testNumber, boolean useJni)
    {
        // Run a few iterations first to find out approximately how fast
        // the benchmark is.
        resetBenchmarkTiming();
        startBenchmarkTiming();
        if (testSuite == Compress) {
            benchmarkCompress(CALIBRATE_ITERATIONS, testName, useJni);
        }
        else if (testSuite == Uncompress) {
            benchmarkUncompress(CALIBRATE_ITERATIONS, testName, useJni);
        }
        else {
            throw new AssertionError("Unexpected testSuite " + testSuite);
        }
        stopBenchmarkTiming();

        // Let each test case run for about 200ms, but at least as many
        // as we used to calibrate.
        // Run five times and pick the median.
        long iterations = 0;
        if (benchmarkRealTimeUsec > 0) {
            iterations = 400000 * CALIBRATE_ITERATIONS / benchmarkRealTimeUsec;
        }
        iterations = Math.max(iterations, CALIBRATE_ITERATIONS);
        long[] benchmarkRuns = new long[NUMBER_OF_RUNS];


        for (int run = 0; run < NUMBER_OF_RUNS; ++run) {
            resetBenchmarkTiming();
            startBenchmarkTiming();

            if (testSuite == Compress) {
                benchmarkCompress(iterations, testName, useJni);
            }
            else if (testSuite == Uncompress) {
                benchmarkUncompress(iterations, testName, useJni);
            }
            else {
                throw new AssertionError("Unexpected testSuite " + testSuite);
            }

            stopBenchmarkTiming();

            benchmarkRuns[run] = benchmarkRealTimeUsec;
        }

        long realTimeUs = getMedianValue(benchmarkRuns);
        long bytesPerSecond = benchmarkBytesProcessed * 1000000 / realTimeUs;

        String heading;
        if (useJni) {
            heading = format("%s/%d/jni", testSuite, testNumber);
        }
        else {
            heading = format("%s/%d/java", testSuite, testNumber);
        }
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

        int size = readTestDataFile(files.get(testName)).length;
        System.err.printf(
                "%-18s %10d %10d %10d %10s  %s\n",
                heading,
                size,
                (realTimeUs * 1000 / iterations),
                iterations,
                humanReadableSpeed,
                benchmarkLabel);
    }

    private long getMedianValue(long[] benchmarkRuns)
    {
        ArrayList<Long> list = new ArrayList<Long>(Longs.asList(benchmarkRuns));
        Collections.sort(list);
        return list.get(benchmarkRuns.length / 2);
    }

    public void benchmarkCompress(long iterations, String suite, boolean useJni)
    {
        stopBenchmarkTiming();

        // Read the file and create buffers out side of timing
        byte[] contents = readTestDataFile(files.get(suite));
        byte[] compressed = new byte[Snappy.maxCompressedLength(contents.length)];

        setBenchmarkBytesProcessed(iterations * contents.length);

        // compress the data
        int compressedSize;
        if (useJni) {
            startBenchmarkTiming();
            compressedSize = 0;
            while (iterations-- > 0) {
                try {
                    compressedSize = org.xerial.snappy.Snappy.compress(contents, 0, contents.length, compressed, 0);
                }
                catch (IOException e) {
                    throw Throwables.propagate(e);
                }
            }
            stopBenchmarkTiming();
        }
        else {
            startBenchmarkTiming();
            compressedSize = 0;
            while (iterations-- > 0) {
                compressedSize = Snappy.compress(contents, 0, contents.length, compressed, 0);
            }
            stopBenchmarkTiming();
        }

        double compressionRatio = compressedSize / Math.max(1.0, contents.length);
        setBenchmarkLabel(format("%s (%.2f %%)", suite, 100.0 * compressionRatio));
    }

    public void benchmarkUncompress(long iterations, String suite, boolean useJni)
    {
        stopBenchmarkTiming();

        // Read the file and create buffers out side of timing
        byte[] contents = readTestDataFile(files.get(suite));
        byte[] compressed = new byte[Snappy.maxCompressedLength(contents.length)];
        int compressedSize = Snappy.compress(contents, 0, contents.length, compressed, 0);

        byte[] uncompressed = new byte[contents.length];

        // based in uncompressed size
        setBenchmarkBytesProcessed(iterations * contents.length);

        setBenchmarkLabel(suite);
        if (useJni) {
            startBenchmarkTiming();
            while (iterations-- > 0) {
                try {
                    org.xerial.snappy.Snappy.uncompress(compressed, 0, compressedSize, uncompressed, 0);
                }
                catch (IOException e) {
                    throw Throwables.propagate(e);
                }
            }
            stopBenchmarkTiming();
        }
        else {
            startBenchmarkTiming();
            while (iterations-- > 0) {
                Snappy.uncompress(compressed, 0, compressedSize, uncompressed, 0);
            }
            stopBenchmarkTiming();
        }
        if (!Arrays.equals(uncompressed, contents)) {
            throw new AssertionError(String.format(
                    "Actual   : %s\n" +
                            "Expected : %s",
                    Arrays.toString(uncompressed),
                    Arrays.toString(contents)));
        }
    }

    private void resetBenchmarkTiming()
    {
        benchmarkRealTimeUsec = 0;
    }

    private void startBenchmarkTiming()
    {
        benchmarkRunning = true;
        benchmarkStart = System.nanoTime();
    }

    private void stopBenchmarkTiming()
    {
        if (!benchmarkRunning) {
            return;
        }

        long end = System.nanoTime();
        benchmarkRealTimeUsec = TimeUnit.NANOSECONDS.toMicros(end - benchmarkStart);

        benchmarkRunning = false;
    }

    private void setBenchmarkLabel(String benchmarkLabel)
    {
        this.benchmarkLabel = benchmarkLabel;
    }

    private void setBenchmarkBytesProcessed(long benchmarkBytesProcessed)
    {
        this.benchmarkBytesProcessed = benchmarkBytesProcessed;
    }

    private byte[] readTestDataFile(String fileName)
    {
        try {
            return Files.toByteArray(new File("testdata", fileName));
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}
