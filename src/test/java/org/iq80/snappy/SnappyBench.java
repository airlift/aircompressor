package org.iq80.snappy;

import com.google.common.base.Throwables;
import com.google.common.io.Files;
import com.google.common.primitives.Longs;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

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

        snappyBench.runCompress();
        snappyBench.runUncompress();
    }

    private long benchmarkBytesProcessed;

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
    }

    public void warmUp()
    {
        // Warm up the code
        {
            long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(WARM_UP_SECONDS);
            do {
                for (TestData testData : TestData.values()) {
                    benchmarkCompressJava(testData, 100);
                }
            } while (System.nanoTime() < end);
            end = System.nanoTime() + TimeUnit.SECONDS.toNanos(WARM_UP_SECONDS);
            do {
                for (TestData testData : TestData.values()) {
                    benchmarkUncompressJava(testData, 100);
                }
            } while (System.nanoTime() < end);
        }

    }

    public void runCompress()
    {
        System.err.println();
        System.err.printf("%-12s %10s %10s %10s %10s %8s\n",
                "Benchmark",
                "Size",
                "Compress",
                "JNI",
                "Java",
                "Change");
        System.err.printf("-----------------------------------------------------------------\n");

        for (TestData testData : TestData.values()) {
            runCompress(testData);
        }
    }

    private void runCompress(TestData testData)
    {
        long iterations = calibrateIterations(testData, true);

        // JNI
        long[] jniBenchmarkRuns = new long[NUMBER_OF_RUNS];
        for (int run = 0; run < NUMBER_OF_RUNS; ++run) {
            jniBenchmarkRuns[run] = benchmarkCompressJava(testData, iterations);
        }
        long jniMedianTimeInNanos = getMedianValue(jniBenchmarkRuns);
        long jniBytesPerSecond = (long) (1.0 * benchmarkBytesProcessed / nanosToSeconds(jniMedianTimeInNanos));

        // Java
        long[] javaBenchmarkRuns = new long[NUMBER_OF_RUNS];
        for (int run = 0; run < NUMBER_OF_RUNS; ++run) {
            javaBenchmarkRuns[run] = benchmarkCompressJni(testData, iterations);
        }
        long javaMedianTimeInNanos = getMedianValue(javaBenchmarkRuns);
        long javaBytesPerSecond = (long) (1.0 * benchmarkBytesProcessed / nanosToSeconds(javaMedianTimeInNanos));

        // results
        String heading = format("Compress/%d", testData.ordinal());
        String javaHumanReadableSpeed = toHumanReadableSpeed(javaBytesPerSecond);
        String jniHumanReadableSpeed = toHumanReadableSpeed(jniBytesPerSecond);
        double improvement = 100.0d * (jniBytesPerSecond - javaBytesPerSecond) / jniBytesPerSecond;

        System.err.printf(
                "%-12s %10d %+9.1f%% %10s %10s %+7.1f%%  %s\n",
                heading,
                testData.size(),
                testData.getCompressionRatio() * 100.0,
                javaHumanReadableSpeed,
                jniHumanReadableSpeed,
                improvement,
                testData);
    }

    public long benchmarkCompressJni(TestData testData, long iterations)
    {
        // Read the file and create buffers out side of timing
        byte[] contents = testData.getContents();
        byte[] compressed = new byte[Snappy.maxCompressedLength(contents.length)];

        // based on compressed size
        setBenchmarkBytesProcessed(iterations * contents.length);

        long start = System.nanoTime();
        while (iterations-- > 0) {
            try {
                org.xerial.snappy.Snappy.compress(contents, 0, contents.length, compressed, 0);
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
        long timeInNanos = System.nanoTime() - start;

        return timeInNanos;
    }

    public long benchmarkCompressJava(TestData testData, long iterations)
    {
        // Read the file and create buffers out side of timing
        byte[] contents = testData.getContents();
        byte[] compressed = new byte[Snappy.maxCompressedLength(contents.length)];

        // based on compressed size
        setBenchmarkBytesProcessed(iterations * contents.length);

        long start = System.nanoTime();
        while (iterations-- > 0) {
            Snappy.compress(contents, 0, contents.length, compressed, 0);
        }
        long timeInNanos = System.nanoTime() - start;

        return timeInNanos;
    }

    public void runUncompress()
    {
        System.err.println();
        System.err.printf("%-12s %10s %10s %10s %10s %8s\n",
                "Benchmark",
                "Size",
                "Compress",
                "JNI",
                "Java",
                "Change");
        System.err.printf("-----------------------------------------------------------------\n");

        for (TestData testData : TestData.values()) {
            runUncompress(testData);
        }
    }

    private void runUncompress(TestData testData)
    {
        long iterations = calibrateIterations(testData, false);

        // JNI
        long[] jniBenchmarkRuns = new long[NUMBER_OF_RUNS];
        for (int run = 0; run < NUMBER_OF_RUNS; ++run) {
            jniBenchmarkRuns[run] = benchmarkUncompressJava(testData, iterations);
        }
        long jniMedianTimeInNanos = getMedianValue(jniBenchmarkRuns);
        long jniBytesPerSecond = (long) (1.0 * benchmarkBytesProcessed / nanosToSeconds(jniMedianTimeInNanos));

        // Java
        long[] javaBenchmarkRuns = new long[NUMBER_OF_RUNS];
        for (int run = 0; run < NUMBER_OF_RUNS; ++run) {
            javaBenchmarkRuns[run] = benchmarkUncompressJni(testData, iterations);
        }
        long javaMedianTimeInNanos = getMedianValue(javaBenchmarkRuns);
        long javaBytesPerSecond = (long) (1.0 * benchmarkBytesProcessed / nanosToSeconds(javaMedianTimeInNanos));

        // results
        String heading = format("Compress/%d", testData.ordinal());
        String javaHumanReadableSpeed = toHumanReadableSpeed(javaBytesPerSecond);
        String jniHumanReadableSpeed = toHumanReadableSpeed(jniBytesPerSecond);
        double improvement = 100.0d * (jniBytesPerSecond - javaBytesPerSecond) / jniBytesPerSecond;

        System.err.printf(
                "%-12s %10d %+9.1f%% %10s %10s %+7.1f%%  %s\n",
                heading,
                testData.size(),
                testData.getCompressionRatio() * 100.0,
                javaHumanReadableSpeed,
                jniHumanReadableSpeed,
                improvement,
                testData);
    }

    public long benchmarkUncompressJni(TestData testData, long iterations)
    {
        // Read the file and create buffers out side of timing
        byte[] contents = testData.getContents();
        byte[] compressed = new byte[Snappy.maxCompressedLength(contents.length)];
        int compressedSize = Snappy.compress(contents, 0, contents.length, compressed, 0);

        byte[] uncompressed = new byte[contents.length];

        // based on uncompressed size
        setBenchmarkBytesProcessed(iterations * contents.length);

        long start = System.nanoTime();
        while (iterations-- > 0) {
            try {
                org.xerial.snappy.Snappy.uncompress(compressed, 0, compressedSize, uncompressed, 0);
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
        long timeInNanos = System.nanoTime() - start;

        // verify results
        if (!Arrays.equals(uncompressed, testData.getContents())) {
            throw new AssertionError(String.format(
                    "Actual   : %s\n" +
                            "Expected : %s",
                    Arrays.toString(uncompressed),
                    Arrays.toString(testData.getContents())));
        }

        return timeInNanos;
    }

    public long benchmarkUncompressJava(TestData testData, long iterations)
    {
        // Read the file and create buffers out side of timing
        byte[] contents = testData.getContents();
        byte[] compressed = new byte[Snappy.maxCompressedLength(contents.length)];
        int compressedSize = Snappy.compress(contents, 0, contents.length, compressed, 0);

        byte[] uncompressed = new byte[contents.length];

        // based on uncompressed size
        setBenchmarkBytesProcessed(iterations * contents.length);

        long start = System.nanoTime();
        while (iterations-- > 0) {
            Snappy.uncompress(compressed, 0, compressedSize, uncompressed, 0);
        }
        long timeInNanos = System.nanoTime() - start;

        // verify results
        if (!Arrays.equals(uncompressed, testData.getContents())) {
            throw new AssertionError(String.format(
                    "Actual   : %s\n" +
                            "Expected : %s",
                    Arrays.toString(uncompressed),
                    Arrays.toString(testData.getContents())));
        }

        return timeInNanos;
    }

    private long calibrateIterations(TestData testData, boolean compression)
    {
        // Run a few iterations first to find out approximately how fast
        // the benchmark is.
        long start = System.nanoTime();
        if (compression) {
            benchmarkCompressJni(testData, CALIBRATE_ITERATIONS);
        }
        else {
            benchmarkUncompressJni(testData, CALIBRATE_ITERATIONS);
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

    private void setBenchmarkBytesProcessed(long benchmarkBytesProcessed)
    {
        this.benchmarkBytesProcessed = benchmarkBytesProcessed;
    }

    @SuppressWarnings({"UnusedDeclaration"})
    public enum TestData {
        html("html"),
        urls("urls.10K"),
        jpg("house.jpg"),
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
        private final byte[] contents;
        private final byte[] compressed;
        private final double compressionRatio;

        TestData(String fileName)
        {
            this.fileName = fileName;
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
            compressionRatio = 1.0 * (contents.length - compressedSize) / contents.length;

        }

        public String getFileName()
        {
            return fileName;
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

        public double getCompressionRatio()
        {
            return compressionRatio;
        }
    }
}
