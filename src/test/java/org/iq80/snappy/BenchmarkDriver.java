package org.iq80.snappy;

import com.google.common.base.Throwables;
import org.iq80.snappy.SnappyBench.TestData;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

public enum BenchmarkDriver
{
    JAVA_BLOCK
            {
                @Override
                public long compress(TestData testData, long iterations)
                {
                    // Read the file and create buffers out side of timing
                    byte[] contents = testData.getContents();
                    byte[] compressed = new byte[Snappy.maxCompressedLength(contents.length)];

                    long start = System.nanoTime();
                    while (iterations-- > 0) {
                        Snappy.compress(contents, 0, contents.length, compressed, 0);
                    }
                    long timeInNanos = System.nanoTime() - start;

                    return timeInNanos;
                }

                @Override
                public long uncompress(TestData testData, long iterations)
                {
                    // Read the file and create buffers out side of timing
                    byte[] contents = testData.getContents();
                    byte[] compressed = new byte[Snappy.maxCompressedLength(contents.length)];
                    int compressedSize = Snappy.compress(contents, 0, contents.length, compressed, 0);

                    byte[] uncompressed = new byte[contents.length];

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

                @Override
                public double getCompressionRatio(TestData testData)
                {
                    byte[] contents = testData.getContents();
                    byte[] compressed = new byte[Snappy.maxCompressedLength(contents.length)];
                    int compressedSize = Snappy.compress(contents, 0, contents.length, compressed, 0);
                    return 1.0 * (contents.length - compressedSize) / contents.length;
                }
            },

    JNI_BLOCK
            {
                @Override
                public long compress(TestData testData, long iterations)
                {
                    // Read the file and create buffers out side of timing
                    byte[] contents = testData.getContents();
                    byte[] compressed = new byte[Snappy.maxCompressedLength(contents.length)];

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

                @Override
                public long uncompress(TestData testData, long iterations)
                {
                    // Read the file and create buffers out side of timing
                    byte[] contents = testData.getContents();
                    byte[] compressed = new byte[Snappy.maxCompressedLength(contents.length)];
                    int compressedSize = Snappy.compress(contents, 0, contents.length, compressed, 0);

                    byte[] uncompressed = new byte[contents.length];

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

                @Override
                public double getCompressionRatio(TestData testData)
                {
                    byte[] contents = testData.getContents();
                    byte[] compressed = new byte[Snappy.maxCompressedLength(contents.length)];
                    int compressedSize;
                    try {
                        compressedSize = org.xerial.snappy.Snappy.compress(contents, 0, contents.length, compressed, 0);
                    }
                    catch (IOException e) {
                        throw Throwables.propagate(e);
                    }
                    return 1.0 * (contents.length - compressedSize) / contents.length;
                }
            },

    JAVA_STREAM
            {
                @Override
                public long compress(TestData testData, long iterations)
                {
                    try {

                        // Read the file and create buffers out side of timing
                        byte[] contents = testData.getContents();

                        long start = System.nanoTime();
                        while (iterations-- > 0) {
                            ByteArrayOutputStream rawOut = new ByteArrayOutputStream();
                            SnappyOutputStream out = new SnappyOutputStream(rawOut);
                            out.write(contents);
                            out.close();
                        }
                        long timeInNanos = System.nanoTime() - start;
                        return timeInNanos;

                    }
                    catch (IOException e) {
                        throw Throwables.propagate(e);
                    }

                }

                @Override
                public long uncompress(TestData testData, long iterations)
                {
                    try {

                        // Read the file and create buffers out side of timing
                        byte[] contents = testData.getContents();

                        ByteArrayOutputStream compressedStream = new ByteArrayOutputStream();
                        SnappyOutputStream out = new SnappyOutputStream(compressedStream);
                        out.write(contents);
                        out.close();
                        byte[] compressed = compressedStream.toByteArray();

                        byte[] inputBuffer = new byte[4096];

                        long timeInNanos;
                        long start = System.nanoTime();
                        while (iterations-- > 0) {
                            ByteArrayInputStream compIn = new ByteArrayInputStream(compressed);
                            SnappyInputStream in = new SnappyInputStream(compIn);

                            while (in.read(inputBuffer) >= 0) {
                            }
                            in.close();
                        }
                        timeInNanos = System.nanoTime() - start;

                        return timeInNanos;
                    }
                    catch (IOException e) {
                        throw Throwables.propagate(e);
                    }
                }

                @Override
                public double getCompressionRatio(TestData testData)
                {
                    byte[] contents = testData.getContents();
                    int compressedSize;
                    try {
                        ByteArrayOutputStream rawOut = new ByteArrayOutputStream();
                        SnappyOutputStream out = new SnappyOutputStream(rawOut);
                        out.write(contents);
                        out.close();

                        compressedSize = rawOut.size();
                    }
                    catch (IOException e) {
                        throw Throwables.propagate(e);
                    }
                    return 1.0 * (contents.length - compressedSize) / contents.length;
                }
            },

    JNI_STREAM
            {
                @Override
                public long compress(TestData testData, long iterations)
                {
                    try {

                        // Read the file and create buffers out side of timing
                        byte[] contents = testData.getContents();

                        long start = System.nanoTime();
                        while (iterations-- > 0) {
                            ByteArrayOutputStream rawOut = new ByteArrayOutputStream();
                            org.xerial.snappy.SnappyOutputStream out = new org.xerial.snappy.SnappyOutputStream(rawOut);
                            out.write(contents);
                            out.close();
                        }
                        long timeInNanos = System.nanoTime() - start;
                        return timeInNanos;

                    }
                    catch (IOException e) {
                        throw Throwables.propagate(e);
                    }

                }

                @Override
                public long uncompress(TestData testData, long iterations)
                {
                    try {

                        // Read the file and create buffers out side of timing
                        byte[] contents = testData.getContents();

                        ByteArrayOutputStream compressedStream = new ByteArrayOutputStream();
                        org.xerial.snappy.SnappyOutputStream out = new org.xerial.snappy.SnappyOutputStream(compressedStream);
                        out.write(contents);
                        out.close();
                        byte[] compressed = compressedStream.toByteArray();

                        byte[] inputBuffer = new byte[4096];

                        long timeInNanos;
                        long start = System.nanoTime();
                        while (iterations-- > 0) {
                            ByteArrayInputStream compIn = new ByteArrayInputStream(compressed);
                            org.xerial.snappy.SnappyInputStream in = new org.xerial.snappy.SnappyInputStream(compIn);

                            while (in.read(inputBuffer) >= 0) {
                            }
                            in.close();
                        }
                        timeInNanos = System.nanoTime() - start;

                        return timeInNanos;
                    }
                    catch (IOException e) {
                        throw Throwables.propagate(e);
                    }
                }

                @Override
                public double getCompressionRatio(TestData testData)
                {
                    byte[] contents = testData.getContents();
                    int compressedSize;
                    try {
                        ByteArrayOutputStream rawOut = new ByteArrayOutputStream();
                        org.xerial.snappy.SnappyOutputStream out = new org.xerial.snappy.SnappyOutputStream(rawOut);
                        out.write(contents);
                        out.close();

                        compressedSize = rawOut.size();
                    }
                    catch (IOException e) {
                        throw Throwables.propagate(e);
                    }
                    return 1.0 * (contents.length - compressedSize) / contents.length;
                }
            },;

    public abstract long compress(TestData testData, long iterations);

    public abstract long uncompress(TestData testData, long iterations);

    public abstract double getCompressionRatio(TestData testData);

}
