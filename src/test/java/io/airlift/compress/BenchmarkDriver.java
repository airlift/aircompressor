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
import io.airlift.compress.SnappyBench.TestData;

import java.io.ByteArrayInputStream;
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
                public long roundTrip(TestData testData, long iterations)
                {
                    // Read the file and create buffers out side of timing
                    byte[] contents = testData.getContents();
                    byte[] compressed = new byte[Snappy.maxCompressedLength(contents.length)];
                    byte[] uncompressed = new byte[contents.length];

                    long start = System.nanoTime();
                    while (iterations-- > 0) {
                        int compressedSize = Snappy.compress(contents, 0, contents.length, compressed, 0);
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

                    long timeInNanos;
                    try {
                        long start = System.nanoTime();
                        while (iterations-- > 0) {
                            org.xerial.snappy.Snappy.uncompress(compressed, 0, compressedSize, uncompressed, 0);
                        }
                        timeInNanos = System.nanoTime() - start;
                    }
                    catch (IOException e) {
                        throw Throwables.propagate(e);
                    }

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
                public long roundTrip(TestData testData, long iterations)
                {
                    // Read the file and create buffers out side of timing
                    byte[] contents = testData.getContents();
                    byte[] compressed = new byte[Snappy.maxCompressedLength(contents.length)];
                    byte[] uncompressed = new byte[contents.length];

                    long timeInNanos;
                    try {
                        long start = System.nanoTime();
                        while (iterations-- > 0) {
                            int compressedSize = Snappy.compress(contents, 0, contents.length, compressed, 0);
                            org.xerial.snappy.Snappy.uncompress(compressed, 0, compressedSize, uncompressed, 0);
                        }
                        timeInNanos = System.nanoTime() - start;
                    }
                    catch (IOException e) {
                        throw Throwables.propagate(e);
                    }

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
                        ByteArrayOutputStream rawOut = new ByteArrayOutputStream(Snappy.maxCompressedLength(contents.length));

                        long start = System.nanoTime();
                        while (iterations-- > 0) {
                            rawOut.reset();
                            SnappyOutputStream out = SnappyOutputStream.newChecksumFreeBenchmarkOutputStream(rawOut);
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

                        ByteArrayOutputStream compressedStream = new ByteArrayOutputStream(Snappy.maxCompressedLength(contents.length));
                        SnappyOutputStream out = SnappyOutputStream.newChecksumFreeBenchmarkOutputStream(compressedStream);
                        out.write(contents);
                        out.close();
                        byte[] compressed = compressedStream.toByteArray();

                        byte[] inputBuffer = new byte[4096];

                        long timeInNanos;
                        long start = System.nanoTime();
                        while (iterations-- > 0) {
                            ByteArrayInputStream compIn = new ByteArrayInputStream(compressed);
                            SnappyInputStream in = new SnappyInputStream(compIn, false);

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
                public long roundTrip(TestData testData, long iterations)
                {
                    try {
                        // Read the file and create buffers out side of timing
                        byte[] contents = testData.getContents();
                        byte[] inputBuffer = new byte[4096];
                        ByteArrayOutputStream compressedStream = new ByteArrayOutputStream(Snappy.maxCompressedLength(contents.length));

                        long timeInNanos;
                        long start = System.nanoTime();
                        while (iterations-- > 0) {
                            compressedStream.reset();
                            SnappyOutputStream out = SnappyOutputStream.newChecksumFreeBenchmarkOutputStream(compressedStream);
                            out.write(contents);
                            out.close();

                            ByteArrayInputStream compIn = new ByteArrayInputStream(compressedStream.getBuffer(), 0, compressedStream.size());
                            SnappyInputStream in = new SnappyInputStream(compIn, false);

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
                        ByteArrayOutputStream rawOut = new ByteArrayOutputStream(Snappy.maxCompressedLength(contents.length));
                        SnappyOutputStream out = SnappyOutputStream.newChecksumFreeBenchmarkOutputStream(rawOut);
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
                        ByteArrayOutputStream rawOut = new ByteArrayOutputStream(org.xerial.snappy.Snappy.maxCompressedLength(contents.length));

                        long start = System.nanoTime();
                        while (iterations-- > 0) {
                            rawOut.reset();
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
                        ByteArrayOutputStream compressedStream = new ByteArrayOutputStream(org.xerial.snappy.Snappy.maxCompressedLength(contents.length));

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
                public long roundTrip(TestData testData, long iterations)
                {
                    try {

                        // Read the file and create buffers out side of timing
                        byte[] contents = testData.getContents();
                        byte[] inputBuffer = new byte[4096];
                        ByteArrayOutputStream compressedStream = new ByteArrayOutputStream(org.xerial.snappy.Snappy.maxCompressedLength(contents.length));

                        long timeInNanos;
                        long start = System.nanoTime();
                        while (iterations-- > 0) {
                            compressedStream.reset();
                            org.xerial.snappy.SnappyOutputStream out = new org.xerial.snappy.SnappyOutputStream(compressedStream);
                            out.write(contents);
                            out.close();

                            ByteArrayInputStream compIn = new ByteArrayInputStream(compressedStream.getBuffer(), 0, compressedStream.size());
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
                        ByteArrayOutputStream rawOut = new ByteArrayOutputStream(org.xerial.snappy.Snappy.maxCompressedLength(contents.length));
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

    public abstract long roundTrip(TestData testData, long iterations);

    public abstract double getCompressionRatio(TestData testData);

}
