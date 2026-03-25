/*
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
package io.airlift.compress.v3.zstd;

import io.airlift.compress.v3.TestingData;
import io.airlift.compress.v3.benchmark.DataSet;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Random;

import static com.google.common.io.ByteStreams.toByteArray;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Abstract base class for testing Zstd stream implementations.
 * Subclasses provide the concrete stream implementations to test.
 */
abstract class AbstractTestZstdStream
{
    /**
     * Creates a compressing output stream wrapping the given output stream.
     */
    protected abstract OutputStream createCompressingOutputStream(OutputStream out)
            throws IOException;

    /**
     * Creates a decompressing input stream wrapping the given input stream.
     */
    protected abstract InputStream createDecompressingInputStream(InputStream in)
            throws IOException;

    @Test
    void testSimple()
            throws Exception
    {
        byte[] original = "aaaaaaaaaaaabbbbbbbaaaaaa".getBytes(UTF_8);

        byte[] compressed = compress(original);
        byte[] uncompressed = decompress(compressed);

        assertThat(uncompressed).isEqualTo(original);
    }

    @Test
    void testEmptyStream()
            throws Exception
    {
        byte[] empty = new byte[0];
        byte[] compressed = compress(empty);
        byte[] uncompressed = decompress(compressed);

        assertThat(uncompressed).isEqualTo(empty);
    }

    @Test
    void testSingleByte()
            throws Exception
    {
        byte[] original = new byte[] {42};
        byte[] compressed = compress(original);
        byte[] uncompressed = decompress(compressed);

        assertThat(uncompressed).isEqualTo(original);
    }

    @Test
    void testLargeData()
            throws Exception
    {
        // Create data larger than buffer sizes to test multiple buffer fills
        byte[] original = new byte[500000];
        new Random(42).nextBytes(original);

        byte[] compressed = compress(original);
        byte[] uncompressed = decompress(compressed);

        assertThat(uncompressed).isEqualTo(original);
    }

    @Test
    void testCompressibleData()
            throws Exception
    {
        // Create highly compressible data
        byte[] original = new byte[100000];
        Arrays.fill(original, (byte) 'a');

        byte[] compressed = compress(original);
        byte[] uncompressed = decompress(compressed);

        assertThat(uncompressed).isEqualTo(original);
        // Highly compressible data should compress well
        assertThat(compressed.length).isLessThan(original.length / 10);
    }

    @Test
    void testUncompressibleData()
            throws Exception
    {
        // Create random (uncompressible) data
        byte[] original = new byte[10000];
        new Random(12345).nextBytes(original);

        byte[] compressed = compress(original);
        byte[] uncompressed = decompress(compressed);

        assertThat(uncompressed).isEqualTo(original);
    }

    @Test
    void testSingleByteWrites()
            throws Exception
    {
        byte[] original = new byte[10000];
        new Random(999).nextBytes(original);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (OutputStream zstdOut = createCompressingOutputStream(out)) {
            for (byte b : original) {
                zstdOut.write(b);
            }
        }
        byte[] compressed = out.toByteArray();

        byte[] uncompressed = decompress(compressed);
        assertThat(uncompressed).isEqualTo(original);
    }

    @Test
    void testSingleByteReads()
            throws Exception
    {
        byte[] original = new byte[10000];
        new Random(888).nextBytes(original);

        byte[] compressed = compress(original);

        ByteArrayInputStream in = new ByteArrayInputStream(compressed);
        try (InputStream zstdIn = createDecompressingInputStream(in)) {
            byte[] uncompressed = new byte[original.length];
            int i = 0;
            int b;
            while ((b = zstdIn.read()) != -1) {
                uncompressed[i++] = (byte) b;
            }
            assertThat(i).isEqualTo(original.length);
            assertThat(uncompressed).isEqualTo(original);
        }
    }

    @Test
    void testFlush()
            throws Exception
    {
        byte[] original = new byte[10000];
        new Random(555).nextBytes(original);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (OutputStream zstdOut = createCompressingOutputStream(out)) {
            zstdOut.write(original, 0, 5000);
            zstdOut.flush();
            zstdOut.write(original, 5000, 5000);
            zstdOut.flush();
        }
        byte[] compressed = out.toByteArray();

        byte[] uncompressed = decompress(compressed);
        assertThat(uncompressed).isEqualTo(original);
    }

    @Test
    void testMultipleFlushes()
            throws Exception
    {
        byte[] original = new byte[10000];
        new Random(444).nextBytes(original);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (OutputStream zstdOut = createCompressingOutputStream(out)) {
            zstdOut.write(original);
            for (int i = 0; i < 10; i++) {
                zstdOut.flush();
            }
        }
        byte[] compressed = out.toByteArray();

        byte[] uncompressed = decompress(compressed);
        assertThat(uncompressed).isEqualTo(original);
    }

    @Test
    void testCloseIsIdempotent()
            throws Exception
    {
        byte[] original = new byte[10000];
        new Random(333).nextBytes(original);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        OutputStream zstdOut = createCompressingOutputStream(out);
        zstdOut.write(original);
        zstdOut.close();
        zstdOut.close();  // Should not throw

        byte[] compressed = out.toByteArray();

        InputStream zstdIn = createDecompressingInputStream(new ByteArrayInputStream(compressed));
        byte[] uncompressed = toByteArray(zstdIn);
        zstdIn.close();
        zstdIn.close();  // Should not throw

        assertThat(uncompressed).isEqualTo(original);
    }

    @Test
    void testWriteAfterClose()
            throws Exception
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        OutputStream zstdOut = createCompressingOutputStream(out);

        assertThatThrownBy(() -> {
            zstdOut.close();
            zstdOut.write(42);
        }).isInstanceOf(IOException.class)
                .hasMessageContaining("closed");
    }

    @Test
    void testReadAfterClose()
            throws Exception
    {
        byte[] compressed = compress("test".getBytes(UTF_8));
        InputStream zstdIn = createDecompressingInputStream(new ByteArrayInputStream(compressed));

        assertThatThrownBy(() -> {
            zstdIn.close();
            zstdIn.read();
        }).isInstanceOf(IOException.class)
                .hasMessageContaining("closed");
    }

    @Test
    void testAvailable()
            throws Exception
    {
        byte[] original = "test data".getBytes(UTF_8);
        byte[] compressed = compress(original);

        try (InputStream zstdIn = createDecompressingInputStream(new ByteArrayInputStream(compressed))) {
            // Initially available may be 0 until first read
            assertThat(zstdIn.available()).isGreaterThanOrEqualTo(0);

            // After reading, available should reflect buffered data
            zstdIn.read();
            assertThat(zstdIn.available()).isGreaterThanOrEqualTo(0);
        }
    }

    @Test
    void testByteForByteTestData()
            throws Exception
    {
        for (DataSet dataSet : TestingData.DATA_SETS) {
            byte[] original = dataSet.getUncompressed();
            byte[] compressed = compress(original);
            byte[] uncompressed = decompress(compressed);
            assertThat(uncompressed).isEqualTo(original);
        }
    }

    @Test
    void testInteropWithJavaStreams()
            throws Exception
    {
        byte[] original = new byte[50000];
        new Random(777).nextBytes(original);

        // Compress with this implementation
        byte[] compressed = compress(original);

        // Decompress with pure Java
        byte[] uncompressed = javaDecompress(compressed);
        assertThat(uncompressed).isEqualTo(original);
    }

    @Test
    void testInteropFromJavaStreams()
            throws Exception
    {
        byte[] original = new byte[50000];
        new Random(666).nextBytes(original);

        // Compress with pure Java
        byte[] compressed = javaCompress(original);

        // Decompress with this implementation
        byte[] uncompressed = decompress(compressed);
        assertThat(uncompressed).isEqualTo(original);
    }

    protected byte[] compress(byte[] original)
            throws IOException
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (OutputStream zstdOut = createCompressingOutputStream(out)) {
            zstdOut.write(original);
        }
        return out.toByteArray();
    }

    protected byte[] decompress(byte[] compressed)
            throws IOException
    {
        return toByteArray(createDecompressingInputStream(new ByteArrayInputStream(compressed)));
    }

    protected static byte[] javaCompress(byte[] original)
            throws IOException
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (OutputStream zstdOut = new ZstdJavaOutputStream(out)) {
            zstdOut.write(original);
        }
        return out.toByteArray();
    }

    protected static byte[] javaDecompress(byte[] compressed)
            throws IOException
    {
        return toByteArray(new ZstdJavaInputStream(new ByteArrayInputStream(compressed)));
    }
}
