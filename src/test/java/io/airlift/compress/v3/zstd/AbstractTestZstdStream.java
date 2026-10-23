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

import com.google.common.io.Resources;
import com.google.common.primitives.Bytes;
import io.airlift.compress.v3.MalformedInputException;
import io.airlift.compress.v3.TestingData;
import io.airlift.compress.v3.benchmark.DataSet;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Math.min;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

abstract class AbstractTestZstdStream
{
    protected abstract ZstdOutputStream createCompressingOutputStream(OutputStream out)
            throws IOException;

    protected abstract ZstdInputStream createDecompressingInputStream(InputStream in);

    @Test
    void testEmptyStream()
            throws Exception
    {
        byte[] empty = new byte[0];
        byte[] compressed = compress(empty);
        byte[] uncompressed = decompressShortReads(compressed);

        assertThat(uncompressed).isEqualTo(empty);
    }

    @Test
    void testConcatenatedFrames()
            throws Exception
    {
        byte[] first = new byte[3000];
        new Random(123).nextBytes(first);
        byte[] second = "aaaaaaaaaaaabbbbbbbaaaaaa".getBytes(UTF_8);

        // an empty frame does not end the stream; the frame after it is still decoded
        assertThat(decompressShortReads(Bytes.concat(compress(new byte[0]), compress(first)))).isEqualTo(first);
        // Java-written frames always carry a checksum
        assertThat(decompressShortReads(Bytes.concat(javaCompress(first), javaCompress(second)))).isEqualTo(Bytes.concat(first, second));
    }

    @Test
    void testTruncatedStreamFails()
            throws Exception
    {
        byte[] original = new byte[1000];
        Arrays.fill(original, (byte) 'a');
        byte[] compressed = compress(original);

        // every prefix fails, including zero bytes, and a frame missing only its checksum after the content filled the read
        for (int length = 0; length < compressed.length; length++) {
            try (InputStream in = createDecompressingInputStream(shortReads(Arrays.copyOf(compressed, length)))) {
                assertThatThrownBy(() -> {
                    in.readNBytes(original.length);
                    in.read();
                }, "%s compressed bytes", length)
                        .isInstanceOf(IOException.class);
                // the stream stays failed rather than reporting a clean end of stream
                assertThatThrownBy(in::read, "%s compressed bytes", length)
                        .isInstanceOf(IOException.class);
            }
        }
    }

    @Test
    void testResetState()
            throws Exception
    {
        byte[] original = new byte[10_000];
        new Random(42).nextBytes(original);
        ByteArrayInputStream source = new ByteArrayInputStream(compress(original));

        try (ZstdInputStream in = createDecompressingInputStream(source)) {
            assertThat(in.readNBytes(10)).isEqualTo(Arrays.copyOf(original, 10));
            // repositioning the source and resetting discards the buffered decoder state, every time
            for (int i = 0; i < 3; i++) {
                source.reset();
                in.delegate().resetState();
                assertThat(in.readAllBytes()).isEqualTo(original);
            }
        }
    }

    @Test
    void testResetStateAtEndOfSource()
            throws Exception
    {
        byte[] original = new byte[1000];
        Arrays.fill(original, (byte) 'a');
        ByteArrayInputStream source = new ByteArrayInputStream(compress(original));

        try (ZstdInputStream in = createDecompressingInputStream(source)) {
            assertThat(in.readAllBytes()).isEqualTo(original);

            // a source with nothing left after a reset is truncated, as an empty input is
            in.delegate().resetState();
            assertThatThrownBy(in::read)
                    .isInstanceOf(IOException.class);
        }
    }

    @Test
    void testResetStateAfterFailure()
            throws Exception
    {
        byte[] original = new byte[1000];
        Arrays.fill(original, (byte) 'a');
        byte[] compressed = compress(original);

        SwitchableInputStream source = new SwitchableInputStream(new ByteArrayInputStream(Arrays.copyOf(compressed, compressed.length - 1)));
        try (ZstdInputStream in = createDecompressingInputStream(source)) {
            assertThatThrownBy(in::readAllBytes)
                    .isInstanceOf(IOException.class);

            // a failure does not carry over to the data read after a reset
            source.switchTo(new ByteArrayInputStream(compressed));
            in.delegate().resetState();
            assertThat(in.readAllBytes()).isEqualTo(original);
        }
    }

    @Test
    void testCorruptInputFails()
            throws Exception
    {
        byte[] compressed = Resources.toByteArray(Resources.getResource("data/zstd/offset-before-start.zst"));

        try (InputStream in = createDecompressingInputStream(new ByteArrayInputStream(compressed))) {
            assertThatThrownBy(in::readAllBytes)
                    .isInstanceOf(MalformedInputException.class);
            assertThatThrownBy(in::read)
                    .isInstanceOfAny(IOException.class, MalformedInputException.class);
        }
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
    void testCompressibleData()
            throws Exception
    {
        byte[] original = new byte[100_000];
        Arrays.fill(original, (byte) 'a');

        byte[] compressed = compress(original);
        byte[] uncompressed = decompress(compressed);

        assertThat(uncompressed).isEqualTo(original);
        assertThat(compressed.length).isLessThan(original.length / 10);
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

        try (InputStream zstdIn = createDecompressingInputStream(new ByteArrayInputStream(compressed))) {
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
    void testCloseIsIdempotent()
            throws Exception
    {
        byte[] original = new byte[10000];
        new Random(333).nextBytes(original);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        OutputStream zstdOut = createCompressingOutputStream(out);
        zstdOut.write(original);
        zstdOut.close();
        zstdOut.close(); // Should not throw

        byte[] compressed = out.toByteArray();

        AtomicInteger sourceCloses = new AtomicInteger();
        InputStream source = new FilterInputStream(new ByteArrayInputStream(compressed))
        {
            @Override
            public void close()
            {
                sourceCloses.incrementAndGet();
            }
        };
        InputStream zstdIn = createDecompressingInputStream(source);
        byte[] uncompressed = zstdIn.readAllBytes();
        zstdIn.close();
        zstdIn.close(); // Should not throw

        assertThat(uncompressed).isEqualTo(original);
        assertThat(sourceCloses).hasValue(1);
    }

    @Test
    void testWriteAfterClose()
            throws Exception
    {
        OutputStream zstdOut = createCompressingOutputStream(new ByteArrayOutputStream());
        zstdOut.close();

        assertThatThrownBy(() -> zstdOut.write(42))
                .isInstanceOf(IOException.class)
                .hasMessage("Stream is closed");
    }

    @Test
    void testReadAfterClose()
            throws Exception
    {
        byte[] compressed = compress("test".getBytes(UTF_8));
        InputStream zstdIn = createDecompressingInputStream(new ByteArrayInputStream(compressed));
        zstdIn.close();

        assertThatThrownBy(zstdIn::read)
                .isInstanceOf(IOException.class)
                .hasMessage("Stream is closed");
    }

    @Test
    void testFinishWithoutClosingSource()
            throws Exception
    {
        byte[] original = new byte[10000];
        new Random(222).nextBytes(original);

        AtomicInteger destinationCloses = new AtomicInteger();
        ByteArrayOutputStream out = new ByteArrayOutputStream()
        {
            @Override
            public void close()
            {
                destinationCloses.incrementAndGet();
            }
        };
        ZstdOutputStream zstdOut = createCompressingOutputStream(out);
        zstdOut.write(original);
        zstdOut.finishWithoutClosingSource();

        // the frame is complete, but the destination stays open
        assertThat(decompress(out.toByteArray())).isEqualTo(original);
        assertThat(destinationCloses).hasValue(0);

        assertThatThrownBy(() -> zstdOut.write(42))
                .isInstanceOf(IOException.class)
                .hasMessage("Stream is finished");

        // close still owns the destination, and closes it exactly once
        zstdOut.close();
        zstdOut.close();
        assertThat(destinationCloses).hasValue(1);
        assertThat(decompress(out.toByteArray())).isEqualTo(original);
    }

    @Test
    void testCloseAfterFailedFinishClosesDestination()
            throws Exception
    {
        AtomicInteger destinationCloses = new AtomicInteger();
        ZstdOutputStream zstdOut = createCompressingOutputStream(failingDestination(destinationCloses, false));
        zstdOut.write(new byte[] {1, 2, 3});

        assertThatThrownBy(zstdOut::finishWithoutClosingSource)
                .isInstanceOf(IOException.class)
                .hasMessage("write failed");
        assertThat(destinationCloses).hasValue(0);

        // the frame was not ended, so finishing again and closing report that rather than succeeding
        assertThatThrownBy(zstdOut::finishWithoutClosingSource)
                .isInstanceOf(IOException.class)
                .hasMessage("Stream has failed")
                .hasRootCauseMessage("write failed");
        assertThatThrownBy(zstdOut::close)
                .isInstanceOf(IOException.class)
                .hasMessage("Stream has failed")
                .hasRootCauseMessage("write failed");
        assertThat(destinationCloses).hasValue(1);

        // closing again does nothing
        zstdOut.close();
        assertThat(destinationCloses).hasValue(1);
    }

    @Test
    void testCloseReportsFinishFailure()
            throws Exception
    {
        AtomicInteger destinationCloses = new AtomicInteger();
        ZstdOutputStream zstdOut = createCompressingOutputStream(failingDestination(destinationCloses, true));
        zstdOut.write(new byte[] {1, 2, 3});

        assertThatThrownBy(zstdOut::close)
                .isInstanceOf(IOException.class)
                .hasMessage("write failed")
                .satisfies(e -> assertThat(e.getSuppressed()).extracting(Throwable::getMessage).containsExactly("close failed"));
        assertThat(destinationCloses).hasValue(1);
    }

    @Test
    void testContentChecksum()
            throws Exception
    {
        byte[] original = new byte[10000];
        new Random(321).nextBytes(original);

        byte[] compressed = compress(original);
        compressed[compressed.length - 1] ^= 1;

        assertThatThrownBy(() -> javaDecompress(compressed))
                .isInstanceOf(MalformedInputException.class)
                .hasMessageStartingWith("Bad checksum");
        assertThatThrownBy(() -> decompress(compressed))
                .isInstanceOf(MalformedInputException.class);
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
    void testInteropToJavaStreams()
            throws Exception
    {
        byte[] original = new byte[50_000];
        new Random(777).nextBytes(original);

        byte[] compressed = compress(original);
        byte[] uncompressed = javaDecompress(compressed);
        assertThat(uncompressed).isEqualTo(original);
    }

    @Test
    void testInteropFromJavaStreams()
            throws Exception
    {
        byte[] original = new byte[50_000];
        new Random(666).nextBytes(original);

        byte[] compressed = javaCompress(original);
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
        try (InputStream in = createDecompressingInputStream(new ByteArrayInputStream(compressed))) {
            return in.readAllBytes();
        }
    }

    protected byte[] decompressShortReads(byte[] compressed)
            throws IOException
    {
        try (InputStream in = createDecompressingInputStream(shortReads(compressed))) {
            return in.readAllBytes();
        }
    }

    // Returns one byte per read, like a network source that delivers data as it arrives
    private static InputStream shortReads(byte[] data)
    {
        return new FilterInputStream(new ByteArrayInputStream(data))
        {
            @Override
            public int read(byte[] buffer, int offset, int length)
                    throws IOException
            {
                return super.read(buffer, offset, min(length, 1));
            }
        };
    }

    private static byte[] javaCompress(byte[] original)
            throws IOException
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (OutputStream zstdOut = new ZstdJavaOutputStream(out)) {
            zstdOut.write(original);
        }
        return out.toByteArray();
    }

    private static byte[] javaDecompress(byte[] compressed)
            throws IOException
    {
        try (InputStream in = new ZstdJavaInputStream(new ByteArrayInputStream(compressed))) {
            return in.readAllBytes();
        }
    }

    protected static OutputStream failingDestination(AtomicInteger closes, boolean failClose)
    {
        return new OutputStream()
        {
            @Override
            public void write(int b)
                    throws IOException
            {
                throw new IOException("write failed");
            }

            @Override
            public void close()
                    throws IOException
            {
                closes.incrementAndGet();
                if (failClose) {
                    throw new IOException("close failed");
                }
            }
        };
    }

    private static class SwitchableInputStream
            extends FilterInputStream
    {
        SwitchableInputStream(InputStream in)
        {
            super(in);
        }

        void switchTo(InputStream in)
        {
            this.in = in;
        }
    }
}
