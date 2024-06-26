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

import com.google.common.primitives.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Math.min;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@EnabledIf("isNativeEnabled")
class TestZstdNativeStream
        extends AbstractTestZstdStream
{
    static boolean isNativeEnabled()
    {
        return ZstdNative.isEnabled();
    }

    @Override
    protected ZstdOutputStream createCompressingOutputStream(OutputStream out)
            throws IOException
    {
        return new ZstdOutputStream(new ZstdNativeOutputStream(out));
    }

    @Override
    protected ZstdInputStream createDecompressingInputStream(InputStream in)
    {
        return new ZstdInputStream(new ZstdNativeInputStream(in));
    }

    // the Java stream supports only the default compression level
    @Test
    void testCompressionLevel()
            throws Exception
    {
        byte[] original = mixedData(555);

        byte[] compressedDefault = compress(original, 3);
        byte[] compressedHigh = compress(original, 19);

        assertThat(compressedHigh.length).isLessThan(compressedDefault.length);
        assertThat(decompress(compressedDefault)).isEqualTo(original);
        assertThat(decompress(compressedHigh)).isEqualTo(original);
    }

    @Test
    void testFactoriesCreateNativeStreams()
            throws Exception
    {
        byte[] original = mixedData(666);

        try (ZstdOutputStream zstdOut = ZstdOutputStream.create(new ByteArrayOutputStream())) {
            assertThat(zstdOut.delegate()).isInstanceOf(ZstdNativeOutputStream.class);
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (ZstdOutputStream zstdOut = ZstdOutputStream.create(out, 19)) {
            assertThat(zstdOut.delegate()).isInstanceOf(ZstdNativeOutputStream.class);
            zstdOut.write(original);
        }
        assertThat(out.toByteArray()).isEqualTo(compress(original, 19));

        try (ZstdInputStream in = ZstdInputStream.create(new ByteArrayInputStream(out.toByteArray()))) {
            assertThat(in.delegate()).isInstanceOf(ZstdNativeInputStream.class);
            assertThat(in.readAllBytes()).isEqualTo(original);
        }
    }

    // the Java decoder rejects skippable frames as an invalid magic prefix
    @Test
    void testSkippableFrame()
            throws Exception
    {
        byte[] original = "aaaaaaaaaaaabbbbbbbaaaaaa".getBytes(UTF_8);
        // skippable frame magic 0x184D2A50 and payload length 3, both little endian, then the 3 byte payload
        byte[] skippableFrame = {0x50, 0x2A, 0x4D, 0x18, 3, 0, 0, 0, 1, 2, 3};

        assertThat(decompressShortReads(Bytes.concat(skippableFrame, compress(original)))).isEqualTo(original);
    }

    // the Java stream refills its whole input buffer before decoding, so it would read the failing source
    @Test
    void testReadReturnsDecodedDataBeforeReadingSource()
            throws Exception
    {
        byte[] original = new byte[10000];
        new Random(444).nextBytes(original);

        InputStream source = new SequenceInputStream(new ByteArrayInputStream(compress(original)), failingSource());
        try (InputStream in = new ZstdNativeInputStream(source)) {
            byte[] uncompressed = new byte[original.length + 1];
            assertThat(in.read(uncompressed)).isEqualTo(original.length);
            assertThat(Arrays.copyOf(uncompressed, original.length)).isEqualTo(original);
        }
    }

    // the Java stream buffers until it is finished, so its flush does not make the data readable
    @Test
    void testFlushedDataReadableBeforeClose()
            throws Exception
    {
        byte[] first = mixedData(555);
        byte[] second = mixedData(556);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (OutputStream zstdOut = new ZstdNativeOutputStream(out);
                InputStream in = new ZstdNativeInputStream(writtenBytes(out))) {
            zstdOut.write(first);
            zstdOut.flush();
            assertThat(readInSmallChunks(in, first.length)).isEqualTo(first);

            zstdOut.write(second);
            zstdOut.flush();
            assertThat(readInSmallChunks(in, second.length)).isEqualTo(second);
        }
    }

    // a failed write leaves the libzstd context unusable, so the native stream refuses further writes; the Java stream has no such state
    @Test
    void testWriteAfterFailedWriteFails()
            throws Exception
    {
        // larger than a zstd block, so the write reaches the destination
        byte[] original = new byte[200_000];
        new Random(777).nextBytes(original);

        AtomicInteger destinationCloses = new AtomicInteger();
        ZstdNativeOutputStream zstdOut = new ZstdNativeOutputStream(failingDestination(destinationCloses, false));
        assertThatThrownBy(() -> zstdOut.write(original))
                .isInstanceOf(IOException.class)
                .hasMessage("write failed");

        assertThatThrownBy(() -> zstdOut.write(42))
                .isInstanceOf(IOException.class)
                .hasMessage("Stream has failed")
                .hasRootCauseMessage("write failed");

        // the frame cannot be ended, so closing reports the failure, and still closes the destination
        assertThatThrownBy(zstdOut::close)
                .isInstanceOf(IOException.class)
                .hasMessage("Stream has failed")
                .hasRootCauseMessage("write failed");
        assertThat(destinationCloses).hasValue(1);
    }

    // native streams own a shared arena that a cleaner frees; the Java stream holds no native memory
    @Test
    void testUseFromAnotherThread()
            throws Exception
    {
        byte[] original = mixedData(888);

        try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ZstdNativeOutputStream zstdOut = new ZstdNativeOutputStream(out);
            executor.submit(() -> {
                try (zstdOut) {
                    zstdOut.write(original);
                }
                return null;
            }).get();

            ZstdNativeInputStream in = new ZstdNativeInputStream(new ByteArrayInputStream(out.toByteArray()));
            byte[] uncompressed = executor.submit(() -> {
                try (in) {
                    return in.readAllBytes();
                }
            }).get();
            assertThat(uncompressed).isEqualTo(original);
        }
    }

    private static byte[] compress(byte[] original, int compressionLevel)
            throws IOException
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (OutputStream zstdOut = new ZstdNativeOutputStream(out, compressionLevel)) {
            zstdOut.write(original);
        }
        return out.toByteArray();
    }

    private static byte[] mixedData(long seed)
    {
        byte[] data = new byte[100_000];
        Random random = new Random(seed);
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) ('a' + random.nextInt(4));
        }
        return data;
    }

    // reads smaller than a block leave decoded data inside libzstd between reads
    private static byte[] readInSmallChunks(InputStream in, int length)
            throws IOException
    {
        byte[] data = new byte[length];
        for (int offset = 0; offset < length; offset += 1000) {
            in.readNBytes(data, offset, 1000);
        }
        return data;
    }

    // Fail on further reads to model a live source with no more data available yet
    private static InputStream failingSource()
    {
        return new InputStream()
        {
            @Override
            public int read()
                    throws IOException
            {
                throw new IOException("source read after the available data");
            }
        };
    }

    // Returns the bytes written to the destination so far, and fails rather than block once it has returned them all
    private static InputStream writtenBytes(ByteArrayOutputStream out)
    {
        return new InputStream()
        {
            private int position;

            @Override
            public int read()
                    throws IOException
            {
                byte[] buffer = new byte[1];
                read(buffer, 0, 1);
                return buffer[0] & 0xFF;
            }

            @Override
            public int read(byte[] buffer, int offset, int length)
                    throws IOException
            {
                byte[] written = out.toByteArray();
                if (position == written.length) {
                    throw new IOException("source read after the available data");
                }
                int size = min(length, written.length - position);
                System.arraycopy(written, position, buffer, offset, size);
                position += size;
                return size;
            }
        };
    }
}
