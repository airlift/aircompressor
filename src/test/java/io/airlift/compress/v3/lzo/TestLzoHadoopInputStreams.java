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
package io.airlift.compress.v3.lzo;

import io.airlift.compress.v3.MalformedInputException;
import io.airlift.compress.v3.hadoop.HadoopStreams;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Random;
import java.util.zip.Adler32;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestLzoHadoopInputStreams
{
    @Test
    void lzoZeroLengthReadAtEndOfStreamReturnsZero()
            throws IOException
    {
        try (LzoHadoopInputStream input = new LzoHadoopInputStream(new ByteArrayInputStream(new byte[0]), 64)) {
            assertThat(input.read(new byte[0], 0, 0)).isEqualTo(0);
            assertThat(input.read()).isEqualTo(-1);
        }
    }

    @Test
    void lzopZeroLengthReadAtEndOfStreamReturnsZero()
            throws IOException
    {
        try (LzopHadoopInputStream input = new LzopHadoopInputStream(createEmptyLzopStream(), 64)) {
            assertThat(input.read(new byte[0], 0, 0)).isEqualTo(0);
            assertThat(input.read()).isEqualTo(-1);
        }
    }

    @Test
    void lzoReadsBlocksLargerThanConfiguredBufferSize()
            throws IOException
    {
        byte[] data = compressibleData(1024 * 1024);
        byte[] compressed = write(new LzoHadoopStreams(256 * 1024), data);

        assertThat(readFully(new LzoHadoopStreams(64 * 1024).createInputStream(new ByteArrayInputStream(compressed)))).isEqualTo(data);
    }

    @Test
    void lzopReadsBlocksLargerThanConfiguredBufferSize()
            throws IOException
    {
        byte[] data = compressibleData(1024 * 1024);
        byte[] compressed = write(new LzopHadoopStreams(256 * 1024), data);

        assertThat(readFully(new LzopHadoopStreams(64 * 1024).createInputStream(new ByteArrayInputStream(compressed)))).isEqualTo(data);
    }

    private static byte[] compressibleData(int size)
    {
        byte[] data = new byte[size];
        Random random = new Random(0);
        for (int i = 0; i < size; i += 8) {
            data[i] = (byte) random.nextInt(16);
        }
        return data;
    }

    private static byte[] write(HadoopStreams streams, byte[] data)
            throws IOException
    {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (OutputStream output = streams.createOutputStream(bytes)) {
            output.write(data);
        }
        return bytes.toByteArray();
    }

    private static byte[] readFully(InputStream input)
            throws IOException
    {
        try (input) {
            ByteArrayOutputStream result = new ByteArrayOutputStream();
            // the first byte goes through the single-byte path
            result.write(input.read());
            byte[] buffer = new byte[1000];
            int read;
            while ((read = input.read(buffer, 0, buffer.length)) != -1) {
                result.write(buffer, 0, read);
            }
            return result.toByteArray();
        }
    }

    @Test
    void lzopStaysFailedAfterRejectedBlockLength()
            throws IOException
    {
        ByteArrayOutputStream output = lzopHeader();
        writeBigEndianInt(output, 4);
        writeBigEndianInt(output, 4);
        output.writeBytes(new byte[] {1, 2, 3, 4});
        writeBigEndianInt(output, Integer.MAX_VALUE);
        writeBigEndianInt(output, 4);
        output.writeBytes(new byte[4]);

        LzopHadoopInputStream input = new LzopHadoopInputStream(new ByteArrayInputStream(output.toByteArray()), 64);
        byte[] buffer = new byte[2];
        assertThat(input.read(buffer, 0, 2)).isEqualTo(2);
        assertThat(input.read(buffer, 0, 2)).isEqualTo(2);
        assertThatThrownBy(() -> input.read(buffer, 0, 2)).hasMessageContaining("Invalid uncompressed block length");
        assertThatThrownBy(() -> input.read(buffer, 0, 2)).hasMessageContaining("Stream has failed");
        assertThatThrownBy(input::read).hasMessageContaining("Stream has failed");
    }

    @Test
    void lzoAcceptsAggregateBlockLengthAboveLimit()
            throws IOException
    {
        // Hadoop writes the aggregate length of a large write, followed by independently compressed chunks
        byte[] data = compressibleData(1024);
        LzoCompressor compressor = new LzoCompressor();
        byte[] chunk = new byte[compressor.maxCompressedLength(data.length)];
        int chunkLength = compressor.compress(data, 0, data.length, chunk, 0, chunk.length);

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        writeBigEndianInt(output, LzoConstants.MAX_BLOCK_SIZE + 1);
        writeBigEndianInt(output, chunkLength);
        output.write(chunk, 0, chunkLength);

        byte[] bytes = output.toByteArray();
        byte[] buffer = new byte[data.length];
        try (LzoHadoopInputStream input = new LzoHadoopInputStream(new ByteArrayInputStream(bytes), 64)) {
            assertThat(input.read(buffer, 0, 10)).isEqualTo(10);
            assertThat(buffer).startsWith(Arrays.copyOf(data, 10));
        }
    }

    @Test
    void lzoRejectsInvalidBlockLength()
    {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        writeBigEndianInt(output, -1);
        writeBigEndianInt(output, 4);
        output.writeBytes(new byte[4]);

        LzoHadoopInputStream input = new LzoHadoopInputStream(new ByteArrayInputStream(output.toByteArray()), 64);
        assertThatThrownBy(input::read).hasMessageContaining("Invalid uncompressed block length");
        assertThatThrownBy(input::read).hasMessageContaining("Stream has failed");
    }

    @Test
    void lzopRejectsEncodedMinusOneBlockLength()
            throws IOException
    {
        ByteArrayOutputStream output = lzopHeader();
        writeBigEndianInt(output, -1);
        writeBigEndianInt(output, 4);
        writeBigEndianInt(output, 4);
        output.writeBytes(new byte[] {1, 2, 3, 4});
        writeBigEndianInt(output, 0);

        LzopHadoopInputStream input = new LzopHadoopInputStream(new ByteArrayInputStream(output.toByteArray()), 64);
        assertThatThrownBy(input::read).hasMessageContaining("Invalid uncompressed block length");
        assertThatThrownBy(input::read).hasMessageContaining("Stream has failed");
    }

    @Test
    void resetStateClearsFailure()
            throws IOException
    {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        writeBigEndianInt(output, -1);
        LzoHadoopInputStream lzo = new LzoHadoopInputStream(new ByteArrayInputStream(output.toByteArray()), 64);
        assertThatThrownBy(lzo::read).hasMessageContaining("Invalid uncompressed block length");
        lzo.resetState();
        assertThat(lzo.read()).isEqualTo(-1);

        ByteArrayOutputStream lzopOutput = lzopHeader();
        writeBigEndianInt(lzopOutput, -1);
        LzopHadoopInputStream lzop = new LzopHadoopInputStream(new ByteArrayInputStream(lzopOutput.toByteArray()), 64);
        assertThatThrownBy(lzop::read).hasMessageContaining("Invalid uncompressed block length");
        lzop.resetState();
        assertThatThrownBy(lzop::read).isInstanceOf(EOFException.class);
    }

    @Test
    void lzopStaysFailedAfterTruncatedBlockPayload()
            throws IOException
    {
        ByteArrayOutputStream output = lzopHeader();
        writeBigEndianInt(output, 4);
        writeBigEndianInt(output, 4);
        output.writeBytes(new byte[] {1, 2, 3, 4});
        // second block declares 8 bytes but the stream ends after 2
        writeBigEndianInt(output, 8);
        writeBigEndianInt(output, 8);
        output.writeBytes(new byte[] {9, 9});

        LzopHadoopInputStream input = new LzopHadoopInputStream(new ByteArrayInputStream(output.toByteArray()), 64);
        byte[] buffer = new byte[4];
        assertThat(input.read(buffer, 0, 4)).isEqualTo(4);
        assertThatThrownBy(() -> input.read(buffer, 0, 2)).isInstanceOf(EOFException.class);
        assertThatThrownBy(() -> input.read(buffer, 0, 2)).hasMessageContaining("Stream has failed");
        assertThatThrownBy(input::read).hasMessageContaining("Stream has failed");
    }

    @Test
    void lzopStaysFailedAfterTruncatedBlockPayloadOnDirectRead()
            throws IOException
    {
        ByteArrayOutputStream output = lzopHeader();
        writeBigEndianInt(output, 8);
        writeBigEndianInt(output, 8);
        output.writeBytes(new byte[] {9, 9});

        LzopHadoopInputStream input = new LzopHadoopInputStream(new ByteArrayInputStream(output.toByteArray()), 64);
        byte[] buffer = new byte[64];
        assertThatThrownBy(() -> input.read(buffer, 0, 64)).isInstanceOf(EOFException.class);
        assertThatThrownBy(() -> input.read(buffer, 0, 64)).hasMessageContaining("Stream has failed");
    }

    @Test
    void lzopStaysFailedAfterTruncatedBlockLength()
            throws IOException
    {
        ByteArrayOutputStream output = lzopHeader();
        // only 2 of the 4 bytes of the uncompressed length
        output.writeBytes(new byte[] {0, 0});

        LzopHadoopInputStream input = new LzopHadoopInputStream(new ByteArrayInputStream(output.toByteArray()), 64);
        assertThatThrownBy(input::read).hasMessageContaining("truncated");
        assertThatThrownBy(input::read).hasMessageContaining("Stream has failed");
    }

    @Test
    void lzopRejectsCompressedLengthAboveUncompressedLength()
            throws IOException
    {
        ByteArrayOutputStream output = lzopHeader();
        writeBigEndianInt(output, 4);
        writeBigEndianInt(output, Integer.MAX_VALUE);

        LzopHadoopInputStream input = new LzopHadoopInputStream(new ByteArrayInputStream(output.toByteArray()), 64);
        assertThatThrownBy(input::read).hasMessageContaining("Invalid compressed block length");
        assertThatThrownBy(input::read).hasMessageContaining("Stream has failed");
    }

    @Test
    void lzoRejectsCompressedChunkLengthAboveLimit()
    {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        writeBigEndianInt(output, 4);
        writeBigEndianInt(output, -1);

        LzoHadoopInputStream input = new LzoHadoopInputStream(new ByteArrayInputStream(output.toByteArray()), 64);
        assertThatThrownBy(input::read).hasMessageContaining("Invalid compressed chunk length");
        assertThatThrownBy(input::read).hasMessageContaining("Stream has failed");
    }

    @Test
    void lzoFailsOnTruncatedChunkWithHugeDeclaredLength()
    {
        // a corrupt length must fail on the missing bytes without allocating the declared size
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        writeBigEndianInt(output, 1024);
        writeBigEndianInt(output, 1_000_000_000);
        output.writeBytes(new byte[100]);

        LzoHadoopInputStream input = new LzoHadoopInputStream(new ByteArrayInputStream(output.toByteArray()), 64);
        assertThatThrownBy(input::read).isInstanceOf(EOFException.class);
        assertThatThrownBy(input::read).hasMessageContaining("Stream has failed");
    }

    @Test
    void lzoReadsChunkLargerThanInitialCompressedBuffer()
            throws IOException
    {
        // incompressible data makes the compressed chunk larger than the initial compressed buffer
        byte[] data = new byte[300_000];
        new Random(1).nextBytes(data);
        byte[] compressed = write(new LzoHadoopStreams(data.length), data);

        assertThat(readFully(new LzoHadoopStreams(data.length).createInputStream(new ByteArrayInputStream(compressed)))).isEqualTo(data);
    }

    @Test
    void lzoReadsChunkWithHigherExpansionThanOurCompressor()
            throws IOException
    {
        // 5 literal bytes, 100 matches each followed by 4 literals, then the end marker: 808 bytes decoding to 704
        ByteArrayOutputStream chunk = new ByteArrayOutputStream();
        chunk.writeBytes(new byte[] {0x15, 0x41, 0x41, 0x41, 0x41});
        for (int i = 0; i < 100; i++) {
            chunk.writeBytes(new byte[] {0x21, 0x00, 0x00, 0x01, 0x41, 0x41, 0x41, 0x41});
        }
        chunk.writeBytes(new byte[] {0x11, 0x00, 0x00});
        byte[] chunkBytes = chunk.toByteArray();
        byte[] expected = new byte[1024];
        int expectedLength = new LzoDecompressor().decompress(chunkBytes, 0, chunkBytes.length, expected, 0, expected.length);

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        writeBigEndianInt(output, expectedLength);
        writeBigEndianInt(output, chunkBytes.length);
        output.writeBytes(chunkBytes);

        assertThat(chunkBytes.length).isGreaterThan(expectedLength);
        assertThat(readFully(new LzoHadoopInputStream(new ByteArrayInputStream(output.toByteArray()), 1024)))
                .isEqualTo(Arrays.copyOf(expected, expectedLength));
    }

    @Test
    void lzoStaysFailedAfterCorruptChunk()
            throws IOException
    {
        byte[] data = compressibleData(1024);
        LzoCompressor compressor = new LzoCompressor();
        byte[] chunk = new byte[compressor.maxCompressedLength(data.length)];
        int chunkLength = compressor.compress(data, 0, data.length, chunk, 0, chunk.length);

        // one block of two chunks, the second one corrupt
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        writeBigEndianInt(output, 2048);
        writeBigEndianInt(output, chunkLength);
        output.write(chunk, 0, chunkLength);
        writeBigEndianInt(output, 16);
        output.writeBytes(new byte[16]);
        output.writeBytes(new byte[1024]);

        LzoHadoopInputStream input = new LzoHadoopInputStream(new ByteArrayInputStream(output.toByteArray()), 4096);
        byte[] buffer = new byte[1024];
        assertThat(input.read(buffer, 0, 1024)).isEqualTo(1024);
        assertThatThrownBy(() -> input.read(buffer, 0, 1024)).isInstanceOf(MalformedInputException.class);
        assertThatThrownBy(() -> input.read(buffer, 0, 1024)).hasMessageContaining("Stream has failed");
        assertThatThrownBy(input::read).hasMessageContaining("Stream has failed");
    }

    @Test
    void lzopStaysFailedAfterCorruptCompressedPayload()
            throws IOException
    {
        // compressed length is below the uncompressed length, so the payload is decoded
        ByteArrayOutputStream output = lzopHeader();
        writeBigEndianInt(output, 100);
        writeBigEndianInt(output, 16);
        output.writeBytes(new byte[16]);
        output.writeBytes(new byte[100]);

        LzopHadoopInputStream input = new LzopHadoopInputStream(new ByteArrayInputStream(output.toByteArray()), 64);
        byte[] buffer = new byte[10];
        assertThatThrownBy(() -> input.read(buffer, 0, 10)).isInstanceOf(MalformedInputException.class);
        assertThatThrownBy(() -> input.read(buffer, 0, 10)).hasMessageContaining("Stream has failed");
        assertThatThrownBy(input::read).hasMessageContaining("Stream has failed");
    }

    @Test
    void lzoRejectsChunkThatDecodesPastBlockLength()
    {
        // the block declares 1024 bytes, but its chunk decodes to 1025
        byte[] bytes = lzoBlock(1024, compressibleData(1025));

        // grown buffer (retry path) and caller buffer (direct path) both have room for the extra byte
        LzoHadoopInputStream buffered = new LzoHadoopInputStream(new ByteArrayInputStream(bytes), 64);
        assertThatThrownBy(() -> buffered.read(new byte[10], 0, 10)).hasMessageContaining("more than the remaining block length");
        assertThatThrownBy(buffered::read).hasMessageContaining("Stream has failed");

        LzoHadoopInputStream direct = new LzoHadoopInputStream(new ByteArrayInputStream(bytes), 64);
        assertThatThrownBy(() -> direct.read(new byte[2048], 0, 2048)).hasMessageContaining("more than the remaining block length");
        assertThatThrownBy(direct::read).hasMessageContaining("Stream has failed");
    }

    @Test
    void lzoReadsChunkThatFillsBlockLength()
            throws IOException
    {
        byte[] data = compressibleData(1024);
        byte[] bytes = lzoBlock(1024, data);

        assertThat(readFully(new LzoHadoopInputStream(new ByteArrayInputStream(bytes), 64))).isEqualTo(data);
    }

    @Test
    void lzoSkipsEmptyChunkOnDirectRead()
            throws IOException
    {
        byte[] data = compressibleData(1024);
        LzoCompressor compressor = new LzoCompressor();
        byte[] chunk = new byte[compressor.maxCompressedLength(data.length)];
        int chunkLength = compressor.compress(data, 0, data.length, chunk, 0, chunk.length);

        // an empty chunk followed by a valid one in the same block
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        writeBigEndianInt(output, data.length);
        writeBigEndianInt(output, 0);
        writeBigEndianInt(output, chunkLength);
        output.write(chunk, 0, chunkLength);

        LzoHadoopInputStream input = new LzoHadoopInputStream(new ByteArrayInputStream(output.toByteArray()), 64);
        byte[] buffer = new byte[2048];
        assertThat(input.read(buffer, 0, buffer.length)).isEqualTo(1024);
        assertThat(Arrays.copyOf(buffer, 1024)).isEqualTo(data);
    }

    @Test
    void lzoGrowsChunkBufferInStepsCloseToChunkSize()
            throws Exception
    {
        // the block declares 32MB, but the only chunk decodes to 20KB
        byte[] data = compressibleData(20 * 1024);
        byte[] bytes = lzoBlock(32 * 1024 * 1024, data);

        LzoHadoopInputStream input = new LzoHadoopInputStream(new ByteArrayInputStream(bytes), 64);
        byte[] buffer = new byte[100];
        assertThat(input.read(buffer, 0, buffer.length)).isEqualTo(100);

        Field field = LzoHadoopInputStream.class.getDeclaredField("uncompressedChunk");
        field.setAccessible(true);
        assertThat(((byte[]) field.get(input)).length).isBetween(data.length, 2 * data.length + 16);
    }

    private static byte[] lzoBlock(int blockLength, byte[] chunkData)
    {
        LzoCompressor compressor = new LzoCompressor();
        byte[] chunk = new byte[compressor.maxCompressedLength(chunkData.length)];
        int chunkLength = compressor.compress(chunkData, 0, chunkData.length, chunk, 0, chunk.length);

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        writeBigEndianInt(output, blockLength);
        writeBigEndianInt(output, chunkLength);
        output.write(chunk, 0, chunkLength);
        return output.toByteArray();
    }

    private static ByteArrayInputStream createEmptyLzopStream()
            throws IOException
    {
        ByteArrayOutputStream output = lzopHeader();
        writeBigEndianInt(output, 0);
        return new ByteArrayInputStream(output.toByteArray());
    }

    private static ByteArrayOutputStream lzopHeader()
            throws IOException
    {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        output.write(LzoConstants.LZOP_MAGIC);

        byte[] header = new byte[25];
        header[0] = 0x09;
        header[1] = 0x40;
        header[2] = 0x20;
        header[3] = (byte) 0xA0;
        header[4] = 0x10;
        header[5] = 0x10;
        header[6] = LzoConstants.LZO_1X_VARIANT;
        output.write(header);

        Adler32 checksum = new Adler32();
        checksum.update(header);
        writeBigEndianInt(output, (int) checksum.getValue());
        return output;
    }

    private static void writeBigEndianInt(ByteArrayOutputStream output, int value)
    {
        output.write(value >>> 24);
        output.write(value >>> 16);
        output.write(value >>> 8);
        output.write(value);
    }
}
