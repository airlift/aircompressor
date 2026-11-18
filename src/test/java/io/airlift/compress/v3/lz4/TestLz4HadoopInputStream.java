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
package io.airlift.compress.v3.lz4;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestLz4HadoopInputStream
{
    private static final int BUFFER_SIZE = 64 * 1024;

    @Test
    void zeroLengthReadAtEndOfStreamReturnsZero()
            throws IOException
    {
        try (Lz4HadoopInputStream input = new Lz4HadoopInputStream(
                new Lz4JavaDecompressor(), new ByteArrayInputStream(new byte[0]), 64)) {
            assertThat(input.read(new byte[0], 0, 0)).isEqualTo(0);
            assertThat(input.read()).isEqualTo(-1);
        }
    }

    @Test
    void roundTripGrowsCompressedBuffer()
            throws IOException
    {
        // random data does not compress, so every chunk is about as large as the buffer
        // and the compressed buffer has to grow several times while the first one is read
        byte[] original = new byte[300_000];
        new Random(42).nextBytes(original);
        ByteArrayOutputStream compressed = new ByteArrayOutputStream();
        try (Lz4HadoopOutputStream output = new Lz4HadoopOutputStream(new Lz4JavaCompressor(), compressed, BUFFER_SIZE)) {
            output.write(original);
        }
        try (Lz4HadoopInputStream input = input(compressed.toByteArray())) {
            assertThat(input.readAllBytes()).isEqualTo(original);
        }
    }

    @Test
    void rejectsInvalidLengths()
            throws IOException
    {
        assertReadFails(ints(-1), IOException.class, "Invalid uncompressed block length: 4294967295");
        assertReadFails(ints(1, -1), IOException.class, "Invalid compressed chunk length: 4294967295");
        assertReadFails(ints(1, 0), IOException.class, "Invalid compressed chunk length: 0");
        assertReadFails(ints(1, Integer.MAX_VALUE), IOException.class, "Invalid compressed chunk length: 2147483647");
        assertReadFails(ints(100, 1000), IOException.class, "Compressed chunk length 1000 exceeds LZ4 bound 116 for remaining block length 100");
    }

    @Test
    void rejectsTruncatedStream()
            throws IOException
    {
        // the stream ends right after the block header
        assertReadFails(ints(1), EOFException.class, "encountered EOF while reading compressed chunk length");
        // the stream ends before the chunk data
        assertReadFails(ints(100, 10), EOFException.class, "encountered EOF while reading block data");
        // the header passes validation but declares almost 2GB more than follows,
        // which must fail on the missing data and not allocate the declared length
        assertReadFails(
                concat(ints(Integer.MAX_VALUE, Integer.MAX_VALUE - 1000), new byte[] {1, 2, 3}),
                EOFException.class,
                "encountered EOF while reading block data");
    }

    @Test
    void rejectsChunkLengthExceedingBoundOfRemainingBlock()
            throws IOException
    {
        byte[] literals = new byte[90];
        Arrays.fill(literals, (byte) 'x');
        // the first chunk stores 90 of the 100 bytes as literals, and the length of the second one
        // fits the bound of the whole block (116) but not of the 10 bytes that remain (26)
        byte[] data = concat(
                ints(100, 92),
                new byte[] {(byte) 0xF0, 75},
                literals,
                ints(100));

        try (Lz4HadoopInputStream input = input(data)) {
            byte[] buffer = new byte[90];
            assertThat(input.read(buffer, 0, buffer.length)).isEqualTo(90);
            assertThat(buffer).isEqualTo(literals);
            assertThatThrownBy(input::read)
                    .isInstanceOf(IOException.class)
                    .hasMessage("Compressed chunk length 100 exceeds LZ4 bound 26 for remaining block length 10");
        }
    }

    @Test
    void skipsEmptyChunk()
            throws IOException
    {
        // a block of two bytes made of a chunk that decodes to nothing followed by a chunk with both bytes
        byte[] data = concat(
                ints(2, 1),
                new byte[] {0},
                ints(3),
                new byte[] {0x20, 'a', 'b'});

        try (Lz4HadoopInputStream input = input(data)) {
            assertThat(input.read()).isEqualTo('a');
            assertThat(input.read()).isEqualTo('b');
            assertThat(input.read()).isEqualTo(-1);
        }

        // smaller than the block, so the chunk is decoded into the stream's buffer
        try (Lz4HadoopInputStream input = input(data)) {
            byte[] buffer = new byte[1];
            assertThat(input.read(buffer, 0, 1)).isEqualTo(1);
            assertThat(buffer[0]).isEqualTo((byte) 'a');
            assertThat(input.read(buffer, 0, 1)).isEqualTo(1);
            assertThat(buffer[0]).isEqualTo((byte) 'b');
            assertThat(input.read(buffer, 0, 1)).isEqualTo(-1);
        }

        // large enough for the block, so the chunk is decoded directly into the caller's buffer
        try (Lz4HadoopInputStream input = input(data)) {
            byte[] buffer = new byte[8];
            assertThat(input.read(buffer, 0, buffer.length)).isEqualTo(2);
            assertThat(Arrays.copyOf(buffer, 2)).isEqualTo(new byte[] {'a', 'b'});
            assertThat(input.read(buffer, 0, buffer.length)).isEqualTo(-1);
        }
    }

    @Test
    void doesNotReturnBytesOfRejectedChunk()
            throws IOException
    {
        // the chunk decodes to three bytes while the block declares two
        byte[] data = concat(ints(2, 4), new byte[] {0x30, 'a', 'b', 'c'});
        String message = "Invalid decompressed chunk length 3 for remaining block length 2";

        try (Lz4HadoopInputStream input = input(data)) {
            assertThatThrownBy(input::read).isInstanceOf(IOException.class).hasMessage(message);
            // the rejected chunk is not kept, so the next read looks for another chunk
            assertThatThrownBy(input::read).isInstanceOf(EOFException.class);
        }

        // smaller than the block, so the chunk is decoded into the stream's buffer
        try (Lz4HadoopInputStream input = input(data)) {
            assertThatThrownBy(() -> input.read(new byte[1], 0, 1)).isInstanceOf(IOException.class).hasMessage(message);
            assertThatThrownBy(() -> input.read(new byte[1], 0, 1)).isInstanceOf(EOFException.class);
        }

        // large enough for the block, so the chunk is decoded directly into the caller's buffer
        try (Lz4HadoopInputStream input = input(data)) {
            assertThatThrownBy(() -> input.read(new byte[8], 0, 8)).isInstanceOf(IOException.class).hasMessage(message);
            assertThatThrownBy(() -> input.read(new byte[8], 0, 8)).isInstanceOf(EOFException.class);
        }
    }

    private static void assertReadFails(byte[] data, Class<? extends IOException> exceptionType, String message)
            throws IOException
    {
        try (Lz4HadoopInputStream input = input(data)) {
            assertThatThrownBy(input::read)
                    .isInstanceOf(exceptionType)
                    .hasMessage(message);
        }
    }

    private static Lz4HadoopInputStream input(byte[] data)
    {
        return new Lz4HadoopInputStream(new Lz4JavaDecompressor(), new ByteArrayInputStream(data), BUFFER_SIZE);
    }

    private static byte[] ints(int... values)
            throws IOException
    {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(buffer)) {
            for (int value : values) {
                output.writeInt(value);
            }
        }
        return buffer.toByteArray();
    }

    private static byte[] concat(byte[]... parts)
            throws IOException
    {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        for (byte[] part : parts) {
            buffer.write(part);
        }
        return buffer.toByteArray();
    }
}
