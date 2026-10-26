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
package io.airlift.compress.v3.internal;

import io.airlift.compress.v3.lz4.Lz4JavaCompressor;
import io.airlift.compress.v3.snappy.SnappyJavaCompressor;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Random;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.assertj.core.api.Assertions.assertThat;

class TestLiteralDecompressor
{
    @Test
    void testLz4LiteralBlockAndPadding()
    {
        byte[] data = new byte[131072];
        new Random(42).nextBytes(data);
        Lz4JavaCompressor compressor = new Lz4JavaCompressor();
        byte[] encoded = new byte[compressor.maxCompressedLength(data.length)];
        int length = compressor.compress(data, 0, data.length, encoded, 0, encoded.length);
        byte[] output = new byte[data.length + 14];
        Arrays.fill(output, (byte) 42);
        MemorySegment destination = MemorySegment.ofArray(output).asSlice(7, data.length);
        assertThat(LiteralDecompressor.tryLz4(MemorySegment.ofArray(encoded).asSlice(0, length), length, destination, data.length)).isEqualTo(data.length);
        assertThat(Arrays.copyOfRange(output, 7, 7 + data.length)).isEqualTo(data);
        assertThat(Arrays.copyOfRange(output, 0, 7)).containsOnly((byte) 42);
        assertThat(Arrays.copyOfRange(output, output.length - 7, output.length)).containsOnly((byte) 42);
    }

    @Test
    void testSnappyLiteralLengths()
    {
        // Exercise all four extended literal length widths, including legal nonminimal encodings.
        for (int bytes = 1; bytes <= 4; bytes++) {
            int size = 200;
            byte[] encoded = new byte[2 + 1 + bytes + size];
            encoded[0] = (byte) 0xC8;
            encoded[1] = 1;
            encoded[2] = (byte) ((59 + bytes) << 2);
            encoded[3] = (byte) (size - 1);
            for (int i = 0; i < size; i++) {
                encoded[3 + bytes + i] = (byte) i;
            }
            byte[] output = new byte[size + 7];
            Arrays.fill(output, (byte) 42);
            assertThat(LiteralDecompressor.trySnappy(MemorySegment.ofArray(encoded), encoded.length, MemorySegment.ofArray(output), output.length)).isEqualTo(size);
            for (int i = 0; i < size; i++) {
                assertThat(output[i]).isEqualTo((byte) i);
            }
            assertThat(Arrays.copyOfRange(output, size, output.length)).containsOnly((byte) 42);
        }
    }

    @Test
    void testSnappyMultipleLiterals()
    {
        byte[] data = new byte[131072];
        new Random(42).nextBytes(data);
        SnappyJavaCompressor compressor = new SnappyJavaCompressor();
        byte[] encoded = new byte[compressor.maxCompressedLength(data.length)];
        int length = compressor.compress(data, 0, data.length, encoded, 0, encoded.length);
        MemorySegment output = MemorySegment.ofArray(new byte[data.length]);
        assertThat(LiteralDecompressor.trySnappy(MemorySegment.ofArray(encoded), length, output, data.length)).isEqualTo(data.length);
        assertThat(output.toArray(JAVA_BYTE)).isEqualTo(data);
    }

    @Test
    void testZstdRawFrame()
    {
        byte[] data = new byte[256 * 1024];
        new Random(42).nextBytes(data);
        for (boolean singleSegment : new boolean[] {false, true}) {
            ByteBuffer encoded = ByteBuffer.allocate(data.length + 16).order(ByteOrder.LITTLE_ENDIAN);
            encoded.putInt(0xFD2FB528);
            encoded.put((byte) (singleSegment ? 0xA0 : 0x80));
            if (!singleSegment) {
                encoded.put((byte) 0x58); // 2 MiB window
            }
            encoded.putInt(data.length);
            for (int offset = 0; offset < data.length; offset += 128 * 1024) {
                int block = ((128 * 1024) << 3) | (offset == 0 ? 0 : 1);
                encoded.put((byte) block).put((byte) (block >>> 8)).put((byte) (block >>> 16));
                encoded.put(data, offset, 128 * 1024);
            }
            byte[] frame = Arrays.copyOf(encoded.array(), encoded.position());
            MemorySegment output = MemorySegment.ofArray(new byte[data.length + 7]).fill((byte) 42);
            assertThat(LiteralDecompressor.tryZstdRaw(MemorySegment.ofArray(frame), frame.length, output, output.byteSize())).isEqualTo(data.length);
            assertThat(output.asSlice(0, data.length).toArray(JAVA_BYTE)).isEqualTo(data);
            assertThat(output.asSlice(data.length).toArray(JAVA_BYTE)).containsOnly((byte) 42);
            for (int mutation = 0; mutation < 6; mutation++) {
                byte[] invalid = frame.clone();
                switch (mutation) {
                    case 0 -> invalid[0] = 0;
                    case 1 -> invalid[4] |= 4; // checksum must not be silently ignored
                    case 2 -> invalid[5 + (singleSegment ? 0 : 1)]++; // mismatched content size
                    case 3 -> invalid[invalid.length - 128 * 1024 - 3] &= (byte) ~1; // no last block
                    case 4 -> invalid[9 + (singleSegment ? 0 : 1)] |= 4; // compressed block
                    case 5 -> invalid = Arrays.copyOf(frame, frame.length + 1); // trailing data
                    default -> throw new AssertionError();
                }
                output.fill((byte) 42);
                assertThat(LiteralDecompressor.tryZstdRaw(MemorySegment.ofArray(invalid), invalid.length, output, output.byteSize())).isEqualTo(-1);
                assertThat(output.toArray(JAVA_BYTE)).containsOnly((byte) 42);
            }
            MemorySegment small = MemorySegment.ofArray(new byte[data.length - 1]).fill((byte) 42);
            assertThat(LiteralDecompressor.tryZstdRaw(MemorySegment.ofArray(frame), frame.length, small, small.byteSize())).isEqualTo(-1);
            assertThat(small.toArray(JAVA_BYTE)).containsOnly((byte) 42);
        }
    }

    @Test
    void testMalformedAndMatchEncodingsLeaveOutputUnchanged()
    {
        for (byte[] encoded : new byte[][] {
                {}, {(byte) 0xF0}, {(byte) 0xF0, (byte) 255}, {0x30, 1, 2},
                {0x10, 1, 1, 0, 0x50, 1, 2, 3, 4, 5}}) {
            MemorySegment output = MemorySegment.ofArray(new byte[1024]).fill((byte) 42);
            assertThat(LiteralDecompressor.tryLz4(MemorySegment.ofArray(encoded), encoded.length, output, output.byteSize())).isEqualTo(-1);
            assertThat(output.toArray(JAVA_BYTE)).containsOnly((byte) 42);
        }
        for (byte[] encoded : new byte[][] {
                {}, {(byte) 0x80}, {5, 8, 1, 2, 3}, {1, 0, 42, 0},
                {1, 0, 42, 1, 1}, {1, (byte) 0xFC, (byte) 255, (byte) 255, (byte) 255, (byte) 255},
                {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x1F}}) {
            MemorySegment output = MemorySegment.ofArray(new byte[1024]).fill((byte) 42);
            assertThat(LiteralDecompressor.trySnappy(MemorySegment.ofArray(encoded), encoded.length, output, output.byteSize())).isEqualTo(-1);
            assertThat(output.toArray(JAVA_BYTE)).containsOnly((byte) 42);
        }
    }
}
