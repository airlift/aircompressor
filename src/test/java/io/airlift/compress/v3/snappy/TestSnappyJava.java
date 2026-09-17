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
package io.airlift.compress.v3.snappy;

import io.airlift.compress.v3.Compressor;
import io.airlift.compress.v3.Decompressor;
import io.airlift.compress.v3.MalformedInputException;
import io.airlift.compress.v3.thirdparty.XerialSnappyCompressor;
import io.airlift.compress.v3.thirdparty.XerialSnappyDecompressor;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestSnappyJava
        extends AbstractTestSnappy
{
    @Override
    protected SnappyCompressor getCompressor()
    {
        return new SnappyJavaCompressor();
    }

    @Override
    protected SnappyDecompressor getDecompressor()
    {
        return new SnappyJavaDecompressor();
    }

    @Override
    protected Compressor getVerifyCompressor()
    {
        return new XerialSnappyCompressor();
    }

    @Override
    protected Decompressor getVerifyDecompressor()
    {
        return new XerialSnappyDecompressor();
    }

    @Test
    void testUncompressedLengthBoundaries()
    {
        SnappyDecompressor decompressor = getDecompressor();
        byte[][] inputs = {
                {0},
                {0x7f},
                {(byte) 0x80, 1},
                {(byte) 0xff, 0x7f},
                {(byte) 0x80, (byte) 0x80, 1},
                {(byte) 0xff, (byte) 0xff, 0x7f},
                {(byte) 0x80, (byte) 0x80, (byte) 0x80, 1},
                {(byte) 0xff, (byte) 0xff, (byte) 0xff, 0x7f},
                {(byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, 1},
                {(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, 7},
                {(byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, 0},
        };
        int[] lengths = {0, 127, 128, 16383, 16384, 2097151, 2097152, 268435455, 268435456, Integer.MAX_VALUE, 0};
        for (int i = 0; i < inputs.length; i++) {
            assertThat(decompressor.getUncompressedLength(inputs[i], 0)).isEqualTo(lengths[i]);
        }
    }

    @Test
    void testInvalidUncompressedLength()
    {
        SnappyDecompressor decompressor = getDecompressor();
        for (int lastByte = 8; lastByte <= 0xff; lastByte++) {
            String expectedMessage = lastByte < 0x10
                    ? "invalid compressed length"
                    : "last byte of compressed length int has high bits set";

            byte[] input = {(byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) lastByte};
            assertThatThrownBy(() -> decompressor.getUncompressedLength(input, 0))
                    .isInstanceOf(MalformedInputException.class)
                    .hasMessageStartingWith(expectedMessage);

            byte[] paddedInput = new byte[input.length + 3];
            System.arraycopy(input, 0, paddedInput, 3, input.length);
            assertThatThrownBy(() -> decompressor.getUncompressedLength(paddedInput, 3))
                    .isInstanceOf(MalformedInputException.class)
                    .hasMessageStartingWith(expectedMessage);
        }
    }

    @Test
    void testDecompressInvalidUncompressedLength()
    {
        SnappyDecompressor decompressor = getDecompressor();
        for (int lastByte = 8; lastByte <= 0xff; lastByte++) {
            String expectedMessage = lastByte < 0x10
                    ? "invalid compressed length"
                    : "last byte of compressed length int has high bits set";

            byte[] input = {(byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) lastByte};
            byte[] output = new byte[1];
            assertThatThrownBy(() -> decompressor.decompress(input, 0, input.length, output, 0, output.length))
                    .isInstanceOf(MalformedInputException.class)
                    .hasMessageStartingWith(expectedMessage);
        }
    }

    @Test
    void testZeroMatchOffsetFails()
    {
        byte[] zeroMatchOffset = new byte[] {16, 1, 0, 1, 0, 1, 0, 1, 0};
        assertThatThrownBy(() -> new SnappyJavaDecompressor().decompress(zeroMatchOffset, 0, zeroMatchOffset.length, new byte[64], 0, 64))
                .isInstanceOf(MalformedInputException.class)
                .hasMessageContaining("Malformed input: offset=2");
    }
}
