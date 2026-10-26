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

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestSnappyHadoopInputStream
{
    @Test
    void testMissingCompressedChunkHeader()
            throws IOException
    {
        byte[] header = {0, 0, 0, 1};
        try (var input = new SnappyHadoopInputStream(new SnappyJavaDecompressor(), new ByteArrayInputStream(header))) {
            assertThatThrownBy(input::read)
                    .isInstanceOf(EOFException.class)
                    .hasMessageContaining("reading block header");
        }
    }

    @Test
    void testNegativeBlockLengths()
            throws IOException
    {
        for (byte[] header : new byte[][] {{(byte) 0x80, 0, 0, 0}, {0, 0, 0, 1, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff}}) {
            try (var input = new SnappyHadoopInputStream(new SnappyJavaDecompressor(), new ByteArrayInputStream(header))) {
                assertThatThrownBy(input::read)
                        .isInstanceOf(IOException.class)
                        .hasMessageContaining("Negative block length");
            }
        }
    }

    @Test
    void testLargeTruncatedChunk()
            throws IOException
    {
        byte[] header = {0, 0, 0, 1, 0x40, 0, 0, 0};
        try (var input = new SnappyHadoopInputStream(new SnappyJavaDecompressor(), new ByteArrayInputStream(header))) {
            assertThatThrownBy(input::read)
                    .isInstanceOf(EOFException.class)
                    .hasMessageContaining("reading block data");
        }
    }

    @Test
    void zeroLengthReadAtEndOfStreamReturnsZero()
            throws IOException
    {
        try (SnappyHadoopInputStream input = new SnappyHadoopInputStream(
                new SnappyJavaDecompressor(), new ByteArrayInputStream(new byte[0]))) {
            assertThat(input.read(new byte[0], 0, 0)).isEqualTo(0);
            assertThat(input.read()).isEqualTo(-1);
        }
    }
}
