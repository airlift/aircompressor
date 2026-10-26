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

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.Adler32;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestLzoHadoopInputStreams
{
    @Test
    void testMissingCompressedChunkHeader()
            throws IOException
    {
        byte[] header = {0, 0, 0, 1};
        try (var input = new LzoHadoopInputStream(new ByteArrayInputStream(header), 64)) {
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
            try (var input = new LzoHadoopInputStream(new ByteArrayInputStream(header), 64)) {
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
        try (var input = new LzoHadoopInputStream(new ByteArrayInputStream(header), 64)) {
            assertThatThrownBy(input::read)
                    .isInstanceOf(EOFException.class)
                    .hasMessageContaining("reading block data");
        }
    }

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

    private static ByteArrayInputStream createEmptyLzopStream()
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
        writeBigEndianInt(output, 0);
        return new ByteArrayInputStream(output.toByteArray());
    }

    private static void writeBigEndianInt(ByteArrayOutputStream output, int value)
    {
        output.write(value >>> 24);
        output.write(value >>> 16);
        output.write(value >>> 8);
        output.write(value);
    }
}
