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
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestLz4InputValidation
{
    @Test
    void testOverflowingArrayRanges()
    {
        byte[] input = new byte[16];
        byte[] output = new byte[16];
        assertThatThrownBy(() -> new Lz4JavaDecompressor().decompress(input, 1, Integer.MAX_VALUE, output, 0, output.length))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Lz4JavaCompressor().compress(input, 0, input.length, output, 1, Integer.MAX_VALUE))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Lz4FrameCompression.decompress(new Lz4JavaDecompressor(), input, 1, Integer.MAX_VALUE, output, 0, output.length))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testInvalidHadoopHeaders()
            throws IOException
    {
        for (byte[] data : new byte[][] {
                header(1),
                header(-1),
                header(1, -1),
                header(1, 0),
                header(1, Integer.MAX_VALUE),
                header(100, 1000),
                header(100, 10)}) {
            assertThatThrownBy(() -> input(data).read()).isInstanceOf(IOException.class);
        }
        assertThat(input(new byte[0]).read()).isEqualTo(-1);
    }

    @Test
    void testHadoopRoundTrip()
            throws IOException
    {
        byte[] original = new byte[40_000];
        new java.util.Random(42).nextBytes(original);
        ByteArrayOutputStream compressed = new ByteArrayOutputStream();
        try (Lz4HadoopOutputStream output = new Lz4HadoopOutputStream(new Lz4JavaCompressor(), compressed, 4096)) {
            output.write(original);
        }
        assertThat(input(compressed.toByteArray()).readAllBytes()).isEqualTo(original);
    }

    private static Lz4HadoopInputStream input(byte[] data)
    {
        return new Lz4HadoopInputStream(new Lz4JavaDecompressor(), new ByteArrayInputStream(data), 4096);
    }

    private static byte[] header(int... values)
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
}
