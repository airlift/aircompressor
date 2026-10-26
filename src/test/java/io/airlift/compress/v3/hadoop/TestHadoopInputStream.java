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
package io.airlift.compress.v3.hadoop;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestHadoopInputStream
{
    @Test
    void testInvalidCompressedLengths()
    {
        for (int length : new int[] {0, -2, Integer.MAX_VALUE - Long.BYTES + 1, Integer.MAX_VALUE}) {
            assertThatThrownBy(() -> HadoopInputStream.readCompressedBlock(new ByteArrayInputStream(new byte[0]), new byte[0], length))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Invalid compressed chunk length");
        }
    }

    @Test
    void testLargeTruncatedBlock()
    {
        assertThatThrownBy(() -> HadoopInputStream.readCompressedBlock(new ByteArrayInputStream(new byte[0]), new byte[0], 1024 * 1024 * 1024))
                .isInstanceOf(EOFException.class);
    }

    @Test
    void testLargeBlockAndBufferReuse()
            throws IOException
    {
        byte[] input = new byte[1_000_000];
        new Random(0).nextBytes(input);
        byte[] buffer = HadoopInputStream.readCompressedBlock(new ByteArrayInputStream(input), new byte[0], input.length);
        assertThat(buffer).startsWith(input);
        assertThat(buffer).hasSize(input.length + Long.BYTES);
        assertThat(HadoopInputStream.readCompressedBlock(new ByteArrayInputStream(input), buffer, input.length)).isSameAs(buffer);
    }
}
