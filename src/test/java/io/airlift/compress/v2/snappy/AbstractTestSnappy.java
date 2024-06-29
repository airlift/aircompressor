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
package io.airlift.compress.v2.snappy;

import io.airlift.compress.v2.AbstractTestCompression;
import io.airlift.compress.v2.MalformedInputException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class AbstractTestSnappy
        extends AbstractTestCompression
{
    @Override
    protected abstract SnappyCompressor getCompressor();

    @Override
    protected abstract SnappyDecompressor getDecompressor();

    @Test
    void testInvalidLiteralLength()
    {
        byte[] data = {
                // Encoded uncompressed length 1024
                -128, 8,
                // op-code
                (byte) 252,
                // Trailer value Integer.MAX_VALUE
                (byte) 0b1111_1111, (byte) 0b1111_1111, (byte) 0b1111_1111, (byte) 0b0111_1111,
                // Some arbitrary data
                0, 0, 0, 0, 0, 0, 0, 0
        };

        assertThatThrownBy(() -> getDecompressor().decompress(data, 0, data.length, new byte[1024], 0, 1024))
                .isInstanceOf(MalformedInputException.class);
    }

    @Test
    void testNegativeLength()
    {
        byte[] data = {(byte) 255, (byte) 255, (byte) 255, (byte) 255, 0b0000_1000};

        assertThatThrownBy(() -> getDecompressor().getUncompressedLength(data, 0))
                .isInstanceOf(MalformedInputException.class)
                .hasMessageStartingWith("invalid compressed length");
    }
}
