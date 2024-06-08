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
package io.airlift.compress.lz4;

import io.airlift.compress.AbstractTestCompression;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class AbstractTestLz4
        extends AbstractTestCompression
{
    @Test
    void testLiteralLengthOverflow()
            throws IOException
    {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        buffer.write((byte) 0b1111_0000); // token
        // Causes overflow for `literalLength`
        byte[] literalLengthBytes = new byte[Integer.MAX_VALUE / 255 + 1]; // ~9MB
        Arrays.fill(literalLengthBytes, (byte) 255);
        buffer.write(literalLengthBytes);
        buffer.write(1);
        buffer.write(new byte[20]);

        byte[] data = buffer.toByteArray();

        assertThatThrownBy(() -> getDecompressor().decompress(data, 0, data.length, new byte[2048], 0, 2048))
                .hasMessageMatching("Malformed input.*|Unknown error occurred.*");
    }

    @Test
    void testMatchLengthOverflow()
            throws IOException
    {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        buffer.write((byte) 0b0000_1111); // token
        buffer.write(new byte[2]); // offset

        // Causes overflow for `matchLength`
        byte[] literalLengthBytes = new byte[Integer.MAX_VALUE / 255 + 1]; // ~9MB
        Arrays.fill(literalLengthBytes, (byte) 255);
        buffer.write(literalLengthBytes);
        buffer.write(1);

        buffer.write(new byte[10]);

        byte[] data = buffer.toByteArray();

        assertThatThrownBy(() -> getDecompressor().decompress(data, 0, data.length, new byte[2048], 0, 2048))
                .hasMessageMatching("Malformed input.*|Unknown error occurred.*");
    }
}
