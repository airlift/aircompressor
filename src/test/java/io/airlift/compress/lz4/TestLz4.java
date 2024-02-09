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
import io.airlift.compress.Compressor;
import io.airlift.compress.Decompressor;
import io.airlift.compress.MalformedInputException;
import io.airlift.compress.thirdparty.JPountzLz4Compressor;
import io.airlift.compress.thirdparty.JPountzLz4Decompressor;
import net.jpountz.lz4.LZ4Factory;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestLz4
        extends AbstractTestCompression
{
    @Override
    protected Compressor getCompressor()
    {
        return new Lz4Compressor();
    }

    @Override
    protected Decompressor getDecompressor()
    {
        return new Lz4Decompressor();
    }

    @Override
    protected Compressor getVerifyCompressor()
    {
        return new JPountzLz4Compressor(LZ4Factory.fastestInstance());
    }

    @Override
    protected Decompressor getVerifyDecompressor()
    {
        return new JPountzLz4Decompressor(LZ4Factory.fastestInstance());
    }

    @Test
    public void testLiteralLengthOverflow()
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

        assertThatThrownBy(() -> new Lz4Decompressor().decompress(data, 0, data.length, new byte[2048], 0, 2048))
                .isInstanceOf(MalformedInputException.class);
    }

    @Test
    public void testMatchLengthOverflow()
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

        assertThatThrownBy(() -> new Lz4Decompressor().decompress(data, 0, data.length, new byte[2048], 0, 2048))
                .isInstanceOf(MalformedInputException.class);
    }
}
