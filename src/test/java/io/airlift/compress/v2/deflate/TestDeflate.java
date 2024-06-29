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
package io.airlift.compress.v2.deflate;

import io.airlift.compress.v2.AbstractTestCompression;
import io.airlift.compress.v2.Compressor;
import io.airlift.compress.v2.Decompressor;
import io.airlift.compress.v2.HadoopNative;
import io.airlift.compress.v2.MalformedInputException;
import io.airlift.compress.v2.lzo.LzoDecompressor;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDeflate
        extends AbstractTestCompression
{
    static {
        HadoopNative.requireHadoopNative();
    }

    @Override
    protected Compressor getCompressor()
    {
        return new DeflateCompressor();
    }

    @Override
    protected Decompressor getDecompressor()
    {
        return new DeflateDecompressor();
    }

    @Override
    protected Compressor getVerifyCompressor()
    {
        return new DeflateCompressor();
    }

    @Override
    protected Decompressor getVerifyDecompressor()
    {
        return new DeflateDecompressor();
    }

    @Test
    void testLiteralLengthOverflow()
            throws IOException
    {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        // Command
        buffer.write(0);
        // Causes overflow for `literalLength`
        buffer.write(new byte[Integer.MAX_VALUE / 255 + 1]); // ~9MB
        buffer.write(1);

        byte[] data = buffer.toByteArray();

        assertThatThrownBy(() -> new LzoDecompressor().decompress(data, 0, data.length, new byte[20000], 0, 20000))
                .isInstanceOf(MalformedInputException.class);
    }

    @Test
    void testMatchLengthOverflow1()
            throws IOException
    {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        // Write some data so that `matchOffset` validation later passes
        // Command
        buffer.write(0);
        buffer.write(new byte[66]);
        buffer.write(8);
        buffer.write(new byte[2107 * 8]);

        // Command
        buffer.write(0b001_0000);
        // Causes overflow for `matchLength`
        buffer.write(new byte[Integer.MAX_VALUE / 255 + 1]); // ~9MB
        buffer.write(1);
        // Trailer
        buffer.write(0b0000_0000);
        buffer.write(0b0000_0100);

        buffer.write(new byte[10]);

        byte[] data = buffer.toByteArray();

        assertThatThrownBy(() -> new LzoDecompressor().decompress(data, 0, data.length, new byte[20000], 0, 20000))
                .isInstanceOf(MalformedInputException.class);
    }

    @Test
    void testMatchLengthOverflow2()
            throws IOException
    {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        // Write some data so that `matchOffset` validation later passes
        // Command
        buffer.write(0);
        buffer.write(246);
        buffer.write(new byte[264]);

        // Command
        buffer.write(0b0010_0000);
        // Causes overflow for `matchLength`
        buffer.write(new byte[Integer.MAX_VALUE / 255 + 1]); // ~9MB
        buffer.write(1);
        // Trailer
        buffer.write(0b0000_0000);
        buffer.write(0b0000_0100);

        buffer.write(new byte[10]);

        byte[] data = buffer.toByteArray();

        assertThatThrownBy(() -> new LzoDecompressor().decompress(data, 0, data.length, new byte[20000], 0, 20000))
                .isInstanceOf(MalformedInputException.class);
    }
}
