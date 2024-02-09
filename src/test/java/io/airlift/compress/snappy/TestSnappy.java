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
package io.airlift.compress.snappy;

import io.airlift.compress.AbstractTestCompression;
import io.airlift.compress.Compressor;
import io.airlift.compress.Decompressor;
import io.airlift.compress.MalformedInputException;
import io.airlift.compress.thirdparty.XerialSnappyCompressor;
import io.airlift.compress.thirdparty.XerialSnappyDecompressor;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSnappy
        extends AbstractTestCompression
{
    @Override
    protected Compressor getCompressor()
    {
        return new SnappyCompressor();
    }

    @Override
    protected Decompressor getDecompressor()
    {
        return new SnappyDecompressor();
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
    public void testInvalidLiteralLength()
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

        assertThatThrownBy(() -> new SnappyDecompressor().decompress(data, 0, data.length, new byte[1024], 0, 1024))
                .isInstanceOf(MalformedInputException.class);
    }
}
