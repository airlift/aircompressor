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
package io.airlift.compress.v2.lzo;

import com.google.common.io.Resources;
import io.airlift.compress.v2.AbstractTestCompression;
import io.airlift.compress.v2.Compressor;
import io.airlift.compress.v2.Decompressor;
import io.airlift.compress.v2.HadoopCodecCompressor;
import io.airlift.compress.v2.HadoopCodecDecompressor;
import io.airlift.compress.v2.HadoopNative;
import io.airlift.compress.v2.thirdparty.HadoopLzoCompressor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

class TestLzopCodec
        extends AbstractTestCompression
{
    static {
        HadoopNative.requireHadoopNative();
    }

    private final CompressionCodec verifyCodec;

    TestLzopCodec()
    {
        com.hadoop.compression.lzo.LzopCodec codec = new com.hadoop.compression.lzo.LzopCodec();
        codec.setConf(new Configuration());
        this.verifyCodec = codec;
    }

    @Override
    protected boolean isMemorySegmentSupported()
    {
        return false;
    }

    @Override
    protected Compressor getCompressor()
    {
        return new HadoopCodecCompressor(new LzopCodec(), new HadoopLzoCompressor());
    }

    @Override
    protected Decompressor getDecompressor()
    {
        return new HadoopCodecDecompressor(new LzopCodec());
    }

    @Override
    protected Compressor getVerifyCompressor()
    {
        return new HadoopCodecCompressor(verifyCodec, new HadoopLzoCompressor());
    }

    @Override
    protected Decompressor getVerifyDecompressor()
    {
        return new HadoopCodecDecompressor(verifyCodec);
    }

    @Test
    void testDecompressNewerVersion()
            throws IOException
    {
        // lzop --no-checksum -o test-no-checksum.lzo test
        // lzop -o test-adler32.lzo test
        // lzop -CC -o test-adler32-both.lzo test
        // lzop --crc32 -o test-crc32.lzo test
        // lzop --crc32 -CC -o test-crc32-both.lzo test

        assertDecompressed("no-checksum");
        assertDecompressed("adler32");
        assertDecompressed("adler32-both");
        assertDecompressed("crc32");
        assertDecompressed("crc32-both");
    }

    private void assertDecompressed(String variant)
            throws IOException
    {
        byte[] compressed = Resources.toByteArray(Resources.getResource(format("data/lzo/test-%s.lzo", variant)));
        byte[] uncompressed = Resources.toByteArray(Resources.getResource("data/lzo/test"));

        byte[] output = new byte[uncompressed.length];
        int decompressedSize = getDecompressor().decompress(compressed, 0, compressed.length, output, 0, output.length);

        assertThat(decompressedSize).isEqualTo(output.length);
        assertByteArraysEqual(uncompressed, 0, uncompressed.length, output, 0, output.length);
    }
}
