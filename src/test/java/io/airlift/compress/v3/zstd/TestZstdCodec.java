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
package io.airlift.compress.v3.zstd;

import com.google.common.io.Resources;
import com.google.common.primitives.Bytes;
import io.airlift.compress.v3.AbstractTestCompression;
import io.airlift.compress.v3.Compressor;
import io.airlift.compress.v3.Decompressor;
import io.airlift.compress.v3.HadoopCodecCompressor;
import io.airlift.compress.v3.HadoopCodecDecompressor;
import io.airlift.compress.v3.thirdparty.ZstdJniCompressor;
import io.airlift.compress.v3.thirdparty.ZstdJniDecompressor;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestZstdCodec
        extends AbstractTestCompression
{
    protected CompressionCodec createCodec()
    {
        return new ZstdCodec();
    }

    @Override
    protected boolean isMemorySegmentSupported()
    {
        return false;
    }

    @Override
    protected Compressor getCompressor()
    {
        return new HadoopCodecCompressor(createCodec(), new ZstdJavaCompressor());
    }

    @Override
    protected Decompressor getDecompressor()
    {
        return new HadoopCodecDecompressor(createCodec());
    }

    @Override
    protected Compressor getVerifyCompressor()
    {
        // Hadoop format is the standard Zstd framed format
        return new ZstdJniCompressor(3);
    }

    @Override
    protected Decompressor getVerifyDecompressor()
    {
        // Hadoop format is the standard Zstd framed format
        return new ZstdJniDecompressor();
    }

    @Test
    void testConcatenatedFrames()
            throws IOException
    {
        byte[] compressed = Resources.toByteArray(Resources.getResource("data/zstd/multiple-frames.zst"));
        byte[] uncompressed = Resources.toByteArray(Resources.getResource("data/zstd/multiple-frames"));

        byte[] output = new byte[uncompressed.length];
        getDecompressor().decompress(compressed, 0, compressed.length, output, 0, output.length);

        assertByteArraysEqual(uncompressed, 0, uncompressed.length, output, 0, output.length);
    }

    @Test
    void testResetState()
            throws IOException
    {
        byte[] original = new byte[10000];
        new Random(42).nextBytes(original);
        ByteArrayOutputStream compressed = new ByteArrayOutputStream();
        try (OutputStream out = createCodec().createOutputStream(compressed)) {
            out.write(original);
        }

        // Reset buffered decoder state after repositioning the source
        ByteArrayInputStream source = new ByteArrayInputStream(compressed.toByteArray());
        try (CompressionInputStream in = createCodec().createInputStream(source)) {
            assertThat(in.readNBytes(10)).hasSize(10);
            for (int i = 0; i < 3; i++) {
                source.reset();
                in.resetState();
                assertThat(in.readAllBytes()).isEqualTo(original);
            }
        }
    }

    @Test
    void testWriteAfterFinish()
            throws IOException
    {
        byte[] first = new byte[10000];
        new Random(43).nextBytes(first);
        byte[] second = new byte[10000];
        new Random(44).nextBytes(second);

        ByteArrayOutputStream compressed = new ByteArrayOutputStream();
        try (CompressionOutputStream out = createCodec().createOutputStream(compressed)) {
            out.write(first);
            out.finish();
            out.write(second);
        }

        try (InputStream in = createCodec().createInputStream(new ByteArrayInputStream(compressed.toByteArray()))) {
            assertThat(in.readAllBytes()).isEqualTo(Bytes.concat(first, second));
        }
    }

    @Test
    void testReadAfterRepeatedClose()
            throws IOException
    {
        InputStream in = createCodec().createInputStream(new ByteArrayInputStream(new byte[0]));
        in.close();
        in.close();
        assertThatThrownBy(in::read)
                .isInstanceOf(IOException.class)
                .hasMessage("Stream is closed");
    }
}
