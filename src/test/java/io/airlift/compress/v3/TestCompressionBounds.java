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
package io.airlift.compress.v3;

import io.airlift.compress.v3.deflate.DeflateJavaCompressor;
import io.airlift.compress.v3.deflate.DeflateJavaDecompressor;
import io.airlift.compress.v3.lz4.Lz4FrameJavaCompressor;
import io.airlift.compress.v3.lz4.Lz4FrameJavaDecompressor;
import io.airlift.compress.v3.lz4.Lz4JavaCompressor;
import io.airlift.compress.v3.lz4.Lz4JavaDecompressor;
import io.airlift.compress.v3.lzo.LzoCompressor;
import io.airlift.compress.v3.lzo.LzoDecompressor;
import io.airlift.compress.v3.snappy.SnappyJavaCompressor;
import io.airlift.compress.v3.snappy.SnappyJavaDecompressor;
import io.airlift.compress.v3.zstd.ZstdJavaCompressor;
import io.airlift.compress.v3.zstd.ZstdJavaDecompressor;
import org.junit.jupiter.api.Test;

import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestCompressionBounds
{
    private record Codec(Compressor compressor, Decompressor decompressor) {}

    @Test
    void testOverflowingArrayRanges()
    {
        byte[] input = "hello world hello world hello world".getBytes(UTF_8);
        for (Codec codec : List.of(
                new Codec(new Lz4JavaCompressor(), new Lz4JavaDecompressor()),
                new Codec(new Lz4FrameJavaCompressor(), new Lz4FrameJavaDecompressor()),
                new Codec(new LzoCompressor(), new LzoDecompressor()),
                new Codec(new SnappyJavaCompressor(), new SnappyJavaDecompressor()),
                new Codec(new ZstdJavaCompressor(), new ZstdJavaDecompressor()),
                new Codec(new DeflateJavaCompressor(), new DeflateJavaDecompressor()))) {
            byte[] compressed = new byte[codec.compressor().maxCompressedLength(input.length)];
            int compressedLength = codec.compressor().compress(input, 0, input.length, compressed, 0, compressed.length);
            byte[] output = new byte[128];
            assertThatThrownBy(() -> codec.compressor().compress(input, 1, Integer.MAX_VALUE, compressed, 0, compressed.length))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> codec.compressor().compress(input, 0, input.length, compressed, 1, Integer.MAX_VALUE))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> codec.decompressor().decompress(compressed, 1, Integer.MAX_VALUE, output, 0, output.length))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> codec.decompressor().decompress(compressed, 0, compressedLength, output, 1, Integer.MAX_VALUE))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void testLengthHeaderRanges()
    {
        byte[] input = new byte[16];
        assertThatThrownBy(() -> new SnappyJavaDecompressor().getUncompressedLength(input, -1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SnappyJavaDecompressor().getUncompressedLength(input, Integer.MAX_VALUE))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ZstdJavaDecompressor().getDecompressedSize(input, 1, Integer.MAX_VALUE))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
