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

import io.airlift.compress.v3.deflate.DeflateNativeCompressor;
import io.airlift.compress.v3.deflate.DeflateNativeDecompressor;
import io.airlift.compress.v3.lz4.Lz4JavaCompressor;
import io.airlift.compress.v3.lz4.Lz4JavaDecompressor;
import io.airlift.compress.v3.lz4.Lz4NativeCompressor;
import io.airlift.compress.v3.lz4.Lz4NativeDecompressor;
import io.airlift.compress.v3.lzo.LzoCompressor;
import io.airlift.compress.v3.lzo.LzoDecompressor;
import io.airlift.compress.v3.snappy.SnappyJavaCompressor;
import io.airlift.compress.v3.snappy.SnappyJavaDecompressor;
import io.airlift.compress.v3.snappy.SnappyNativeCompressor;
import io.airlift.compress.v3.snappy.SnappyNativeDecompressor;
import io.airlift.compress.v3.zstd.ZstdJavaCompressor;
import io.airlift.compress.v3.zstd.ZstdJavaDecompressor;
import io.airlift.compress.v3.zstd.ZstdNativeCompressor;
import io.airlift.compress.v3.zstd.ZstdNativeDecompressor;
import org.junit.jupiter.api.Test;

import java.lang.WrongThreadException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestMemorySegmentCompression
{
    private static final byte[] DATA = "hello world hello world hello world".getBytes(UTF_8);

    private record Codec(Compressor compressor, Decompressor decompressor) {}

    private static List<Codec> javaCodecs()
    {
        return List.of(
                new Codec(new Lz4JavaCompressor(), new Lz4JavaDecompressor()),
                new Codec(new LzoCompressor(), new LzoDecompressor()),
                new Codec(new SnappyJavaCompressor(), new SnappyJavaDecompressor()),
                new Codec(new ZstdJavaCompressor(), new ZstdJavaDecompressor()));
    }

    @Test
    void testSharedOutputLargerThanCompressionBound()
    {
        for (Codec codec : javaCodecs()) {
            try (Arena arena = Arena.ofShared()) {
                MemorySegment compressed = arena.allocate(1024 * 1024).fill((byte) 42);
                int bound = codec.compressor().maxCompressedLength(DATA.length);
                int written = codec.compressor().compress(MemorySegment.ofArray(DATA), compressed);
                byte[] decoded = new byte[DATA.length];
                assertThat(codec.decompressor().decompress(compressed.asSlice(0, written), MemorySegment.ofArray(decoded))).isEqualTo(DATA.length);
                assertThat(decoded).isEqualTo(DATA);
                assertThat(compressed.asSlice(written).toArray(JAVA_BYTE)).containsOnly((byte) 42);
                assertThat(written).isLessThanOrEqualTo(bound);
            }
        }
    }

    @Test
    void testReadOnlyOutput()
    {
        List<Codec> codecs = new ArrayList<>(javaCodecs());
        if (Lz4NativeCompressor.isEnabled()) {
            codecs.add(new Codec(new Lz4NativeCompressor(), new Lz4NativeDecompressor()));
        }
        if (SnappyNativeCompressor.isEnabled()) {
            codecs.add(new Codec(new SnappyNativeCompressor(), new SnappyNativeDecompressor()));
        }
        if (ZstdNativeCompressor.isEnabled()) {
            codecs.add(new Codec(new ZstdNativeCompressor(), new ZstdNativeDecompressor()));
        }
        if (DeflateNativeCompressor.isEnabled()) {
            codecs.add(new Codec(new DeflateNativeCompressor(), new DeflateNativeDecompressor()));
        }
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment output = arena.allocate(256).asReadOnly();
            for (Codec codec : codecs) {
                MemorySegment input = MemorySegment.ofArray(DATA);
                assertThatThrownBy(() -> codec.compressor().compress(input, output))
                        .isInstanceOf(IllegalArgumentException.class);
                byte[] compressed = new byte[256];
                int length = codec.compressor().compress(DATA, 0, DATA.length, compressed, 0, compressed.length);
                assertThatThrownBy(() -> codec.decompressor().decompress(MemorySegment.ofArray(compressed).asSlice(0, length), output))
                        .isInstanceOf(IllegalArgumentException.class);
            }
        }
    }

    @Test
    void testReadOnlyInputAndSharedSegments()
    {
        for (Codec codec : javaCodecs()) {
            for (boolean shared : List.of(false, true)) {
                try (Arena arena = shared ? Arena.ofShared() : Arena.ofConfined()) {
                    MemorySegment input = arena.allocate(DATA.length);
                    input.copyFrom(MemorySegment.ofArray(DATA));
                    MemorySegment compressed = arena.allocate(256);
                    int length = codec.compressor().compress(input.asReadOnly(), compressed);
                    MemorySegment output = arena.allocate(DATA.length);
                    int written = codec.decompressor().decompress(compressed.asSlice(0, length).asReadOnly(), output);
                    assertThat(written).isEqualTo(DATA.length);
                    assertThat(output.toArray(JAVA_BYTE)).isEqualTo(DATA);
                    // Read-only heap input must be accepted too.
                    assertThat(codec.compressor().compress(MemorySegment.ofArray(DATA).asReadOnly(), compressed)).isPositive();
                }
            }
        }
    }

    @Test
    void testClosedAndWrongThreadSegments()
            throws Exception
    {
        for (Codec codec : javaCodecs()) {
            MemorySegment closed;
            try (Arena arena = Arena.ofConfined()) {
                closed = arena.allocate(256);
            }
            MemorySegment input = MemorySegment.ofArray(DATA);
            MemorySegment output = MemorySegment.ofArray(new byte[256]);
            assertThatThrownBy(() -> codec.compressor().compress(closed, output)).isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> codec.compressor().compress(input, closed)).isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> codec.decompressor().decompress(closed, output)).isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> codec.decompressor().decompress(input, closed)).isInstanceOf(IllegalStateException.class);

            try (Arena arena = Arena.ofConfined(); var executor = Executors.newSingleThreadExecutor()) {
                MemorySegment confined = arena.allocate(256);
                executor.submit(() -> {
                    assertThatThrownBy(() -> codec.compressor().compress(input, confined)).isInstanceOf(WrongThreadException.class);
                    assertThatThrownBy(() -> codec.compressor().compress(confined, output)).isInstanceOf(WrongThreadException.class);
                    assertThatThrownBy(() -> codec.decompressor().decompress(input, confined)).isInstanceOf(WrongThreadException.class);
                    assertThatThrownBy(() -> codec.decompressor().decompress(confined, output)).isInstanceOf(WrongThreadException.class);
                }).get();
            }
        }
    }
}
