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

import io.airlift.compress.v3.Compressor;

import java.lang.foreign.MemorySegment;

public interface ZstdCompressor
        extends Compressor
{
    int compress(MemorySegment input, MemorySegment output);

    static ZstdCompressor create()
    {
        if (ZstdNativeCompressor.isEnabled()) {
            return new ZstdNativeCompressor();
        }
        return new ZstdJavaCompressor();
    }

    static ZstdCompressor create(int compressionLevel)
    {
        if (ZstdNativeCompressor.isEnabled()) {
            return new ZstdNativeCompressor(compressionLevel);
        }
        if (compressionLevel != CompressionParameters.DEFAULT_COMPRESSION_LEVEL) {
            throw new IllegalArgumentException("Compression level different from default cannot be used for non-native Zstd compressor");
        }
        return new ZstdJavaCompressor();
    }
}
