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
package io.airlift.compress.v3.deflate;

import java.lang.foreign.MemorySegment;
import java.lang.ref.Cleaner;

import static java.lang.Math.toIntExact;

/**
 * A single compressor is not safe to use by multiple threads concurrently.
 * However, different threads may use different compressors concurrently.
 */
public class DeflateNativeCompressor
        implements DeflateCompressor
{
    private static final Cleaner CLEANER = Cleaner.create();
    private static final int DEFAULT_COMPRESSION_LEVEL = 6;

    private final MemorySegment compressor;

    public DeflateNativeCompressor()
    {
        this(DEFAULT_COMPRESSION_LEVEL);
    }

    public DeflateNativeCompressor(int compressionLevel)
    {
        DeflateNative.verifyEnabled();
        if (compressionLevel < 0 || compressionLevel > 12) {
            throw new IllegalArgumentException("Invalid compression level: %d (must be 0-12)".formatted(compressionLevel));
        }
        this.compressor = DeflateNative.allocCompressor(compressionLevel);
        CLEANER.register(this, new CompressorCleaner(compressor));
    }

    public static boolean isEnabled()
    {
        return DeflateNative.isEnabled();
    }

    @Override
    public int maxCompressedLength(int uncompressedSize)
    {
        return toIntExact(DeflateNative.compressBound(compressor, uncompressedSize));
    }

    @Override
    public int compress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
    {
        MemorySegment inputSegment = MemorySegment.ofArray(input).asSlice(inputOffset, inputLength);
        MemorySegment outputSegment = MemorySegment.ofArray(output).asSlice(outputOffset, maxOutputLength);
        return toIntExact(DeflateNative.compress(compressor, inputSegment, inputLength, outputSegment, maxOutputLength));
    }

    @Override
    public int compress(MemorySegment input, MemorySegment output)
    {
        return toIntExact(DeflateNative.compress(compressor, input, input.byteSize(), output, output.byteSize()));
    }

    private record CompressorCleaner(MemorySegment compressor)
            implements Runnable
    {
        @Override
        public void run()
        {
            DeflateNative.freeCompressor(compressor);
        }
    }
}
