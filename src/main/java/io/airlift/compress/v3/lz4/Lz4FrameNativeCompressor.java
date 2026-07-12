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
package io.airlift.compress.v3.lz4;

import java.lang.foreign.MemorySegment;

/**
 * Compressor that produces the standard LZ4 frame format, delegating per-block compression to the
 * native LZ4 block compressor. The frame container itself (including the xxHash32 header checksum)
 * is written in Java, so only the raw block functions of the native library are used. Frames are
 * emitted with independent blocks.
 * <p>
 * A single compressor is not safe to use by multiple threads concurrently. However, different
 * threads may use different compressors concurrently.
 */
public final class Lz4FrameNativeCompressor
        implements Lz4FrameCompressor
{
    private final Lz4NativeCompressor blockCompressor;

    public Lz4FrameNativeCompressor()
    {
        this.blockCompressor = new Lz4NativeCompressor();
    }

    public Lz4FrameNativeCompressor(int acceleration)
    {
        this.blockCompressor = new Lz4NativeCompressor(acceleration);
    }

    public static boolean isEnabled()
    {
        return Lz4NativeCompressor.isEnabled();
    }

    @Override
    public int maxCompressedLength(int uncompressedSize)
    {
        return Lz4FrameCompression.maxCompressedLength(uncompressedSize);
    }

    @Override
    public int compress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
    {
        return Lz4FrameCompression.compress(blockCompressor, input, inputOffset, inputLength, output, outputOffset, maxOutputLength);
    }

    @Override
    public int compress(MemorySegment input, MemorySegment output)
    {
        return Lz4FrameCompression.compress(blockCompressor, input, output);
    }
}
