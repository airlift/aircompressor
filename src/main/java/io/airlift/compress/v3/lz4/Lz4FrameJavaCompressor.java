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
 * Pure-Java compressor that produces the standard LZ4 frame format, delegating per-block
 * compression to the Java LZ4 block compressor. Frames are emitted with independent blocks.
 * This class is not thread-safe.
 */
public final class Lz4FrameJavaCompressor
        implements Lz4FrameCompressor
{
    private final Lz4JavaCompressor blockCompressor = new Lz4JavaCompressor();

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
