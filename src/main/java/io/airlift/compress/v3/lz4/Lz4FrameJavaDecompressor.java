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

import io.airlift.compress.v3.MalformedInputException;

import java.lang.foreign.MemorySegment;

/**
 * Pure-Java decompressor for the standard LZ4 frame format, delegating per-block decompression to
 * the Java LZ4 block decompressor. Multiple concatenated frames are decoded, and skippable frames
 * are ignored. Only frames with independent blocks and no dictionary are supported (as produced by
 * the Java and native frame compressors); frames using linked blocks or a dictionary are rejected.
 * This class is not thread-safe.
 */
public final class Lz4FrameJavaDecompressor
        implements Lz4FrameDecompressor
{
    private final Lz4JavaDecompressor blockDecompressor = new Lz4JavaDecompressor();

    @Override
    public int decompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
            throws MalformedInputException
    {
        return Lz4FrameCompression.decompress(blockDecompressor, input, inputOffset, inputLength, output, outputOffset, maxOutputLength);
    }

    @Override
    public int decompress(MemorySegment input, MemorySegment output)
            throws MalformedInputException
    {
        return Lz4FrameCompression.decompress(blockDecompressor, input, output);
    }
}
