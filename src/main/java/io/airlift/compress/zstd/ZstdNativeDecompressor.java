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
package io.airlift.compress.zstd;

import java.lang.foreign.MemorySegment;

import static java.lang.Math.toIntExact;

public class ZstdNativeDecompressor
        implements ZstdDecompressor
{
    public ZstdNativeDecompressor()
    {
        ZstdNative.verifyEnabled();
    }

    public static boolean isEnabled()
    {
        return ZstdNative.isEnabled();
    }

    @Override
    public int decompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
    {
        MemorySegment inputSegment = MemorySegment.ofArray(input).asSlice(inputOffset, inputLength);
        MemorySegment outputSegment = MemorySegment.ofArray(output).asSlice(outputOffset, maxOutputLength);
        return toIntExact(ZstdNative.decompress(inputSegment, inputLength, outputSegment, maxOutputLength));
    }

    @Override
    public int decompress(MemorySegment input, MemorySegment output)
    {
        return toIntExact(ZstdNative.decompress(input, input.byteSize(), output, output.byteSize()));
    }

    @Override
    public long getDecompressedSize(byte[] input, int offset, int length)
    {
        MemorySegment inputSegment = MemorySegment.ofArray(input).asSlice(offset, length);
        return ZstdNative.decompressedLength(inputSegment, length);
    }
}
