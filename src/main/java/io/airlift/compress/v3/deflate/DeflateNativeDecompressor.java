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

import io.airlift.compress.v3.MalformedInputException;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.ref.Cleaner;

import static io.airlift.compress.v3.deflate.DeflateNative.LIBDEFLATE_BAD_DATA;
import static io.airlift.compress.v3.deflate.DeflateNative.LIBDEFLATE_INSUFFICIENT_SPACE;
import static io.airlift.compress.v3.deflate.DeflateNative.LIBDEFLATE_SHORT_OUTPUT;
import static io.airlift.compress.v3.deflate.DeflateNative.LIBDEFLATE_SUCCESS;
import static java.lang.Math.toIntExact;

/**
 * A single decompressor is not safe to use by multiple threads concurrently.
 * However, different threads may use different decompressors concurrently.
 */
public class DeflateNativeDecompressor
        implements DeflateDecompressor
{
    private static final Cleaner CLEANER = Cleaner.create();

    private final MemorySegment decompressor;

    public DeflateNativeDecompressor()
    {
        DeflateNative.verifyEnabled();
        this.decompressor = DeflateNative.allocDecompressor();
        CLEANER.register(this, new DecompressorCleaner(decompressor));
    }

    public static boolean isEnabled()
    {
        return DeflateNative.isEnabled();
    }

    @Override
    public int decompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
            throws MalformedInputException
    {
        MemorySegment inputSegment = MemorySegment.ofArray(input).asSlice(inputOffset, inputLength);
        MemorySegment outputSegment = MemorySegment.ofArray(output).asSlice(outputOffset, maxOutputLength);
        return decompress(inputSegment, inputLength, outputSegment, maxOutputLength);
    }

    @Override
    public int decompress(MemorySegment input, MemorySegment output)
            throws MalformedInputException
    {
        return decompress(input, input.byteSize(), output, output.byteSize());
    }

    private int decompress(MemorySegment input, long inputLength, MemorySegment output, long outputLength)
            throws MalformedInputException
    {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment actualOutputLength = arena.allocate(ValueLayout.JAVA_LONG);
            int result = DeflateNative.decompress(decompressor, input, inputLength, output, outputLength, actualOutputLength);
            return switch (result) {
                case LIBDEFLATE_SUCCESS -> toIntExact(actualOutputLength.get(ValueLayout.JAVA_LONG, 0));
                case LIBDEFLATE_BAD_DATA -> throw new MalformedInputException(0, "Invalid or corrupt deflate compressed data");
                case LIBDEFLATE_INSUFFICIENT_SPACE, LIBDEFLATE_SHORT_OUTPUT -> throw new MalformedInputException(0, "Output buffer too small for decompressed data");
                default -> throw new MalformedInputException(0, "Unknown decompression error: " + result);
            };
        }
    }

    private record DecompressorCleaner(MemorySegment decompressor)
            implements Runnable
    {
        @Override
        public void run()
        {
            DeflateNative.freeDecompressor(decompressor);
        }
    }
}
