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
package io.airlift.compress.snappy;

import io.airlift.compress.MalformedInputException;
import io.airlift.compress.internal.NativeLoader.Symbols;
import io.airlift.compress.internal.NativeSignature;

import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static io.airlift.compress.internal.NativeLoader.loadSymbols;
import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static java.lang.invoke.MethodHandles.lookup;

final class SnappyNative
{
    private final MemorySegment lengthBuffer = MemorySegment.ofArray(new long[1]);

    public static long maxCompressedLength(long inputLength)
    {
        return maxCompressedLengthInternal(inputLength);
    }

    public long compress(MemorySegment input, long inputLength, MemorySegment compressed, long compressedLength)
    {
        lengthBuffer.set(JAVA_LONG, 0, compressedLength);
        compressInternal(input, inputLength, compressed, lengthBuffer);
        return lengthBuffer.get(JAVA_LONG, 0);
    }

    public long decompress(MemorySegment compressed, long compressedLength, MemorySegment uncompressed, long uncompressedLength)
    {
        lengthBuffer.set(JAVA_LONG, 0, uncompressedLength);
        decompressInternal(compressed, compressedLength, uncompressed, lengthBuffer);
        return lengthBuffer.get(JAVA_LONG, 0);
    }

    public long decompressedLength(MemorySegment compressed, long compressedLength)
    {
        lengthBuffer.set(JAVA_LONG, 0, 0);
        decompressedLengthInternal(compressed, compressedLength, lengthBuffer);
        return lengthBuffer.get(JAVA_LONG, 0);
    }

    //
    // FFI stuff
    //

    // Defined in snappy-c.h: https://github.com/google/snappy/blob/1.2.1/snappy-c.h#L47
    private static final int SNAPPY_OK = 0;
    private static final int SNAPPY_INVALID_INPUT = 1;
    private static final int SNAPPY_BUFFER_TOO_SMALL = 2;

    private record MethodHandles(
            @NativeSignature(name = "snappy_compress", returnType = int.class, argumentTypes = {MemorySegment.class, long.class, MemorySegment.class, MemorySegment.class})
            MethodHandle compress,
            @NativeSignature(name = "snappy_uncompress", returnType = int.class, argumentTypes = {MemorySegment.class, long.class, MemorySegment.class, MemorySegment.class})
            MethodHandle uncompress,
            @NativeSignature(name = "snappy_max_compressed_length", returnType = long.class, argumentTypes = long.class)
            MethodHandle maxCompressedLength,
            @NativeSignature(name = "snappy_uncompressed_length", returnType = int.class, argumentTypes = {MemorySegment.class, long.class, MemorySegment.class})
            MethodHandle uncompressedLength) {}

    private static final Optional<LinkageError> LINKAGE_ERROR;
    private static final MethodHandle COMPRESS_METHOD;
    private static final MethodHandle DECOMPRESS_METHOD;
    private static final MethodHandle MAX_COMPRESSED_LENGTH_METHOD;
    private static final MethodHandle UNCOMPRESSED_LENGTH_METHOD;

    static {
        Symbols<MethodHandles> symbols = loadSymbols("snappy", MethodHandles.class, lookup());
        LINKAGE_ERROR = symbols.linkageError();
        MethodHandles methodHandles = symbols.symbols();
        COMPRESS_METHOD = methodHandles.compress();
        DECOMPRESS_METHOD = methodHandles.uncompress();
        MAX_COMPRESSED_LENGTH_METHOD = methodHandles.maxCompressedLength();
        UNCOMPRESSED_LENGTH_METHOD = methodHandles.uncompressedLength();
    }

    public static boolean isEnabled()
    {
        return LINKAGE_ERROR.isEmpty();
    }

    public static void verifyEnabled()
    {
        if (LINKAGE_ERROR.isPresent()) {
            throw new IllegalStateException("Snappy native library is not enabled", LINKAGE_ERROR.get());
        }
    }

    private static void compressInternal(MemorySegment input, long inputLength, MemorySegment compressed, MemorySegment compressedLength)
    {
        int result;
        try {
            result = (int) COMPRESS_METHOD.invokeExact(input, inputLength, compressed, compressedLength);
        }
        catch (Throwable t) {
            throw new AssertionError("should not reach here", t);
        }

        switch (result) {
            case SNAPPY_OK -> {}
            case SNAPPY_BUFFER_TOO_SMALL -> throw new IllegalArgumentException("Output buffer too small");
            default -> throw new IllegalArgumentException("Unknown error occurred during compression: result=" + result);
        }
    }

    private static void decompressInternal(MemorySegment compressed, long compressedLength, MemorySegment output, MemorySegment outputLength)
    {
        int result;
        try {
            result = (int) DECOMPRESS_METHOD.invokeExact(compressed, compressedLength, output, outputLength);
        }
        catch (Throwable t) {
            throw new AssertionError("should not reach here", t);
        }

        switch (result) {
            case SNAPPY_OK -> {}
            case SNAPPY_INVALID_INPUT -> throw new MalformedInputException(0, "Invalid input");
            case SNAPPY_BUFFER_TOO_SMALL -> throw new IllegalArgumentException("Output buffer too small");
            default -> throw new IllegalArgumentException("Unknown error occurred during decompression: result=" + result);
        }
    }

    private static long maxCompressedLengthInternal(long inputLength)
    {
        try {
            return (long) MAX_COMPRESSED_LENGTH_METHOD.invokeExact(inputLength);
        }
        catch (Throwable t) {
            throw new AssertionError("should not reach here", t);
        }
    }

    private static void decompressedLengthInternal(MemorySegment compressed, long compressedLength, MemorySegment decompressedLength)
    {
        int result;
        try {
            result = (int) UNCOMPRESSED_LENGTH_METHOD.invokeExact(compressed, compressedLength, decompressedLength);
        }
        catch (Throwable t) {
            throw new AssertionError("should not reach here", t);
        }

        switch (result) {
            case SNAPPY_OK -> {}
            case SNAPPY_INVALID_INPUT -> throw new MalformedInputException(0, "Invalid input");
            default -> throw new IllegalArgumentException("Unknown error occurred during decompressed length calculation: result=" + result);
        }
    }
}
