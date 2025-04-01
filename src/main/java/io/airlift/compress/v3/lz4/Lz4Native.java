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

import io.airlift.compress.v3.internal.NativeLoader.Symbols;
import io.airlift.compress.v3.internal.NativeSignature;

import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static io.airlift.compress.v3.internal.NativeLoader.loadSymbols;
import static java.lang.invoke.MethodHandles.lookup;

final class Lz4Native
{
    private Lz4Native() {}

    private record MethodHandles(
            @NativeSignature(name = "LZ4_compressBound", returnType = int.class, argumentTypes = int.class)
            MethodHandle maxCompressedLength,
            @NativeSignature(name = "LZ4_compress_fast", returnType = int.class, argumentTypes = {MemorySegment.class, MemorySegment.class, int.class, int.class, int.class})
            MethodHandle compress,
            @NativeSignature(name = "LZ4_compress_fast_extState", returnType = int.class, argumentTypes = {MemorySegment.class, MemorySegment.class, MemorySegment.class, int.class, int.class, int.class})
            MethodHandle compressExternalState,
            @NativeSignature(name = "LZ4_decompress_safe", returnType = int.class, argumentTypes = {MemorySegment.class, MemorySegment.class, int.class, int.class})
            MethodHandle decompress,
            @NativeSignature(name = "LZ4_sizeofState", returnType = int.class, argumentTypes = {})
            MethodHandle sizeofState) {}

    private static final Optional<LinkageError> LINKAGE_ERROR;
    private static final MethodHandle MAX_COMPRESSED_LENGTH_METHOD;
    private static final MethodHandle COMPRESS_METHOD;
    private static final MethodHandle COMPRESS_EXTERNAL_STATE_METHOD;
    private static final MethodHandle DECOMPRESS_METHOD;

    // Defined in lz4.h: https://github.com/lz4/lz4/blob/v1.9.4/lib/lz4.c#L51
    public static final int DEFAULT_ACCELERATION = 1;
    public static final int MAX_ACCELERATION = 65537;
    public static final int STATE_SIZE;

    static {
        Symbols<MethodHandles> symbols = loadSymbols("lz4", MethodHandles.class, lookup());
        LINKAGE_ERROR = symbols.linkageError();
        MethodHandles methodHandles = symbols.symbols();
        MAX_COMPRESSED_LENGTH_METHOD = methodHandles.maxCompressedLength();
        COMPRESS_METHOD = methodHandles.compress();
        COMPRESS_EXTERNAL_STATE_METHOD = methodHandles.compressExternalState();
        DECOMPRESS_METHOD = methodHandles.decompress();

        if (LINKAGE_ERROR.isEmpty()) {
            try {
                STATE_SIZE = (int) methodHandles.sizeofState().invokeExact();
            }
            catch (Throwable e) {
                throw new ExceptionInInitializerError(e);
            }
        }
        else {
            STATE_SIZE = -1;
        }
    }

    public static boolean isEnabled()
    {
        return LINKAGE_ERROR.isEmpty();
    }

    public static void verifyEnabled()
    {
        if (LINKAGE_ERROR.isPresent()) {
            throw new IllegalStateException("Lz4 native library is not enabled", LINKAGE_ERROR.get());
        }
    }

    public static int maxCompressedLength(int inputLength)
    {
        try {
            return (int) MAX_COMPRESSED_LENGTH_METHOD.invokeExact(inputLength);
        }
        catch (Throwable e) {
            throw new AssertionError("should not reach here", e);
        }
    }

    public static int compress(MemorySegment input, int inputLength, MemorySegment compressed, int compressedLength, int acceleration)
    {
        int result;
        try {
            result = (int) COMPRESS_METHOD.invokeExact(input, compressed, inputLength, compressedLength, acceleration);
        }
        catch (Throwable e) {
            throw new AssertionError("should not reach here", e);
        }

        // LZ4_compress_default returns 0 on error, but disallow negative values also
        if (result <= 0) {
            throw new IllegalArgumentException("Unknown error occurred during compression: result=" + result);
        }
        return result;
    }

    public static int compress(MemorySegment input, int inputLength, MemorySegment compressed, int compressedLength, int acceleration, MemorySegment state)
    {
        int result;
        try {
            result = (int) COMPRESS_EXTERNAL_STATE_METHOD.invokeExact(state, input, compressed, inputLength, compressedLength, acceleration);
        }
        catch (Throwable e) {
            throw new AssertionError("should not reach here", e);
        }

        // LZ4_compress_default returns 0 on error, but disallow negative values also
        if (result <= 0) {
            throw new IllegalArgumentException("Unknown error occurred during compression: result=" + result);
        }
        return result;
    }

    public static int decompress(MemorySegment compressed, int compressedLength, MemorySegment output, int outputLength)
    {
        int result;
        try {
            result = (int) DECOMPRESS_METHOD.invokeExact(compressed, output, compressedLength, outputLength);
        }
        catch (Throwable e) {
            throw new AssertionError("should not reach here", e);
        }

        // negative return values indicate errors
        if (result < 0) {
            throw new IllegalArgumentException("Unknown error occurred during decompression: result=" + result);
        }
        return result;
    }
}
