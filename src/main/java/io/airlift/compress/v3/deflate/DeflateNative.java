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

import io.airlift.compress.v3.internal.NativeLoader;
import io.airlift.compress.v3.internal.NativeSignature;

import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static java.lang.invoke.MethodHandles.lookup;

final class DeflateNative
{
    private record MethodHandles(
            @NativeSignature(name = "libdeflate_alloc_compressor", returnType = MemorySegment.class, argumentTypes = int.class)
            MethodHandle allocCompressor,
            @NativeSignature(name = "libdeflate_free_compressor", returnType = void.class, argumentTypes = MemorySegment.class)
            MethodHandle freeCompressor,
            @NativeSignature(name = "libdeflate_deflate_compress", returnType = long.class, argumentTypes = {MemorySegment.class, MemorySegment.class, long.class, MemorySegment.class, long.class})
            MethodHandle compress,
            @NativeSignature(name = "libdeflate_deflate_compress_bound", returnType = long.class, argumentTypes = {MemorySegment.class, long.class})
            MethodHandle compressBound,
            @NativeSignature(name = "libdeflate_alloc_decompressor", returnType = MemorySegment.class, argumentTypes = {})
            MethodHandle allocDecompressor,
            @NativeSignature(name = "libdeflate_free_decompressor", returnType = void.class, argumentTypes = MemorySegment.class)
            MethodHandle freeDecompressor,
            @NativeSignature(name = "libdeflate_deflate_decompress", returnType = int.class, argumentTypes = {MemorySegment.class, MemorySegment.class, long.class, MemorySegment.class, long.class, MemorySegment.class})
            MethodHandle decompress) {}

    private DeflateNative() {}

    private static final Optional<LinkageError> LINKAGE_ERROR;
    private static final MethodHandle ALLOC_COMPRESSOR_METHOD;
    private static final MethodHandle FREE_COMPRESSOR_METHOD;
    private static final MethodHandle COMPRESS_METHOD;
    private static final MethodHandle COMPRESS_BOUND_METHOD;
    private static final MethodHandle ALLOC_DECOMPRESSOR_METHOD;
    private static final MethodHandle FREE_DECOMPRESSOR_METHOD;
    private static final MethodHandle DECOMPRESS_METHOD;

    static final int LIBDEFLATE_SUCCESS = 0;
    static final int LIBDEFLATE_BAD_DATA = 1;
    static final int LIBDEFLATE_SHORT_OUTPUT = 2;
    static final int LIBDEFLATE_INSUFFICIENT_SPACE = 3;

    static {
        NativeLoader.Symbols<MethodHandles> symbols = NativeLoader.loadSymbols("deflate", MethodHandles.class, lookup());
        LINKAGE_ERROR = symbols.linkageError();
        MethodHandles methodHandles = symbols.symbols();
        ALLOC_COMPRESSOR_METHOD = methodHandles.allocCompressor();
        FREE_COMPRESSOR_METHOD = methodHandles.freeCompressor();
        COMPRESS_METHOD = methodHandles.compress();
        COMPRESS_BOUND_METHOD = methodHandles.compressBound();
        ALLOC_DECOMPRESSOR_METHOD = methodHandles.allocDecompressor();
        FREE_DECOMPRESSOR_METHOD = methodHandles.freeDecompressor();
        DECOMPRESS_METHOD = methodHandles.decompress();
    }

    public static boolean isEnabled()
    {
        return LINKAGE_ERROR.isEmpty();
    }

    public static void verifyEnabled()
    {
        if (LINKAGE_ERROR.isPresent()) {
            throw new IllegalStateException("Deflate native library is not enabled", LINKAGE_ERROR.get());
        }
    }

    public static MemorySegment allocCompressor(int compressionLevel)
    {
        try {
            MemorySegment compressor = (MemorySegment) ALLOC_COMPRESSOR_METHOD.invokeExact(compressionLevel);
            if (compressor.equals(MemorySegment.NULL)) {
                throw new IllegalStateException("Failed to allocate libdeflate compressor");
            }
            return compressor;
        }
        catch (Error e) {
            throw e;
        }
        catch (Throwable e) {
            throw new Error("Unexpected exception", e);
        }
    }

    public static void freeCompressor(MemorySegment compressor)
    {
        try {
            FREE_COMPRESSOR_METHOD.invokeExact(compressor);
        }
        catch (Error e) {
            throw e;
        }
        catch (Throwable e) {
            throw new Error("Unexpected exception", e);
        }
    }

    public static long compressBound(MemorySegment compressor, long inputLength)
    {
        try {
            return (long) COMPRESS_BOUND_METHOD.invokeExact(compressor, inputLength);
        }
        catch (Error e) {
            throw e;
        }
        catch (Throwable e) {
            throw new Error("Unexpected exception", e);
        }
    }

    public static long compress(MemorySegment compressor, MemorySegment input, long inputLength, MemorySegment output, long outputLength)
    {
        long result;
        try {
            result = (long) COMPRESS_METHOD.invokeExact(compressor, input, inputLength, output, outputLength);
        }
        catch (Error e) {
            throw e;
        }
        catch (Throwable e) {
            throw new Error("Unexpected exception", e);
        }

        if (result == 0) {
            throw new IllegalArgumentException("Output buffer too small");
        }
        return result;
    }

    public static MemorySegment allocDecompressor()
    {
        try {
            MemorySegment decompressor = (MemorySegment) ALLOC_DECOMPRESSOR_METHOD.invokeExact();
            if (decompressor.equals(MemorySegment.NULL)) {
                throw new IllegalStateException("Failed to allocate libdeflate decompressor");
            }
            return decompressor;
        }
        catch (Error e) {
            throw e;
        }
        catch (Throwable e) {
            throw new Error("Unexpected exception", e);
        }
    }

    public static void freeDecompressor(MemorySegment decompressor)
    {
        try {
            FREE_DECOMPRESSOR_METHOD.invokeExact(decompressor);
        }
        catch (Error e) {
            throw e;
        }
        catch (Throwable e) {
            throw new Error("Unexpected exception", e);
        }
    }

    public static int decompress(MemorySegment decompressor, MemorySegment input, long inputLength, MemorySegment output, long outputLength, MemorySegment actualOutputLength)
    {
        try {
            return (int) DECOMPRESS_METHOD.invokeExact(decompressor, input, inputLength, output, outputLength, actualOutputLength);
        }
        catch (Error e) {
            throw e;
        }
        catch (Throwable e) {
            throw new Error("Unexpected exception", e);
        }
    }
}
