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

import io.airlift.compress.internal.NativeLoader;
import io.airlift.compress.internal.NativeSignature;

import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static java.lang.invoke.MethodHandles.lookup;

final class ZstdNative
{
    private record MethodHandles(
            @NativeSignature(name = "ZSTD_compressBound", returnType = long.class, argumentTypes = long.class)
            MethodHandle maxCompressedLength,
            @NativeSignature(name = "ZSTD_compress", returnType = long.class, argumentTypes = {MemorySegment.class, long.class, MemorySegment.class, long.class, int.class})
            MethodHandle compress,
            @NativeSignature(name = "ZSTD_decompress", returnType = long.class, argumentTypes = {MemorySegment.class, long.class, MemorySegment.class, long.class})
            MethodHandle decompress,
            @NativeSignature(name = "ZSTD_getFrameContentSize", returnType = long.class, argumentTypes = {MemorySegment.class, long.class})
            MethodHandle uncompressedLength,
            @NativeSignature(name = "ZSTD_isError", returnType = int.class, argumentTypes = long.class)
            MethodHandle isError,
            @NativeSignature(name = "ZSTD_getErrorName", returnType = MemorySegment.class, argumentTypes = long.class)
            MethodHandle getErrorName,
            @NativeSignature(name = "ZSTD_defaultCLevel", returnType = int.class, argumentTypes = {})
            MethodHandle defaultCLevel) {}

    private ZstdNative() {}

    private static final Optional<LinkageError> LINKAGE_ERROR;
    private static final MethodHandle MAX_COMPRESSED_LENGTH_METHOD;
    private static final MethodHandle COMPRESS_METHOD;
    private static final MethodHandle DECOMPRESS_METHOD;
    private static final MethodHandle UNCOMPRESSED_LENGTH_METHOD;
    private static final MethodHandle IS_ERROR_METHOD;
    private static final MethodHandle GET_ERROR_NAME_METHOD;

    // TODO should we just hardcode this to 3?
    public static final int DEFAULT_COMPRESSION_LEVEL;

    static {
        NativeLoader.Symbols<MethodHandles> symbols = NativeLoader.loadSymbols("zstd", MethodHandles.class, lookup());
        LINKAGE_ERROR = symbols.linkageError();
        MethodHandles methodHandles = symbols.symbols();
        MAX_COMPRESSED_LENGTH_METHOD = methodHandles.maxCompressedLength();
        COMPRESS_METHOD = methodHandles.compress();
        DECOMPRESS_METHOD = methodHandles.decompress();
        UNCOMPRESSED_LENGTH_METHOD = methodHandles.uncompressedLength();
        IS_ERROR_METHOD = methodHandles.isError();
        GET_ERROR_NAME_METHOD = methodHandles.getErrorName();
        if (LINKAGE_ERROR.isEmpty()) {
            try {
                DEFAULT_COMPRESSION_LEVEL = (int) methodHandles.defaultCLevel().invokeExact();
            }
            catch (Throwable e) {
                throw new ExceptionInInitializerError(e);
            }
        }
        else {
            DEFAULT_COMPRESSION_LEVEL = -1;
        }
    }

    public static boolean isEnabled()
    {
        return LINKAGE_ERROR.isEmpty();
    }

    public static void verifyEnabled()
    {
        if (LINKAGE_ERROR.isPresent()) {
            throw new IllegalStateException("Zstd native library is not enabled", LINKAGE_ERROR.get());
        }
    }

    public static long maxCompressedLength(long inputLength)
    {
        long result;
        try {
            result = (long) MAX_COMPRESSED_LENGTH_METHOD.invokeExact(inputLength);
        }
        catch (Throwable e) {
            throw new AssertionError("should not reach here", e);
        }
        if (isError(result)) {
            throw new IllegalArgumentException("Unknown error occurred during compression: " + getErrorName(result));
        }
        return result;
    }

    public static long compress(MemorySegment input, long inputLength, MemorySegment compressed, long compressedLength, int compressionLevel)
    {
        long result;
        try {
            result = (long) COMPRESS_METHOD.invokeExact(compressed, compressedLength, input, inputLength, compressionLevel);
        }
        catch (Error e) {
            throw e;
        }
        catch (Throwable e) {
            throw new Error("Unexpected exception", e);
        }

        if (isError(result)) {
            throw new IllegalArgumentException("Unknown error occurred during compression: " + getErrorName(result));
        }
        return result;
    }

    public static long decompress(MemorySegment compressed, long compressedLength, MemorySegment output, long outputLength)
    {
        long result;
        try {
            result = (long) DECOMPRESS_METHOD.invokeExact(output, outputLength, compressed, compressedLength);
        }
        catch (Error e) {
            throw e;
        }
        catch (Throwable e) {
            throw new Error("Unexpected exception", e);
        }

        if (isError(result)) {
            throw new IllegalArgumentException("Unknown error occurred during decompression: " + getErrorName(result));
        }
        return result;
    }

    public static long decompressedLength(MemorySegment compressed, long compressedLength)
    {
        long result;
        try {
            result = (long) UNCOMPRESSED_LENGTH_METHOD.invokeExact(compressed, compressedLength);
        }
        catch (Error e) {
            throw e;
        }
        catch (Throwable e) {
            throw new Error("Unexpected exception", e);
        }

        if (isError(result)) {
            throw new IllegalArgumentException("Unknown error occurred during decompression: " + getErrorName(result));
        }
        return result;
    }

    private static boolean isError(long code)
    {
        try {
            return (int) IS_ERROR_METHOD.invokeExact(code) != 0;
        }
        catch (Error e) {
            throw e;
        }
        catch (Throwable e) {
            throw new Error("Unexpected exception", e);
        }
    }

    private static String getErrorName(long code)
    {
        try {
            MemorySegment name = (MemorySegment) GET_ERROR_NAME_METHOD.invokeExact(code);
            return name.reinterpret(Long.MAX_VALUE).getString(0);
        }
        catch (Error e) {
            throw e;
        }
        catch (Throwable e) {
            throw new Error("Unexpected exception", e);
        }
    }
}
