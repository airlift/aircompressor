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
package io.airlift.compress.v3.zstd;

import io.airlift.compress.v3.internal.NativeLoader;
import io.airlift.compress.v3.internal.NativeSignature;

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
            // Decompression streaming
            @NativeSignature(name = "ZSTD_createDStream", returnType = MemorySegment.class, argumentTypes = {})
            MethodHandle createDecompressStream,
            @NativeSignature(name = "ZSTD_freeDStream", returnType = void.class, argumentTypes = MemorySegment.class)
            MethodHandle freeDecompressStream,
            @NativeSignature(name = "ZSTD_initDStream", returnType = long.class, argumentTypes = MemorySegment.class)
            MethodHandle initDecompressStream,
            @NativeSignature(name = "ZSTD_decompressStream_simpleArgs", returnType = long.class, argumentTypes = {MemorySegment.class, MemorySegment.class, long.class, MemorySegment.class, MemorySegment.class, long.class, MemorySegment.class})
            MethodHandle decompressStreamSimpleArgs,
            @NativeSignature(name = "ZSTD_DStreamInSize", returnType = long.class, argumentTypes = {})
            MethodHandle decompressStreamInputSize,
            @NativeSignature(name = "ZSTD_DStreamOutSize", returnType = long.class, argumentTypes = {})
            MethodHandle decompressStreamOutputSize,
            // Compression streaming
            @NativeSignature(name = "ZSTD_createCStream", returnType = MemorySegment.class, argumentTypes = {})
            MethodHandle createCompressStream,
            @NativeSignature(name = "ZSTD_freeCStream", returnType = void.class, argumentTypes = MemorySegment.class)
            MethodHandle freeCompressStream,
            @NativeSignature(name = "ZSTD_initCStream", returnType = long.class, argumentTypes = {MemorySegment.class, int.class})
            MethodHandle initCompressStream,
            @NativeSignature(name = "ZSTD_compressStream2_simpleArgs", returnType = long.class, argumentTypes = {MemorySegment.class, MemorySegment.class, long.class, MemorySegment.class, MemorySegment.class, long.class, MemorySegment.class, int.class})
            MethodHandle compressStream2SimpleArgs,
            @NativeSignature(name = "ZSTD_CStreamInSize", returnType = long.class, argumentTypes = {})
            MethodHandle compressStreamInputSize,
            @NativeSignature(name = "ZSTD_CStreamOutSize", returnType = long.class, argumentTypes = {})
            MethodHandle compressStreamOutputSize,
            // Error handling and utilities
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
    // Decompression streaming
    private static final MethodHandle CREATE_DECOMPRESS_STREAM_METHOD;
    private static final MethodHandle FREE_DECOMPRESS_STREAM_METHOD;
    private static final MethodHandle INIT_DECOMPRESS_STREAM_METHOD;
    private static final MethodHandle DECOMPRESS_STREAM_SIMPLE_ARGS_METHOD;
    // Compression streaming
    private static final MethodHandle CREATE_COMPRESS_STREAM_METHOD;
    private static final MethodHandle FREE_COMPRESS_STREAM_METHOD;
    private static final MethodHandle INIT_COMPRESS_STREAM_METHOD;
    private static final MethodHandle COMPRESS_STREAM_2_SIMPLE_ARGS_METHOD;
    // Error handling
    private static final MethodHandle IS_ERROR_METHOD;
    private static final MethodHandle GET_ERROR_NAME_METHOD;

    // TODO should we just hardcode this to 3?
    public static final int DEFAULT_COMPRESSION_LEVEL;

    // Streaming buffer sizes
    public static final int DECOMPRESS_STREAM_INPUT_SIZE;
    public static final int COMPRESS_STREAM_OUTPUT_SIZE;

    private static final long CONTENT_SIZE_UNKNOWN = -1L;

    // ZSTD_EndDirective values for compressStream2
    public static final int ZSTD_E_CONTINUE = 0;
    public static final int ZSTD_E_FLUSH = 1;
    public static final int ZSTD_E_END = 2;

    static {
        NativeLoader.Symbols<MethodHandles> symbols = NativeLoader.loadSymbols("zstd", MethodHandles.class, lookup());
        LINKAGE_ERROR = symbols.linkageError();
        MethodHandles methodHandles = symbols.symbols();
        MAX_COMPRESSED_LENGTH_METHOD = methodHandles.maxCompressedLength();
        COMPRESS_METHOD = methodHandles.compress();
        DECOMPRESS_METHOD = methodHandles.decompress();
        UNCOMPRESSED_LENGTH_METHOD = methodHandles.uncompressedLength();
        // Decompression streaming
        CREATE_DECOMPRESS_STREAM_METHOD = methodHandles.createDecompressStream();
        FREE_DECOMPRESS_STREAM_METHOD = methodHandles.freeDecompressStream();
        INIT_DECOMPRESS_STREAM_METHOD = methodHandles.initDecompressStream();
        DECOMPRESS_STREAM_SIMPLE_ARGS_METHOD = methodHandles.decompressStreamSimpleArgs();
        // Compression streaming
        CREATE_COMPRESS_STREAM_METHOD = methodHandles.createCompressStream();
        FREE_COMPRESS_STREAM_METHOD = methodHandles.freeCompressStream();
        INIT_COMPRESS_STREAM_METHOD = methodHandles.initCompressStream();
        COMPRESS_STREAM_2_SIMPLE_ARGS_METHOD = methodHandles.compressStream2SimpleArgs();
        // Error handling
        IS_ERROR_METHOD = methodHandles.isError();
        GET_ERROR_NAME_METHOD = methodHandles.getErrorName();
        if (LINKAGE_ERROR.isEmpty()) {
            try {
                DEFAULT_COMPRESSION_LEVEL = (int) methodHandles.defaultCLevel().invokeExact();
                DECOMPRESS_STREAM_INPUT_SIZE = (int) (long) methodHandles.decompressStreamInputSize().invokeExact();
                COMPRESS_STREAM_OUTPUT_SIZE = (int) (long) methodHandles.compressStreamOutputSize().invokeExact();
            }
            catch (Throwable e) {
                throw new ExceptionInInitializerError(e);
            }
        }
        else {
            DEFAULT_COMPRESSION_LEVEL = -1;
            DECOMPRESS_STREAM_INPUT_SIZE = -1;
            COMPRESS_STREAM_OUTPUT_SIZE = -1;
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

        if (CONTENT_SIZE_UNKNOWN != result && result < 0) {
            throw new IllegalArgumentException("Unknown error occurred during decompression: " + getErrorName(result));
        }
        return result;
    }

    public static MemorySegment createDecompressStream()
    {
        try {
            return (MemorySegment) CREATE_DECOMPRESS_STREAM_METHOD.invokeExact();
        }
        catch (Error e) {
            throw e;
        }
        catch (Throwable e) {
            throw new Error("Unexpected exception", e);
        }
    }

    public static void freeDecompressStream(MemorySegment stream)
    {
        try {
            FREE_DECOMPRESS_STREAM_METHOD.invokeExact(stream);
        }
        catch (Error e) {
            throw e;
        }
        catch (Throwable e) {
            throw new Error("Unexpected exception", e);
        }
    }

    public static long initDecompressStream(MemorySegment stream)
    {
        long result;
        try {
            result = (long) INIT_DECOMPRESS_STREAM_METHOD.invokeExact(stream);
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

    public static long decompressStreamSimpleArgs(
            MemorySegment stream,
            MemorySegment dst,
            long dstCapacity,
            MemorySegment dstPos,
            MemorySegment src,
            long srcSize,
            MemorySegment srcPos)
    {
        long result;
        try {
            result = (long) DECOMPRESS_STREAM_SIMPLE_ARGS_METHOD.invokeExact(stream, dst, dstCapacity, dstPos, src, srcSize, srcPos);
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

    public static MemorySegment createCompressStream()
    {
        try {
            return (MemorySegment) CREATE_COMPRESS_STREAM_METHOD.invokeExact();
        }
        catch (Error e) {
            throw e;
        }
        catch (Throwable e) {
            throw new Error("Unexpected exception", e);
        }
    }

    public static void freeCompressStream(MemorySegment stream)
    {
        try {
            FREE_COMPRESS_STREAM_METHOD.invokeExact(stream);
        }
        catch (Error e) {
            throw e;
        }
        catch (Throwable e) {
            throw new Error("Unexpected exception", e);
        }
    }

    public static long initCompressStream(MemorySegment stream, int compressionLevel)
    {
        long result;
        try {
            result = (long) INIT_COMPRESS_STREAM_METHOD.invokeExact(stream, compressionLevel);
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

    public static long compressStream2SimpleArgs(
            MemorySegment stream,
            MemorySegment dst,
            long dstCapacity,
            MemorySegment dstPos,
            MemorySegment src,
            long srcSize,
            MemorySegment srcPos,
            int endOp)
    {
        long result;
        try {
            result = (long) COMPRESS_STREAM_2_SIMPLE_ARGS_METHOD.invokeExact(stream, dst, dstCapacity, dstPos, src, srcSize, srcPos, endOp);
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
