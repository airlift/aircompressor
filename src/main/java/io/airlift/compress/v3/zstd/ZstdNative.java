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

import io.airlift.compress.v3.MalformedInputException;
import io.airlift.compress.v3.internal.NativeLoader;
import io.airlift.compress.v3.internal.NativeSignature;

import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static java.lang.Long.compareUnsigned;
import static java.lang.invoke.MethodHandles.lookup;

final class ZstdNative
{
    private record MethodHandles(
            @NativeSignature(name = "ZSTD_compressBound", returnType = long.class, argumentTypes = long.class)
            MethodHandle maxCompressedLength,
            @NativeSignature(name = "ZSTD_createCCtx", returnType = MemorySegment.class, argumentTypes = {})
            MethodHandle createCompressionContext,
            @NativeSignature(name = "ZSTD_freeCCtx", returnType = long.class, argumentTypes = MemorySegment.class)
            MethodHandle freeCompressionContext,
            @NativeSignature(name = "ZSTD_compressCCtx", returnType = long.class, argumentTypes = {MemorySegment.class, MemorySegment.class, long.class, MemorySegment.class, long.class, int.class})
            MethodHandle compress,
            @NativeSignature(name = "ZSTD_createDCtx", returnType = MemorySegment.class, argumentTypes = {})
            MethodHandle createDecompressionContext,
            @NativeSignature(name = "ZSTD_freeDCtx", returnType = long.class, argumentTypes = MemorySegment.class)
            MethodHandle freeDecompressionContext,
            @NativeSignature(name = "ZSTD_decompressDCtx", returnType = long.class, argumentTypes = {MemorySegment.class, MemorySegment.class, long.class, MemorySegment.class, long.class})
            MethodHandle decompress,
            @NativeSignature(name = "ZSTD_getFrameContentSize", returnType = long.class, argumentTypes = {MemorySegment.class, long.class})
            MethodHandle uncompressedLength,
            @NativeSignature(name = "ZSTD_isError", returnType = int.class, argumentTypes = long.class)
            MethodHandle isError,
            @NativeSignature(name = "ZSTD_getErrorCode", returnType = int.class, argumentTypes = long.class)
            MethodHandle getErrorCode,
            @NativeSignature(name = "ZSTD_getErrorName", returnType = MemorySegment.class, argumentTypes = long.class)
            MethodHandle getErrorName,
            @NativeSignature(name = "ZSTD_defaultCLevel", returnType = int.class, argumentTypes = {})
            MethodHandle defaultCLevel) {}

    private ZstdNative() {}

    private static final Optional<LinkageError> LINKAGE_ERROR;
    private static final MethodHandle MAX_COMPRESSED_LENGTH_METHOD;
    private static final MethodHandle CREATE_COMPRESSION_CONTEXT_METHOD;
    private static final MethodHandle FREE_COMPRESSION_CONTEXT_METHOD;
    private static final MethodHandle COMPRESS_METHOD;
    private static final MethodHandle CREATE_DECOMPRESSION_CONTEXT_METHOD;
    private static final MethodHandle FREE_DECOMPRESSION_CONTEXT_METHOD;
    private static final MethodHandle DECOMPRESS_METHOD;
    private static final MethodHandle UNCOMPRESSED_LENGTH_METHOD;
    private static final MethodHandle IS_ERROR_METHOD;
    private static final MethodHandle GET_ERROR_CODE_METHOD;
    private static final MethodHandle GET_ERROR_NAME_METHOD;

    // TODO should we just hardcode this to 3?
    public static final int DEFAULT_COMPRESSION_LEVEL;

    private static final long CONTENT_SIZE_UNKNOWN = -1L;

    // ZSTD_ErrorCode values from zstd_errors.h that ZSTD_decompress reports for invalid, truncated or unsupported input
    private static final int ERROR_PREFIX_UNKNOWN = 10;
    private static final int ERROR_VERSION_UNSUPPORTED = 12;
    private static final int ERROR_FRAME_PARAMETER_UNSUPPORTED = 14;
    private static final int ERROR_FRAME_PARAMETER_WINDOW_TOO_LARGE = 16;
    private static final int ERROR_CORRUPTION_DETECTED = 20;
    private static final int ERROR_CHECKSUM_WRONG = 22;
    private static final int ERROR_LITERALS_HEADER_WRONG = 24;
    private static final int ERROR_DICTIONARY_CORRUPTED = 30;
    private static final int ERROR_DICTIONARY_WRONG = 32;
    private static final int ERROR_TABLE_LOG_TOO_LARGE = 44;
    private static final int ERROR_MAX_SYMBOL_VALUE_TOO_LARGE = 46;
    private static final int ERROR_MAX_SYMBOL_VALUE_TOO_SMALL = 48;
    private static final int ERROR_DST_SIZE_TOO_SMALL = 70;
    private static final int ERROR_SRC_SIZE_WRONG = 72;

    // ZSTD_isError(code) == unsigned(code) > (size_t)(-ZSTD_error_maxCode); ZSTD_error_maxCode is 120 in zstd 1.5.7.
    private static final long ERROR_THRESHOLD = -120L;

    static {
        NativeLoader.Symbols<MethodHandles> symbols = NativeLoader.loadSymbols("zstd", MethodHandles.class, lookup());
        LINKAGE_ERROR = symbols.linkageError();
        MethodHandles methodHandles = symbols.symbols();
        MAX_COMPRESSED_LENGTH_METHOD = methodHandles.maxCompressedLength();
        CREATE_COMPRESSION_CONTEXT_METHOD = methodHandles.createCompressionContext();
        FREE_COMPRESSION_CONTEXT_METHOD = methodHandles.freeCompressionContext();
        COMPRESS_METHOD = methodHandles.compress();
        CREATE_DECOMPRESSION_CONTEXT_METHOD = methodHandles.createDecompressionContext();
        FREE_DECOMPRESSION_CONTEXT_METHOD = methodHandles.freeDecompressionContext();
        DECOMPRESS_METHOD = methodHandles.decompress();
        UNCOMPRESSED_LENGTH_METHOD = methodHandles.uncompressedLength();
        IS_ERROR_METHOD = methodHandles.isError();
        GET_ERROR_CODE_METHOD = methodHandles.getErrorCode();
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

    public static MemorySegment createCompressionContext()
    {
        MemorySegment context;
        try {
            context = (MemorySegment) CREATE_COMPRESSION_CONTEXT_METHOD.invokeExact();
        }
        catch (Error e) {
            throw e;
        }
        catch (Throwable e) {
            throw new Error("Unexpected exception", e);
        }
        if (context.equals(MemorySegment.NULL)) {
            throw new IllegalStateException("Failed to allocate zstd compression context");
        }
        return context;
    }

    public static void freeCompressionContext(MemorySegment context)
    {
        try {
            long _ = (long) FREE_COMPRESSION_CONTEXT_METHOD.invokeExact(context);
        }
        catch (Error e) {
            throw e;
        }
        catch (Throwable e) {
            throw new Error("Unexpected exception", e);
        }
    }

    public static MemorySegment createDecompressionContext()
    {
        MemorySegment context;
        try {
            context = (MemorySegment) CREATE_DECOMPRESSION_CONTEXT_METHOD.invokeExact();
        }
        catch (Error e) {
            throw e;
        }
        catch (Throwable e) {
            throw new Error("Unexpected exception", e);
        }
        if (context.equals(MemorySegment.NULL)) {
            throw new IllegalStateException("Failed to allocate zstd decompression context");
        }
        return context;
    }

    public static void freeDecompressionContext(MemorySegment context)
    {
        try {
            long _ = (long) FREE_DECOMPRESSION_CONTEXT_METHOD.invokeExact(context);
        }
        catch (Error e) {
            throw e;
        }
        catch (Throwable e) {
            throw new Error("Unexpected exception", e);
        }
    }

    public static long compress(MemorySegment context, MemorySegment input, long inputLength, MemorySegment compressed, long compressedLength, int compressionLevel)
    {
        long result;
        try {
            result = (long) COMPRESS_METHOD.invokeExact(context, compressed, compressedLength, input, inputLength, compressionLevel);
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

    public static long decompress(MemorySegment context, MemorySegment compressed, long compressedLength, MemorySegment output, long outputLength)
    {
        long result;
        try {
            result = (long) DECOMPRESS_METHOD.invokeExact(context, output, outputLength, compressed, compressedLength);
        }
        catch (Error e) {
            throw e;
        }
        catch (Throwable e) {
            throw new Error("Unexpected exception", e);
        }

        if (isError(result)) {
            if (isMalformedInputError(result)) {
                throw new MalformedInputException(0, getErrorName(result));
            }
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

        // any other negative result is ZSTD_CONTENTSIZE_ERROR, returned for an invalid magic or a truncated frame header
        if (CONTENT_SIZE_UNKNOWN != result && result < 0) {
            throw new MalformedInputException(0, "Invalid or truncated frame header");
        }
        return result;
    }

    static boolean isError(long code)
    {
        return compareUnsigned(code, ERROR_THRESHOLD) > 0;
    }

    // Visible for testing
    static boolean nativeIsError(long code)
    {
        try {
            return (int) IS_ERROR_METHOD.invokeExact(code) != 0;
        }
        catch (Throwable e) {
            throw new AssertionError("should not reach here", e);
        }
    }

    // Matches ZstdJavaDecompressor, which reports all of these as MalformedInputException, including an output buffer that is too small
    private static boolean isMalformedInputError(long code)
    {
        int errorCode;
        try {
            errorCode = (int) GET_ERROR_CODE_METHOD.invokeExact(code);
        }
        catch (Throwable e) {
            throw new AssertionError("should not reach here", e);
        }
        return switch (errorCode) {
            // unknown magic or frame parameters the decoder does not support
            case ERROR_PREFIX_UNKNOWN, ERROR_VERSION_UNSUPPORTED, ERROR_FRAME_PARAMETER_UNSUPPORTED, ERROR_FRAME_PARAMETER_WINDOW_TOO_LARGE -> true;
            // corrupt frame content, including entropy table headers and dictionary references
            case ERROR_CORRUPTION_DETECTED, ERROR_CHECKSUM_WRONG, ERROR_LITERALS_HEADER_WRONG, ERROR_DICTIONARY_CORRUPTED, ERROR_DICTIONARY_WRONG -> true;
            case ERROR_TABLE_LOG_TOO_LARGE, ERROR_MAX_SYMBOL_VALUE_TOO_LARGE, ERROR_MAX_SYMBOL_VALUE_TOO_SMALL -> true;
            // truncated input, or content that does not fit in the output buffer
            case ERROR_SRC_SIZE_WRONG, ERROR_DST_SIZE_TOO_SMALL -> true;
            // GENERIC, memory_allocation and parameter errors are not caused by the input
            default -> false;
        };
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
