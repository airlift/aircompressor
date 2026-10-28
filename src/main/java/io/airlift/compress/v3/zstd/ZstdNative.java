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
import io.airlift.compress.v3.internal.LiteralDecompressor;
import io.airlift.compress.v3.internal.NativeBuffers;
import io.airlift.compress.v3.internal.NativeLoader;
import io.airlift.compress.v3.internal.NativeSignature;

import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static java.lang.Long.compareUnsigned;
import static java.lang.Math.toIntExact;
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
            @NativeSignature(critical = true, name = "ZSTD_getFrameContentSize", returnType = long.class, argumentTypes = {MemorySegment.class, long.class})
            MethodHandle uncompressedLength,
            @NativeSignature(name = "ZSTD_findFrameCompressedSize", returnType = long.class, argumentTypes = {MemorySegment.class, long.class})
            MethodHandle frameCompressedSize,
            // Decompression streaming
            @NativeSignature(name = "ZSTD_createDStream", returnType = MemorySegment.class, argumentTypes = {})
            MethodHandle createDecompressStream,
            @NativeSignature(name = "ZSTD_freeDStream", returnType = long.class, argumentTypes = MemorySegment.class)
            MethodHandle freeDecompressStream,
            @NativeSignature(name = "ZSTD_initDStream", returnType = long.class, argumentTypes = MemorySegment.class)
            MethodHandle initDecompressStream,
            @NativeSignature(name = "ZSTD_decompressStream_simpleArgs", returnType = long.class, argumentTypes = {MemorySegment.class, MemorySegment.class, long.class, MemorySegment.class, MemorySegment.class, long.class, MemorySegment.class})
            MethodHandle decompressStreamSimpleArgs,
            @NativeSignature(name = "ZSTD_DStreamInSize", returnType = long.class, argumentTypes = {})
            MethodHandle decompressStreamInputSize,
            // Compression streaming
            @NativeSignature(name = "ZSTD_createCStream", returnType = MemorySegment.class, argumentTypes = {})
            MethodHandle createCompressStream,
            @NativeSignature(name = "ZSTD_freeCStream", returnType = long.class, argumentTypes = MemorySegment.class)
            MethodHandle freeCompressStream,
            @NativeSignature(name = "ZSTD_initCStream", returnType = long.class, argumentTypes = {MemorySegment.class, int.class})
            MethodHandle initCompressStream,
            @NativeSignature(name = "ZSTD_CCtx_setParameter", returnType = long.class, argumentTypes = {MemorySegment.class, int.class, int.class})
            MethodHandle setCompressStreamParameter,
            @NativeSignature(name = "ZSTD_compressStream2_simpleArgs", returnType = long.class, argumentTypes = {MemorySegment.class, MemorySegment.class, long.class, MemorySegment.class, MemorySegment.class, long.class, MemorySegment.class, int.class})
            MethodHandle compressStream2SimpleArgs,
            @NativeSignature(name = "ZSTD_CStreamOutSize", returnType = long.class, argumentTypes = {})
            MethodHandle compressStreamOutputSize,
            // Error handling and utilities
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
    private static final MethodHandle FRAME_COMPRESSED_SIZE_METHOD;
    // Decompression streaming
    private static final MethodHandle CREATE_DECOMPRESS_STREAM_METHOD;
    private static final MethodHandle FREE_DECOMPRESS_STREAM_METHOD;
    private static final MethodHandle INIT_DECOMPRESS_STREAM_METHOD;
    private static final MethodHandle DECOMPRESS_STREAM_SIMPLE_ARGS_METHOD;
    // Compression streaming
    private static final MethodHandle CREATE_COMPRESS_STREAM_METHOD;
    private static final MethodHandle FREE_COMPRESS_STREAM_METHOD;
    private static final MethodHandle INIT_COMPRESS_STREAM_METHOD;
    private static final MethodHandle SET_COMPRESS_STREAM_PARAMETER_METHOD;
    private static final MethodHandle COMPRESS_STREAM_2_SIMPLE_ARGS_METHOD;
    // Error handling
    private static final MethodHandle IS_ERROR_METHOD;
    private static final MethodHandle GET_ERROR_CODE_METHOD;
    private static final MethodHandle GET_ERROR_NAME_METHOD;

    // TODO should we just hardcode this to 3?
    public static final int DEFAULT_COMPRESSION_LEVEL;

    // Streaming buffer sizes
    public static final int DECOMPRESS_STREAM_INPUT_SIZE;
    public static final int COMPRESS_STREAM_OUTPUT_SIZE;

    private static final long CONTENT_SIZE_UNKNOWN = -1L;

    // ZSTD_ErrorCode values from zstd_errors.h that ZSTD_decompress and ZSTD_decompressStream report for invalid, truncated or unsupported input
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

    // ZSTD_EndDirective values for compressStream2
    public static final int ZSTD_E_CONTINUE = 0;
    public static final int ZSTD_E_FLUSH = 1;
    public static final int ZSTD_E_END = 2;

    // ZSTD_cParameter values for setCompressStreamParameter
    public static final int ZSTD_C_CHECKSUM_FLAG = 201;

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
        FRAME_COMPRESSED_SIZE_METHOD = methodHandles.frameCompressedSize();
        // Decompression streaming
        CREATE_DECOMPRESS_STREAM_METHOD = methodHandles.createDecompressStream();
        FREE_DECOMPRESS_STREAM_METHOD = methodHandles.freeDecompressStream();
        INIT_DECOMPRESS_STREAM_METHOD = methodHandles.initDecompressStream();
        DECOMPRESS_STREAM_SIMPLE_ARGS_METHOD = methodHandles.decompressStreamSimpleArgs();
        // Compression streaming
        CREATE_COMPRESS_STREAM_METHOD = methodHandles.createCompressStream();
        FREE_COMPRESS_STREAM_METHOD = methodHandles.freeCompressStream();
        INIT_COMPRESS_STREAM_METHOD = methodHandles.initCompressStream();
        SET_COMPRESS_STREAM_PARAMETER_METHOD = methodHandles.setCompressStreamParameter();
        COMPRESS_STREAM_2_SIMPLE_ARGS_METHOD = methodHandles.compressStream2SimpleArgs();
        // Error handling
        IS_ERROR_METHOD = methodHandles.isError();
        GET_ERROR_CODE_METHOD = methodHandles.getErrorCode();
        GET_ERROR_NAME_METHOD = methodHandles.getErrorName();
        if (LINKAGE_ERROR.isEmpty()) {
            try {
                DEFAULT_COMPRESSION_LEVEL = (int) methodHandles.defaultCLevel().invokeExact();
                DECOMPRESS_STREAM_INPUT_SIZE = toIntExact((long) methodHandles.decompressStreamInputSize().invokeExact());
                COMPRESS_STREAM_OUTPUT_SIZE = toIntExact((long) methodHandles.compressStreamOutputSize().invokeExact());
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
        try (NativeBuffers buffers = new NativeBuffers()) {
            MemorySegment nativeInput = buffers.input(input, inputLength);
            MemorySegment nativeOutput = buffers.output(compressed, Math.min(compressedLength, maxCompressedLength(inputLength)));
            long result;
            try {
                result = (long) COMPRESS_METHOD.invokeExact(context, nativeOutput, nativeOutput.byteSize(), nativeInput, inputLength, compressionLevel);
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
            NativeBuffers.copyOutput(nativeOutput, compressed, result);
            return result;
        }
    }

    public static long decompress(MemorySegment context, MemorySegment compressed, long compressedLength, MemorySegment output, long outputLength)
    {
        if (compressedLength > 64 * 1024 && (!compressed.isNative() || !output.isNative())) {
            long rawLength = LiteralDecompressor.tryZstdRaw(compressed, compressedLength, output, outputLength);
            if (rawLength >= 0) {
                return rawLength;
            }
        }
        try (NativeBuffers buffers = new NativeBuffers()) {
            MemorySegment nativeInput = buffers.input(compressed, compressedLength);
            long outputBound = output.isNative() ? outputLength : decompressedOutputBound(nativeInput, compressedLength, outputLength);
            MemorySegment nativeOutput = buffers.output(output, outputBound);
            long result;
            try {
                result = (long) DECOMPRESS_METHOD.invokeExact(context, nativeOutput, outputBound, nativeInput, compressedLength);
            }
            catch (Error e) {
                throw e;
            }
            catch (Throwable e) {
                throw new Error("Unexpected exception", e);
            }

            if (isError(result)) {
                throw decompressionError("Unknown error occurred during decompression", result);
            }
            NativeBuffers.copyOutput(nativeOutput, output, result);
            return result;
        }
    }

    // Input must be native: finding a frame's end can scan arbitrarily many block headers.
    static long decompressedOutputBound(MemorySegment compressed, long compressedLength, long outputLength)
    {
        long position = 0;
        long total = 0;
        while (position < compressedLength) {
            MemorySegment frame = compressed.asSlice(position, compressedLength - position);
            long contentSize = decompressedLength(frame, frame.byteSize());
            if (contentSize == CONTENT_SIZE_UNKNOWN || contentSize >= outputLength - total) {
                return outputLength;
            }
            long frameSize;
            try {
                frameSize = (long) FRAME_COMPRESSED_SIZE_METHOD.invokeExact(frame, frame.byteSize());
            }
            catch (Error e) {
                throw e;
            }
            catch (Throwable e) {
                throw new Error("Unexpected exception", e);
            }
            if (isError(frameSize)) {
                throw decompressionError("Invalid or truncated frame", frameSize);
            }
            total += contentSize;
            position += frameSize;
        }
        return total;
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
            long _ = (long) FREE_DECOMPRESS_STREAM_METHOD.invokeExact(stream);
        }
        catch (Error e) {
            throw e;
        }
        catch (Throwable e) {
            throw new Error("Unexpected exception", e);
        }
    }

    public static void initDecompressStream(MemorySegment stream)
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
            throw decompressionError("Failed to initialize zstd decompression stream", result);
        }
    }

    public static long decompressStreamSimpleArgs(
            MemorySegment stream,
            MemorySegment output,
            long outputLength,
            MemorySegment outputPosition,
            MemorySegment input,
            long inputLength,
            MemorySegment inputPosition)
    {
        long result;
        try {
            result = (long) DECOMPRESS_STREAM_SIMPLE_ARGS_METHOD.invokeExact(stream, output, outputLength, outputPosition, input, inputLength, inputPosition);
        }
        catch (Error e) {
            throw e;
        }
        catch (Throwable e) {
            throw new Error("Unexpected exception", e);
        }

        if (isError(result)) {
            throw decompressionError("Unknown error occurred during decompression", result);
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
            long _ = (long) FREE_COMPRESS_STREAM_METHOD.invokeExact(stream);
        }
        catch (Error e) {
            throw e;
        }
        catch (Throwable e) {
            throw new Error("Unexpected exception", e);
        }
    }

    public static void initCompressStream(MemorySegment stream, int compressionLevel)
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
            throw new IllegalArgumentException("Failed to initialize zstd compression stream at level " + compressionLevel + ": " + getErrorName(result));
        }
    }

    public static void setCompressStreamParameter(MemorySegment stream, int parameter, int value)
    {
        long result;
        try {
            result = (long) SET_COMPRESS_STREAM_PARAMETER_METHOD.invokeExact(stream, parameter, value);
        }
        catch (Error e) {
            throw e;
        }
        catch (Throwable e) {
            throw new Error("Unexpected exception", e);
        }

        if (isError(result)) {
            throw new IllegalArgumentException("Failed to set zstd compression parameter " + parameter + " to " + value + ": " + getErrorName(result));
        }
    }

    public static long compressStream2SimpleArgs(
            MemorySegment stream,
            MemorySegment output,
            long outputLength,
            MemorySegment outputPosition,
            MemorySegment input,
            long inputLength,
            MemorySegment inputPosition,
            int endOp)
    {
        long result;
        try {
            result = (long) COMPRESS_STREAM_2_SIMPLE_ARGS_METHOD.invokeExact(stream, output, outputLength, outputPosition, input, inputLength, inputPosition, endOp);
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

    private static RuntimeException decompressionError(String message, long code)
    {
        if (isMalformedInputError(code)) {
            return new MalformedInputException(0, getErrorName(code));
        }
        return new IllegalArgumentException(message + ": " + getErrorName(code));
    }

    // Matches ZstdJavaDecompressor, which reports all of these as MalformedInputException, including an output buffer too small for one-shot decompression
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
