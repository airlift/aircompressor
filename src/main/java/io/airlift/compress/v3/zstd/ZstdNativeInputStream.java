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

import java.io.IOException;
import java.io.InputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.ref.Cleaner;

import static io.airlift.compress.v3.zstd.Util.checkPositionIndexes;
import static io.airlift.compress.v3.zstd.ZstdNative.DECOMPRESS_STREAM_INPUT_SIZE;
import static io.airlift.compress.v3.zstd.ZstdNative.createDecompressStream;
import static io.airlift.compress.v3.zstd.ZstdNative.decompressStreamSimpleArgs;
import static io.airlift.compress.v3.zstd.ZstdNative.initDecompressStream;
import static java.util.Objects.requireNonNull;

/**
 * An {@link InputStream} that decompresses zstd-compressed data using the native zstd library.
 * <p>
 * This native wrapper reads compressed data from the underlying {@link InputStream} into a buffer
 * sized by libzstd's recommendation, and passes the caller's output buffer to libzstd with no Java-side
 * buffer in between. For optimal performance, callers should provide sufficiently large output buffers
 * to minimize the number of calls into native code, or should wrap this stream in a buffered stream.
 * <p>
 * Native memory is used for the internal zstd streaming context. A {@link Cleaner} is
 * registered as a safety net to free native resources if {@link #close()} is not called.
 * <p>
 * This class is not thread-safe for concurrent access, but can be used from different
 * threads sequentially (e.g., in cooperative scheduling environments like Trino).
 */
public final class ZstdNativeInputStream
        extends ZstdInputStreamBase
{
    private static final Cleaner CLEANER = Cleaner.create();

    private final InputStream inputStream;
    private final Cleaner.Cleanable cleanable;

    private final MemorySegment decompressStream;

    private final byte[] inputBuffer;
    private final MemorySegment inputSegment;
    private int inputPosition;
    private int inputLimit;

    // Position pointers as heap arrays - reused across calls
    private final long[] sourcePosition = new long[1];
    private final long[] destinationPosition = new long[1];
    private final MemorySegment sourcePositionSegment = MemorySegment.ofArray(sourcePosition);
    private final MemorySegment destinationPositionSegment = MemorySegment.ofArray(destinationPosition);

    private final byte[] oneByte = new byte[1];

    private boolean closed;
    // the underlying stream has returned end of stream
    private boolean endOfStream;
    // the last native call ended a frame, so the source may end here
    private boolean frameComplete;
    // the last native call filled the output, so libzstd may still hold decoded data
    private boolean outputPending;
    // a decode or truncation error occurred, and the native context must not be used again
    private boolean failed;

    public ZstdNativeInputStream(InputStream inputStream)
    {
        ZstdNative.verifyEnabled();
        this.inputStream = requireNonNull(inputStream, "inputStream is null");

        // a shared arena allows sequential use from different threads, and the cleaner frees it deterministically on close
        NativeResources resources = new NativeResources();
        this.cleanable = CLEANER.register(this, resources);
        Arena arena = resources.arena();

        this.inputBuffer = new byte[DECOMPRESS_STREAM_INPUT_SIZE];
        this.inputSegment = MemorySegment.ofArray(inputBuffer);

        try {
            MemorySegment stream = createDecompressStream();
            if (stream.equals(MemorySegment.NULL)) {
                throw new IllegalStateException("Failed to create zstd decompression stream");
            }
            this.decompressStream = stream.reinterpret(arena, ZstdNative::freeDecompressStream);
            initDecompressStream(decompressStream);
        }
        catch (Throwable e) {
            try {
                cleanable.clean();
            }
            catch (Throwable cleanupFailure) {
                e.addSuppressed(cleanupFailure);
            }
            throw e;
        }
    }

    public static boolean isEnabled()
    {
        return ZstdNative.isEnabled();
    }

    @Override
    public int read()
            throws IOException
    {
        int result = read(oneByte, 0, 1);
        if (result == -1) {
            return -1;
        }
        return oneByte[0] & 0xFF;
    }

    @Override
    public int read(byte[] output, int outputOffset, int outputLength)
            throws IOException
    {
        if (closed) {
            throw new IOException("Stream is closed");
        }
        if (failed) {
            throw new IOException("Stream has failed");
        }

        if (output == null) {
            throw new NullPointerException("output is null");
        }
        checkPositionIndexes(outputOffset, outputOffset + outputLength, output.length);
        if (outputLength == 0) {
            return 0;
        }

        int totalRead = 0;
        while (totalRead < outputLength) {
            // Only read the source when libzstd has nothing left to give; return decoded data
            // first, so a source that blocks or fails does not hold back bytes already produced
            if (inputPosition >= inputLimit && !outputPending) {
                if (totalRead > 0) {
                    return totalRead;
                }
                if (!readInput()) {
                    if (!frameComplete) {
                        failed = true;
                        throw new IOException("Truncated zstd stream");
                    }
                    return -1;
                }
            }

            int inputPositionBefore = inputPosition;
            int outputAvailable = outputLength - totalRead;
            sourcePosition[0] = inputPosition;
            destinationPosition[0] = 0;

            MemorySegment outputSegment = MemorySegment.ofArray(output).asSlice(outputOffset + totalRead, outputAvailable);
            long result;
            try {
                result = decompressStreamSimpleArgs(
                        decompressStream,
                        outputSegment,
                        outputAvailable,
                        destinationPositionSegment,
                        inputSegment,
                        inputLimit,
                        sourcePositionSegment);
            }
            catch (RuntimeException e) {
                failed = true;
                throw e;
            }

            inputPosition = (int) sourcePosition[0];
            int outputWritten = (int) destinationPosition[0];
            totalRead += outputWritten;

            // result == 0 means the frame is complete and fully flushed
            frameComplete = result == 0;
            outputPending = !frameComplete && outputWritten == outputAvailable;
            if (!frameComplete && outputWritten == 0 && inputPosition == inputPositionBefore && inputPosition < inputLimit) {
                // libzstd neither consumed input nor produced output, so looping would never end
                failed = true;
                throw new IOException("Decompression made no progress");
            }
        }

        return totalRead;
    }

    private boolean readInput()
            throws IOException
    {
        if (endOfStream) {
            return false;
        }
        int bytesRead = inputStream.read(inputBuffer);
        if (bytesRead == -1) {
            endOfStream = true;
            return false;
        }
        inputPosition = 0;
        inputLimit = bytesRead;
        return true;
    }

    @Override
    public void close()
            throws IOException
    {
        if (!closed) {
            closed = true;
            try (inputStream) {
                cleanable.clean();
            }
        }
    }

    /**
     * Holds native resources that must be freed.
     * Implements Runnable so it can be used with Cleaner.
     */
    private record NativeResources(Arena arena)
            implements Runnable
    {
        NativeResources()
        {
            this(Arena.ofShared());
        }

        @Override
        public void run()
        {
            arena.close();
        }
    }
}
