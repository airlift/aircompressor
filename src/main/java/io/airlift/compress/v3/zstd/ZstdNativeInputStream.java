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
import java.lang.foreign.MemorySegment;

import static io.airlift.compress.v3.zstd.Util.checkPositionIndexes;
import static io.airlift.compress.v3.zstd.Util.checkState;
import static io.airlift.compress.v3.zstd.ZstdNative.DECOMPRESS_STREAM_INPUT_SIZE;
import static io.airlift.compress.v3.zstd.ZstdNative.createDecompressStream;
import static io.airlift.compress.v3.zstd.ZstdNative.decompressStreamSimpleArgs;
import static io.airlift.compress.v3.zstd.ZstdNative.initDecompressStream;
import static java.lang.ref.Reference.reachabilityFence;
import static java.util.Objects.requireNonNull;

/**
 * An {@link InputStream} that decompresses zstd-compressed data using the native zstd library.
 * <p>
 * This native wrapper reads compressed data from the underlying {@link InputStream} into a buffer
 * sized by libzstd's recommendation. Small reads use a decoded buffer to amortize calls into native
 * code, while large reads decompress directly into the caller's buffer.
 * <p>
 * Native memory is used for the internal zstd streaming context. A {@link java.lang.ref.Cleaner} is
 * registered as a safety net to free native resources if {@link #close()} is not called.
 * <p>
 * This class is not thread-safe for concurrent access, but can be used from different
 * threads sequentially (e.g., in cooperative scheduling environments like Trino).
 */
public final class ZstdNativeInputStream
        extends ZstdInputStreamBase
{
    private final InputStream inputStream;
    private final ZstdNativeResources resources;

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

    private final byte[] decodedBuffer = new byte[8192];
    private int decodedPosition;
    private int decodedLimit;

    private boolean closed;
    // the underlying stream has returned end of stream
    private boolean endOfStream;
    // the last native call ended a frame, so the source may end here
    private boolean frameComplete;
    // the last native call filled the output, so libzstd may still hold decoded data
    private boolean outputPending;
    // a decode or truncation error occurred, and the native context is unusable until reset
    private boolean failed;

    public ZstdNativeInputStream(InputStream inputStream)
    {
        ZstdNative.verifyEnabled();
        this.inputStream = requireNonNull(inputStream, "inputStream is null");

        this.resources = new ZstdNativeResources(this);

        this.inputBuffer = new byte[DECOMPRESS_STREAM_INPUT_SIZE];
        this.inputSegment = MemorySegment.ofArray(inputBuffer);

        try {
            MemorySegment stream = createDecompressStream();
            if (stream.equals(MemorySegment.NULL)) {
                throw new IllegalStateException("Failed to create zstd decompression stream");
            }
            this.decompressStream = stream.reinterpret(resources.arena(), ZstdNative::freeDecompressStream);
            initDecompressStream(decompressStream);
        }
        catch (Throwable e) {
            try {
                resources.clean();
            }
            catch (Throwable cleanupFailure) {
                e.addSuppressed(cleanupFailure);
            }
            throw e;
        }
        finally {
            // the cleaner is registered with this stream, which must stay reachable until native code is done with its segments
            reachabilityFence(this);
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
        checkReadable();
        if (decodedPosition == decodedLimit && !fillDecodedBuffer()) {
            return -1;
        }
        return decodedBuffer[decodedPosition++] & 0xFF;
    }

    @Override
    public int read(byte[] output, int outputOffset, int outputLength)
            throws IOException
    {
        checkReadable();

        if (output == null) {
            throw new NullPointerException("output is null");
        }
        checkPositionIndexes(outputOffset, outputOffset + outputLength, output.length);
        if (outputLength == 0) {
            return 0;
        }

        if (decodedPosition == decodedLimit) {
            if (outputLength >= decodedBuffer.length) {
                return readDirect(output, outputOffset, outputLength);
            }
            if (!fillDecodedBuffer()) {
                return -1;
            }
        }
        int size = Math.min(outputLength, decodedLimit - decodedPosition);
        System.arraycopy(decodedBuffer, decodedPosition, output, outputOffset, size);
        decodedPosition += size;
        return size;
    }

    private boolean fillDecodedBuffer()
            throws IOException
    {
        decodedPosition = 0;
        decodedLimit = Math.max(0, readDirect(decodedBuffer, 0, decodedBuffer.length));
        return decodedLimit > 0;
    }

    private void checkReadable()
            throws IOException
    {
        if (closed) {
            throw new IOException("Stream is closed");
        }
        if (failed) {
            throw new IOException("Stream has failed");
        }
    }

    private int readDirect(byte[] output, int outputOffset, int outputLength)
            throws IOException
    {

        MemorySegment outputSegment = MemorySegment.ofArray(output);

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
            // libzstd continues from the output position, so the whole array is passed rather than a slice of it
            int outputStart = outputOffset + totalRead;
            sourcePosition[0] = inputPosition;
            destinationPosition[0] = outputStart;

            long result;
            try {
                result = decompressStreamSimpleArgs(
                        decompressStream,
                        outputSegment,
                        outputOffset + outputLength,
                        destinationPositionSegment,
                        inputSegment,
                        inputLimit,
                        sourcePositionSegment);
            }
            catch (IllegalArgumentException e) {
                // libzstd reports a failure that is not caused by the input, such as an allocation failure, as an argument error
                failed = true;
                throw new IOException(e.getMessage(), e);
            }
            catch (RuntimeException e) {
                failed = true;
                throw e;
            }
            finally {
                // the segments passed to libzstd do not keep this stream reachable, so the cleaner could free them mid-call
                reachabilityFence(this);
            }

            inputPosition = (int) sourcePosition[0];
            int outputWritten = (int) destinationPosition[0] - outputStart;
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

    // reuses the native context
    @Override
    void resetState()
    {
        checkState(!closed, "Stream is closed");
        try {
            initDecompressStream(decompressStream);
        }
        finally {
            reachabilityFence(this);
        }
        inputPosition = 0;
        inputLimit = 0;
        decodedPosition = 0;
        decodedLimit = 0;
        endOfStream = false;
        frameComplete = false;
        outputPending = false;
        failed = false;
    }

    @Override
    public void close()
            throws IOException
    {
        if (!closed) {
            closed = true;
            try (inputStream) {
                resources.clean();
            }
        }
    }
}
