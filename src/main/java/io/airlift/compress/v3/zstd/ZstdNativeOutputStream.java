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
import java.io.OutputStream;
import java.lang.foreign.MemorySegment;

import static io.airlift.compress.v3.zstd.Util.checkPositionIndexes;
import static io.airlift.compress.v3.zstd.ZstdNative.COMPRESS_STREAM_OUTPUT_SIZE;
import static io.airlift.compress.v3.zstd.ZstdNative.DEFAULT_COMPRESSION_LEVEL;
import static io.airlift.compress.v3.zstd.ZstdNative.ZSTD_C_CHECKSUM_FLAG;
import static io.airlift.compress.v3.zstd.ZstdNative.ZSTD_E_CONTINUE;
import static io.airlift.compress.v3.zstd.ZstdNative.ZSTD_E_END;
import static io.airlift.compress.v3.zstd.ZstdNative.ZSTD_E_FLUSH;
import static io.airlift.compress.v3.zstd.ZstdNative.compressStream2SimpleArgs;
import static io.airlift.compress.v3.zstd.ZstdNative.createCompressStream;
import static io.airlift.compress.v3.zstd.ZstdNative.initCompressStream;
import static io.airlift.compress.v3.zstd.ZstdNative.setCompressStreamParameter;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static java.lang.ref.Reference.reachabilityFence;
import static java.util.Objects.requireNonNull;

/**
 * An {@link OutputStream} that compresses data using the native zstd library.
 * <p>
 * This native wrapper buffers small writes to amortize calls into native code. Reusable native
 * buffers allow compression to run without delaying JVM safepoints. Compressed data is written to
 * the underlying {@link OutputStream} from a buffer sized by libzstd's recommendation.
 * <p>
 * Native memory is used for the zstd streaming context. A {@link java.lang.ref.Cleaner} is
 * registered as a safety net to free native resources if {@link #close()} is not called.
 * <p>
 * This class is not thread-safe for concurrent access, but can be used from different
 * threads sequentially (e.g., in cooperative scheduling environments like Trino).
 */
public final class ZstdNativeOutputStream
        extends ZstdOutputStreamBase
{
    private static final MemorySegment EMPTY_INPUT = MemorySegment.NULL;

    private final OutputStream outputStream;
    private final ZstdNativeResources resources;

    private final MemorySegment compressStream;

    private final byte[] outputBuffer;
    private final MemorySegment outputSegment;
    private final MemorySegment inputSegment;

    private final MemorySegment sourcePositionSegment;
    private final MemorySegment destinationPositionSegment;

    private final byte[] inputBuffer = new byte[8192];
    private int bufferedInputSize;

    // set once a finish is attempted, whether or not it succeeded; the native context is freed and the underlying stream may still be open
    private boolean finished;
    // the first compression, write, flush or finish failure, after which the frame cannot be continued or ended
    private Throwable failure;
    private boolean closed;

    public ZstdNativeOutputStream(OutputStream outputStream)
            throws IOException
    {
        this(outputStream, DEFAULT_COMPRESSION_LEVEL);
    }

    public ZstdNativeOutputStream(OutputStream outputStream, int compressionLevel)
            throws IOException
    {
        ZstdNative.verifyEnabled();
        this.outputStream = requireNonNull(outputStream, "outputStream is null");

        this.resources = new ZstdNativeResources(this);

        this.outputBuffer = new byte[COMPRESS_STREAM_OUTPUT_SIZE];

        try {
            this.inputSegment = resources.arena().allocate(Constants.MAX_BLOCK_SIZE);
            this.outputSegment = resources.arena().allocate(outputBuffer.length);
            this.sourcePositionSegment = resources.arena().allocate(JAVA_LONG);
            this.destinationPositionSegment = resources.arena().allocate(JAVA_LONG);
            MemorySegment stream = createCompressStream();
            if (stream.equals(MemorySegment.NULL)) {
                throw new IOException("Failed to create zstd compression stream");
            }
            this.compressStream = stream.reinterpret(resources.arena(), ZstdNative::freeCompressStream);
            initCompressStream(compressStream, compressionLevel);
            // ZstdJavaOutputStream always writes a content checksum, so match it
            setCompressStreamParameter(compressStream, ZSTD_C_CHECKSUM_FLAG, 1);
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
    public void write(int b)
            throws IOException
    {
        checkWritable();
        inputBuffer[bufferedInputSize++] = (byte) b;
        if (bufferedInputSize == inputBuffer.length) {
            writeBufferedInput();
        }
    }

    @Override
    public void write(byte[] buffer, int offset, int length)
            throws IOException
    {
        checkWritable();

        if (buffer == null) {
            throw new NullPointerException("buffer is null");
        }
        checkPositionIndexes(offset, offset + length, buffer.length);

        if (length >= inputBuffer.length) {
            writeBufferedInput();
            writeDirect(buffer, offset, length);
            return;
        }

        int size = Math.min(length, inputBuffer.length - bufferedInputSize);
        System.arraycopy(buffer, offset, inputBuffer, bufferedInputSize, size);
        bufferedInputSize += size;
        if (bufferedInputSize == inputBuffer.length) {
            writeBufferedInput();
        }
        if (size < length) {
            bufferedInputSize = length - size;
            System.arraycopy(buffer, offset + size, inputBuffer, 0, bufferedInputSize);
        }
    }

    private void checkWritable()
            throws IOException
    {
        if (closed) {
            throw new IOException("Stream is closed");
        }
        if (failure != null) {
            throw failedException();
        }
        if (finished) {
            throw new IOException("Stream is finished");
        }
    }

    private void writeBufferedInput()
            throws IOException
    {
        if (bufferedInputSize > 0) {
            writeDirect(inputBuffer, 0, bufferedInputSize);
            bufferedInputSize = 0;
        }
    }

    private void writeDirect(byte[] buffer, int offset, int length)
            throws IOException
    {
        try {
            while (length > 0) {
                int chunkSize = Math.min(length, (int) inputSegment.byteSize());
                MemorySegment.copy(buffer, offset, inputSegment, JAVA_BYTE, 0, chunkSize);
                sourcePositionSegment.set(JAVA_LONG, 0, 0);
                while (sourcePositionSegment.get(JAVA_LONG, 0) < chunkSize) {
                    destinationPositionSegment.set(JAVA_LONG, 0, 0);
                    compress(inputSegment, chunkSize, ZSTD_E_CONTINUE);
                    writeCompressedOutput();
                }
                offset += chunkSize;
                length -= chunkSize;
            }
        }
        catch (IOException | RuntimeException e) {
            fail(e);
            throw e;
        }
        finally {
            // The cleaner owns both the native buffers and the streaming context.
            reachabilityFence(this);
        }
    }

    private void writeCompressedOutput()
            throws IOException
    {
        int outputWritten = (int) destinationPositionSegment.get(JAVA_LONG, 0);
        if (outputWritten > 0) {
            MemorySegment.copy(outputSegment, JAVA_BYTE, 0, outputBuffer, 0, outputWritten);
            outputStream.write(outputBuffer, 0, outputWritten);
        }
    }

    @Override
    public void flush()
            throws IOException
    {
        if (closed) {
            return;
        }
        if (failure != null) {
            throw failedException();
        }

        try {
            if (!finished) {
                writeBufferedInput();
                flushInternal(ZSTD_E_FLUSH);
            }
            outputStream.flush();
        }
        catch (IOException | RuntimeException e) {
            fail(e);
            throw e;
        }
    }

    // libzstd reports a failure of the stream itself, such as an allocation failure, as an argument error
    private long compress(MemorySegment input, long inputLimit, int endOp)
            throws IOException
    {
        try {
            return compressStream2SimpleArgs(
                    compressStream,
                    outputSegment,
                    outputBuffer.length,
                    destinationPositionSegment,
                    input,
                    inputLimit,
                    sourcePositionSegment,
                    endOp);
        }
        catch (IllegalArgumentException e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    private void flushInternal(int endOp)
            throws IOException
    {
        try {
            long remaining;
            do {
                sourcePositionSegment.set(JAVA_LONG, 0, 0);
                destinationPositionSegment.set(JAVA_LONG, 0, 0);

                remaining = compress(EMPTY_INPUT, 0, endOp);

                writeCompressedOutput();
            }
            while (remaining > 0);
        }
        finally {
            // The cleaner owns both the native buffers and the streaming context.
            reachabilityFence(this);
        }
    }

    @Override
    void finishWithoutClosingSource()
            throws IOException
    {
        if (finished) {
            // the frame was not ended if the stream failed, and that is not retried
            if (failure != null) {
                throw failedException();
            }
            return;
        }

        // a failed end of frame leaves the native context unusable, so free it either way
        finished = true;
        try {
            if (failure != null) {
                throw failedException();
            }
            writeBufferedInput();
            flushInternal(ZSTD_E_END);
        }
        catch (IOException | RuntimeException e) {
            fail(e);
            throw e;
        }
        finally {
            try {
                resources.clean();
            }
            finally {
                reachabilityFence(this);
            }
        }
    }

    @Override
    public void close()
            throws IOException
    {
        if (!closed) {
            closed = true;
            // a failure to finish the frame stays primary; a failure to close the destination is suppressed into it
            try (outputStream) {
                finishWithoutClosingSource();
            }
        }
    }

    private void fail(Throwable e)
    {
        if (failure == null) {
            failure = e;
        }
    }

    // the first failure is the cause, so a later call reports why the stream cannot be used
    private IOException failedException()
    {
        return new IOException("Stream has failed", failure);
    }
}
