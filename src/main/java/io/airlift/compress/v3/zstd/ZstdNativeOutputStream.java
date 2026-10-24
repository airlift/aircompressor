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
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.ref.Cleaner;

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
import static java.util.Objects.requireNonNull;

/**
 * An {@link OutputStream} that compresses data using the native zstd library.
 * <p>
 * This native wrapper passes the caller's input buffer to libzstd with no Java-side buffer in between,
 * and writes compressed data to the underlying {@link OutputStream} from a buffer sized by libzstd's
 * recommendation. For optimal performance, callers should provide sufficiently large input buffers
 * to minimize the number of calls into native code, or should wrap this stream in a buffered stream.
 * <p>
 * Native memory is used for the zstd streaming context. A {@link Cleaner} is
 * registered as a safety net to free native resources if {@link #close()} is not called.
 * <p>
 * This class is not thread-safe for concurrent access, but can be used from different
 * threads sequentially (e.g., in cooperative scheduling environments like Trino).
 */
public final class ZstdNativeOutputStream
        extends ZstdOutputStream
{
    private static final Cleaner CLEANER = Cleaner.create();
    private static final MemorySegment EMPTY_INPUT = MemorySegment.ofArray(new byte[0]);

    private final OutputStream outputStream;
    private final Cleaner.Cleanable cleanable;

    private final MemorySegment compressStream;

    private final byte[] outputBuffer;
    private final MemorySegment outputSegment;

    // Position pointers as heap arrays - reused across calls
    private final long[] sourcePosition = new long[1];
    private final long[] destinationPosition = new long[1];
    private final MemorySegment sourcePositionSegment = MemorySegment.ofArray(sourcePosition);
    private final MemorySegment destinationPositionSegment = MemorySegment.ofArray(destinationPosition);

    private final byte[] oneByte = new byte[1];

    // set once a finish is attempted, whether or not it succeeded; the native context is freed and the underlying stream may still be open
    private boolean finished;
    // a compression, write, flush or finish failed, so the frame cannot be continued or ended
    private boolean failed;
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

        // a shared arena allows sequential use from different threads, and the cleaner frees it deterministically on close
        NativeResources resources = new NativeResources();
        this.cleanable = CLEANER.register(this, resources);
        Arena arena = resources.arena();

        this.outputBuffer = new byte[COMPRESS_STREAM_OUTPUT_SIZE];
        this.outputSegment = MemorySegment.ofArray(outputBuffer);

        try {
            MemorySegment stream = createCompressStream();
            if (stream.equals(MemorySegment.NULL)) {
                throw new IOException("Failed to create zstd compression stream");
            }
            this.compressStream = stream.reinterpret(arena, ZstdNative::freeCompressStream);
            initCompressStream(compressStream, compressionLevel);
            // ZstdJavaOutputStream always writes a content checksum, so match it
            setCompressStreamParameter(compressStream, ZSTD_C_CHECKSUM_FLAG, 1);
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
    public void write(int b)
            throws IOException
    {
        oneByte[0] = (byte) b;
        write(oneByte, 0, 1);
    }

    @Override
    public void write(byte[] buffer, int offset, int length)
            throws IOException
    {
        if (closed) {
            throw new IOException("Stream is closed");
        }
        if (failed) {
            throw new IOException("Stream has failed");
        }
        if (finished) {
            throw new IOException("Stream is finished");
        }

        if (buffer == null) {
            throw new NullPointerException("buffer is null");
        }
        checkPositionIndexes(offset, offset + length, buffer.length);

        MemorySegment inputSegment = MemorySegment.ofArray(buffer);
        int inputPosition = offset;
        int inputLimit = offset + length;
        try {
            while (inputPosition < inputLimit) {
                sourcePosition[0] = inputPosition;
                destinationPosition[0] = 0;

                compressStream2SimpleArgs(
                        compressStream,
                        outputSegment,
                        outputBuffer.length,
                        destinationPositionSegment,
                        inputSegment,
                        inputLimit,
                        sourcePositionSegment,
                        ZSTD_E_CONTINUE);

                inputPosition = (int) sourcePosition[0];

                int outputWritten = (int) destinationPosition[0];
                if (outputWritten > 0) {
                    outputStream.write(outputBuffer, 0, outputWritten);
                }
            }
        }
        catch (IOException | RuntimeException e) {
            failed = true;
            throw e;
        }
    }

    @Override
    public void flush()
            throws IOException
    {
        if (closed) {
            return;
        }
        if (failed) {
            throw new IOException("Stream has failed");
        }

        try {
            if (!finished) {
                flushInternal(ZSTD_E_FLUSH);
            }
            outputStream.flush();
        }
        catch (IOException | RuntimeException e) {
            failed = true;
            throw e;
        }
    }

    private void flushInternal(int endOp)
            throws IOException
    {
        long remaining;
        do {
            sourcePosition[0] = 0;
            destinationPosition[0] = 0;

            remaining = compressStream2SimpleArgs(
                    compressStream,
                    outputSegment,
                    outputBuffer.length,
                    destinationPositionSegment,
                    EMPTY_INPUT,
                    0,
                    sourcePositionSegment,
                    endOp);

            int outputWritten = (int) destinationPosition[0];
            if (outputWritten > 0) {
                outputStream.write(outputBuffer, 0, outputWritten);
            }
        }
        while (remaining > 0);
    }

    @Override
    void finishWithoutClosingSource()
            throws IOException
    {
        if (!finished) {
            // a failed end of frame leaves the native context unusable, so free it either way
            finished = true;
            try {
                if (failed) {
                    throw new IOException("Stream has failed");
                }
                flushInternal(ZSTD_E_END);
            }
            catch (IOException | RuntimeException e) {
                failed = true;
                throw e;
            }
            finally {
                cleanable.clean();
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
                if (failed) {
                    // a failed frame cannot be ended, so only free the native context
                    cleanable.clean();
                }
                else {
                    finishWithoutClosingSource();
                }
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
