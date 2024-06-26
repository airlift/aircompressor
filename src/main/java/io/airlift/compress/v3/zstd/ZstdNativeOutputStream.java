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
import java.lang.ref.Cleaner.Cleanable;

import static io.airlift.compress.v3.zstd.Util.checkPositionIndexes;
import static io.airlift.compress.v3.zstd.ZstdNative.COMPRESS_STREAM_OUTPUT_SIZE;
import static io.airlift.compress.v3.zstd.ZstdNative.DEFAULT_COMPRESSION_LEVEL;
import static io.airlift.compress.v3.zstd.ZstdNative.ZSTD_E_CONTINUE;
import static io.airlift.compress.v3.zstd.ZstdNative.ZSTD_E_END;
import static io.airlift.compress.v3.zstd.ZstdNative.ZSTD_E_FLUSH;
import static io.airlift.compress.v3.zstd.ZstdNative.compressStream2SimpleArgs;
import static io.airlift.compress.v3.zstd.ZstdNative.createCompressStream;
import static io.airlift.compress.v3.zstd.ZstdNative.initCompressStream;
import static java.util.Objects.requireNonNull;

/**
 * An {@link OutputStream} that compresses data using the native zstd library.
 * <p>
 * This native wrapper does not add any additional buffering, other than a small buffer for writing
 * compressed data to the underlying {@link OutputStream}. The compression is performed directly
 * from the caller's input buffer. For optimal performance, callers should provide sufficiently
 * large input buffers to minimize the number of calls into native code, or should wrap this stream
 * in a buffered stream.
 * <p>
 * Native memory is used for the zstd streaming context. A {@link Cleaner} is
 * registered as a safety net to free native resources if {@link #close()} is not called.
 * <p>
 * This class is not thread-safe and must only be used from a single thread.
 */
public final class ZstdNativeOutputStream
        extends ZstdOutputStream
{
    private static final Cleaner CLEANER = Cleaner.create();

    private final OutputStream outputStream;
    private final Cleanable cleanable;

    // Native compression stream context
    private final MemorySegment compressStream;

    // Heap buffer for compressed output data
    private final byte[] outputBuffer;

    // Position pointers as heap arrays - reused across calls
    private final long[] sourcePosition = new long[1];
    private final long[] destinationPosition = new long[1];
    private final MemorySegment sourcePositionSegment = MemorySegment.ofArray(sourcePosition);
    private final MemorySegment destinationPositionSegment = MemorySegment.ofArray(destinationPosition);

    private boolean closed;
    private boolean frameFinished;

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

        // Create native resources with confined arena for deterministic cleanup
        NativeResources resources = new NativeResources();
        this.cleanable = CLEANER.register(this, resources);
        Arena arena = resources.arena();

        // Allocate heap buffer for output
        this.outputBuffer = new byte[COMPRESS_STREAM_OUTPUT_SIZE];

        // Create and initialize compression stream context
        MemorySegment stream = createCompressStream();
        if (stream.equals(MemorySegment.NULL)) {
            cleanable.clean();
            throw new IOException("Failed to create zstd compression stream");
        }
        // Attach stream to arena so it gets freed when arena closes
        this.compressStream = stream.reinterpret(arena, ZstdNative::freeCompressStream);
        initCompressStream(compressStream, compressionLevel);
    }

    @Override
    public void write(int b)
            throws IOException
    {
        write(new byte[] {(byte) b}, 0, 1);
    }

    @Override
    public void write(byte[] buffer, int offset, int length)
            throws IOException
    {
        if (closed) {
            throw new IOException("Stream is closed");
        }

        if (buffer == null) {
            throw new NullPointerException("buffer is null");
        }
        checkPositionIndexes(offset, offset + length, buffer.length);

        // Create memory segments wrapping the buffers
        MemorySegment inputSegment = MemorySegment.ofArray(buffer);
        MemorySegment outputSegment = MemorySegment.ofArray(outputBuffer);

        int inputPos = offset;
        int inputEnd = offset + length;

        while (inputPos < inputEnd) {
            // Set up position pointers
            sourcePosition[0] = inputPos;
            destinationPosition[0] = 0;

            // Compress directly from the caller's buffer
            compressStream2SimpleArgs(
                    compressStream,
                    outputSegment,
                    outputBuffer.length,
                    destinationPositionSegment,
                    inputSegment,
                    inputEnd,
                    sourcePositionSegment,
                    ZSTD_E_CONTINUE);

            // Update input position
            inputPos = (int) sourcePosition[0];

            // Write any compressed output
            int outputWritten = (int) destinationPosition[0];
            if (outputWritten > 0) {
                outputStream.write(outputBuffer, 0, outputWritten);
            }
        }
    }

    @Override
    public void flush()
            throws IOException
    {
        if (closed) {
            throw new IOException("Stream is closed");
        }

        // Flush the zstd stream
        flushInternal(ZSTD_E_FLUSH);

        // Flush the underlying stream
        outputStream.flush();
    }

    private void flushInternal(int endOp)
            throws IOException
    {
        MemorySegment outputSegment = MemorySegment.ofArray(outputBuffer);
        // Empty input - just flushing
        byte[] emptyInput = new byte[0];
        MemorySegment inputSegment = MemorySegment.ofArray(emptyInput);

        long remaining;
        do {
            // Set up position pointers
            sourcePosition[0] = 0;
            destinationPosition[0] = 0;

            // Flush/end stream
            remaining = compressStream2SimpleArgs(
                    compressStream,
                    outputSegment,
                    outputBuffer.length,
                    destinationPositionSegment,
                    inputSegment,
                    0,
                    sourcePositionSegment,
                    endOp);

            // Write any output
            int outputWritten = (int) destinationPosition[0];
            if (outputWritten > 0) {
                outputStream.write(outputBuffer, 0, outputWritten);
            }
        }
        while (remaining > 0);
    }

    /**
     * Finishes the compression stream without closing the underlying output stream.
     * This is useful for Hadoop compatibility where the codec may need to finish
     * compression while keeping the underlying stream open.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void finishWithoutClosingSource()
            throws IOException
    {
        if (!closed && !frameFinished) {
            // End the compression stream
            flushInternal(ZSTD_E_END);
            frameFinished = true;
        }
    }

    @Override
    public void close()
            throws IOException
    {
        if (!closed) {
            try {
                if (!frameFinished) {
                    // End the compression stream
                    flushInternal(ZSTD_E_END);
                }
            }
            finally {
                closed = true;
                cleanable.clean();
                outputStream.close();
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
            this(Arena.ofConfined());
        }

        @Override
        public void run()
        {
            arena.close();
        }
    }
}
