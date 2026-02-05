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
 * Native memory is used for the internal zstd streaming context. A {@link Cleaner} is
 * registered as a safety net to free native resources if {@link #close()} is not called.
 * <p>
 * This class is not thread-safe and must only be used from a single thread.
 */
public final class ZstdNativeInputStream
        extends ZstdInputStream
{
    private static final Cleaner CLEANER = Cleaner.create();

    private final InputStream inputStream;
    private final Cleaner.Cleanable cleanable;

    // Native decompression stream context
    private final MemorySegment decompressStream;

    // Heap buffer for reading compressed data from InputStream
    private final byte[] inputBuffer;
    private int inputPosition;
    private int inputLimit;

    // Position pointers as heap arrays - reused across calls
    private final long[] sourcePosition = new long[1];
    private final long[] destinationPosition = new long[1];
    private final MemorySegment sourcePositionSegment = MemorySegment.ofArray(sourcePosition);
    private final MemorySegment destinationPositionSegment = MemorySegment.ofArray(destinationPosition);

    private boolean closed;
    private boolean endOfStream;

    public ZstdNativeInputStream(InputStream inputStream)
            throws IOException
    {
        ZstdNative.verifyEnabled();
        this.inputStream = requireNonNull(inputStream, "inputStream is null");

        // Create native resources with confined arena for deterministic cleanup
        NativeResources resources = new NativeResources();
        this.cleanable = CLEANER.register(this, resources);
        Arena arena = resources.arena();

        this.inputBuffer = new byte[DECOMPRESS_STREAM_INPUT_SIZE];

        // Create and initialize decompression stream context
        MemorySegment stream = createDecompressStream();
        if (stream.equals(MemorySegment.NULL)) {
            cleanable.clean();
            throw new IOException("Failed to create zstd decompression stream");
        }
        // Attach stream to arena so it gets freed when arena closes
        this.decompressStream = stream.reinterpret(arena, ZstdNative::freeDecompressStream);
        initDecompressStream(decompressStream);
    }

    @Override
    public int read()
            throws IOException
    {
        byte[] singleByte = new byte[1];
        int result = read(singleByte, 0, 1);
        if (result == -1) {
            return -1;
        }
        return singleByte[0] & 0xFF;
    }

    @Override
    public int read(byte[] output, int outputOffset, int outputLength)
            throws IOException
    {
        if (closed) {
            throw new IOException("Stream is closed");
        }

        requireNonNull(output, "output is null");
        checkPositionIndexes(outputOffset, outputOffset + outputLength, output.length);
        if (outputLength == 0) {
            return 0;
        }

        int totalRead = 0;
        while (totalRead < outputLength) {
            // Check if we need to read more input
            if (inputPosition >= inputLimit) {
                if (endOfStream) {
                    return totalRead > 0 ? totalRead : -1;
                }
                // Need more input data
                int bytesRead = inputStream.read(inputBuffer);
                if (bytesRead == -1) {
                    endOfStream = true;
                    return totalRead > 0 ? totalRead : -1;
                }
                inputPosition = 0;
                inputLimit = bytesRead;
            }

            // Set up position pointers
            int inputPositionBefore = inputPosition;
            sourcePosition[0] = inputPosition;
            destinationPosition[0] = 0;

            // Create memory segment wrapping the output slice
            MemorySegment outputSegment = MemorySegment.ofArray(output).asSlice(outputOffset + totalRead, outputLength - totalRead);
            MemorySegment inputSegment = MemorySegment.ofArray(inputBuffer);

            // Decompress directly into the caller's buffer
            long result = decompressStreamSimpleArgs(
                    decompressStream,
                    outputSegment,
                    outputLength - totalRead,
                    destinationPositionSegment,
                    inputSegment,
                    inputLimit,
                    sourcePositionSegment);

            // Update positions from the position arrays
            inputPosition = (int) sourcePosition[0];
            int outputWritten = (int) destinationPosition[0];
            totalRead += outputWritten;

            // result == 0 means frame complete, check for concatenated frames
            if (result == 0) {
                handleFrameComplete();
            }

            // If no progress was made (no input consumed and no output produced), throw to prevent infinite loop
            if (outputWritten == 0 && inputPosition == inputPositionBefore && inputPosition < inputLimit) {
                throw new IOException("Decompression made no progress");
            }
        }

        return totalRead;
    }

    /**
     * Handles completion of a frame, checking for concatenated frames.
     */
    private void handleFrameComplete()
            throws IOException
    {
        // Check if there's more input (concatenated frames)
        if (inputPosition < inputLimit) {
            // More data available, re-init stream for next frame
            initDecompressStream(decompressStream);
        }
        else {
            // Try to read more from input stream
            int bytesRead = inputStream.read(inputBuffer);
            if (bytesRead == -1) {
                endOfStream = true;
            }
            else {
                // Reset input buffer and re-init for next frame
                inputPosition = 0;
                inputLimit = bytesRead;
                initDecompressStream(decompressStream);
            }
        }
    }

    @Override
    public void close()
            throws IOException
    {
        if (!closed) {
            closed = true;
            cleanable.clean();  // Idempotent - safe to call multiple times
            inputStream.close();
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
