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
package io.airlift.compress.v3.lz4;

import io.airlift.compress.v3.MalformedInputException;
import io.airlift.compress.v3.xxhash.XxHash32Hasher;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static io.airlift.compress.v3.lz4.Lz4FrameFormat.BD_4MB;
import static io.airlift.compress.v3.lz4.Lz4FrameFormat.BD_RESERVED_MASK;
import static io.airlift.compress.v3.lz4.Lz4FrameFormat.BLOCK_MAX_SIZE_4MB;
import static io.airlift.compress.v3.lz4.Lz4FrameFormat.BLOCK_SIZE_MASK;
import static io.airlift.compress.v3.lz4.Lz4FrameFormat.END_MARK_SIZE;
import static io.airlift.compress.v3.lz4.Lz4FrameFormat.FLG_BLOCK_CHECKSUM;
import static io.airlift.compress.v3.lz4.Lz4FrameFormat.FLG_BLOCK_INDEPENDENCE;
import static io.airlift.compress.v3.lz4.Lz4FrameFormat.FLG_CONTENT_CHECKSUM;
import static io.airlift.compress.v3.lz4.Lz4FrameFormat.FLG_CONTENT_SIZE;
import static io.airlift.compress.v3.lz4.Lz4FrameFormat.FLG_DICTIONARY_ID;
import static io.airlift.compress.v3.lz4.Lz4FrameFormat.FLG_RESERVED_MASK;
import static io.airlift.compress.v3.lz4.Lz4FrameFormat.FLG_VERSION;
import static io.airlift.compress.v3.lz4.Lz4FrameFormat.HEADER_SIZE;
import static io.airlift.compress.v3.lz4.Lz4FrameFormat.MAGIC;
import static io.airlift.compress.v3.lz4.Lz4FrameFormat.SKIPPABLE_MAGIC;
import static io.airlift.compress.v3.lz4.Lz4FrameFormat.SKIPPABLE_MAGIC_MASK;
import static io.airlift.compress.v3.lz4.Lz4FrameFormat.UNCOMPRESSED_BLOCK_FLAG;
import static io.airlift.compress.v3.lz4.Lz4FrameFormat.blockMaximumSize;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Objects.requireNonNull;

/**
 * Shared implementation of the LZ4 frame format used by both the Java and native frame codecs.
 * <p>
 * The framing (magic number, frame descriptor, the mandatory xxHash32 header checksum, block
 * framing and end mark) is handled here in Java, while the per-block compression is delegated to a
 * raw LZ4 block {@link Lz4Compressor}/{@link Lz4Decompressor}. This keeps the two implementations
 * identical apart from the block codec, and means the native codec only relies on the raw block
 * functions of the native library rather than its frame API.
 * <p>
 * Frames are always written with independent blocks so they can be decoded block-by-block.
 * Decompression handles multiple concatenated frames, skipping any skippable frames. Frames using
 * linked blocks or a dictionary are rejected, as they cannot be decoded block-by-block.
 */
final class Lz4FrameCompression
{
    private static final ValueLayout.OfInt INT_LE = JAVA_INT_UNALIGNED.withOrder(LITTLE_ENDIAN);
    private static final ValueLayout.OfLong LONG_LE = JAVA_LONG_UNALIGNED.withOrder(LITTLE_ENDIAN);

    private Lz4FrameCompression() {}

    public static int maxCompressedLength(int uncompressedSize)
    {
        if (uncompressedSize < 0) {
            throw new IllegalArgumentException("uncompressedSize is negative: " + uncompressedSize);
        }
        // header + end mark + the uncompressed data itself (worst case: every block stored uncompressed)
        // plus a 4-byte block header for each block. Computed in long to avoid int overflow.
        long blocks = ((long) uncompressedSize + BLOCK_MAX_SIZE_4MB - 1) / BLOCK_MAX_SIZE_4MB;
        long maxLength = HEADER_SIZE + END_MARK_SIZE + (long) uncompressedSize + (Integer.BYTES * blocks);
        if (maxLength > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Maximum compressed length exceeds Integer.MAX_VALUE for uncompressedSize: " + uncompressedSize);
        }
        return toIntExact(maxLength);
    }

    public static int compress(Lz4Compressor blockCompressor, byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
    {
        verifyRange(input, inputOffset, inputLength);
        verifyRange(output, outputOffset, maxOutputLength);

        MemorySegment inputSegment = MemorySegment.ofArray(input).asSlice(inputOffset, inputLength);
        MemorySegment outputSegment = MemorySegment.ofArray(output).asSlice(outputOffset, maxOutputLength);
        return compress(blockCompressor, inputSegment, outputSegment);
    }

    public static int compress(Lz4Compressor blockCompressor, MemorySegment input, MemorySegment output)
    {
        long inputLength = input.byteSize();
        long outputLimit = output.byteSize();

        // frame header
        long position = writeInt(output, 0, outputLimit, MAGIC);
        position = writeByte(output, position, outputLimit, (byte) (FLG_VERSION | FLG_BLOCK_INDEPENDENCE));
        position = writeByte(output, position, outputLimit, (byte) BD_4MB);
        int headerChecksum = (XxHash32Hasher.hash(output.asSlice(position - 2, 2)) >>> 8) & 0xFF;
        position = writeByte(output, position, outputLimit, (byte) headerChecksum);

        // data blocks
        byte[] scratch = new byte[blockCompressor.maxCompressedLength(Math.clamp(inputLength, 1, BLOCK_MAX_SIZE_4MB))];
        MemorySegment scratchSegment = MemorySegment.ofArray(scratch);
        long inputPosition = 0;
        while (inputPosition < inputLength) {
            int blockLength = toIntExact(Math.min(BLOCK_MAX_SIZE_4MB, inputLength - inputPosition));
            MemorySegment block = input.asSlice(inputPosition, blockLength);
            int compressedLength = blockCompressor.compress(block, scratchSegment);

            if (compressedLength < blockLength) {
                position = writeInt(output, position, outputLimit, compressedLength);
                ensureCapacity(position, compressedLength, outputLimit);
                MemorySegment.copy(scratchSegment, 0, output, position, compressedLength);
                position += compressedLength;
            }
            else {
                // storing the block uncompressed is smaller (or no larger) than the compressed form
                position = writeInt(output, position, outputLimit, blockLength | UNCOMPRESSED_BLOCK_FLAG);
                ensureCapacity(position, blockLength, outputLimit);
                MemorySegment.copy(input, inputPosition, output, position, blockLength);
                position += blockLength;
            }
            inputPosition += blockLength;
        }

        // end mark
        position = writeInt(output, position, outputLimit, 0);
        return toIntExact(position);
    }

    public static int decompress(Lz4Decompressor blockDecompressor, byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
            throws MalformedInputException
    {
        verifyRange(input, inputOffset, inputLength);
        verifyRange(output, outputOffset, maxOutputLength);

        MemorySegment inputSegment = MemorySegment.ofArray(input).asSlice(inputOffset, inputLength);
        MemorySegment outputSegment = MemorySegment.ofArray(output).asSlice(outputOffset, maxOutputLength);
        return decompress(blockDecompressor, inputSegment, outputSegment);
    }

    public static int decompress(Lz4Decompressor blockDecompressor, MemorySegment input, MemorySegment output)
            throws MalformedInputException
    {
        long inputLength = input.byteSize();

        if (inputLength < HEADER_SIZE) {
            throw new MalformedInputException(0, "Input is too short to be an LZ4 frame");
        }

        // the input may hold several concatenated frames, all of which are decoded
        long position = 0;
        long outputPosition = 0;
        while (position < inputLength) {
            if (position + Integer.BYTES > inputLength) {
                throw new MalformedInputException(position, "Truncated LZ4 frame: incomplete magic number");
            }
            int magic = input.get(INT_LE, position);
            if (magic == MAGIC) {
                Frame frame = decompressFrame(blockDecompressor, input, position, output, outputPosition);
                position = frame.inputPosition();
                outputPosition = frame.outputPosition();
            }
            else if ((magic & SKIPPABLE_MAGIC_MASK) == SKIPPABLE_MAGIC) {
                position = skipFrame(input, position);
            }
            else {
                throw new MalformedInputException(position, "Invalid LZ4 frame magic number");
            }
        }

        return toIntExact(outputPosition);
    }

    /**
     * Position in the input and output after a single frame has been decoded.
     */
    private record Frame(long inputPosition, long outputPosition) {}

    private static Frame decompressFrame(Lz4Decompressor blockDecompressor, MemorySegment input, long frameStart, MemorySegment output, long outputStart)
            throws MalformedInputException
    {
        long inputLength = input.byteSize();
        long outputLimit = output.byteSize();

        // frame descriptor: FLG, BD, optional content size, then the header checksum
        long descriptorStart = frameStart + Integer.BYTES;
        if (descriptorStart + 2 > inputLength) {
            throw new MalformedInputException(descriptorStart, "Truncated LZ4 frame header");
        }
        int flg = input.get(JAVA_BYTE, descriptorStart) & 0xFF;
        int bd = input.get(JAVA_BYTE, descriptorStart + 1) & 0xFF;

        int version = (flg >>> 6) & 0b11;
        if (version != 1) {
            throw new MalformedInputException(descriptorStart, "Unsupported LZ4 frame version: " + version);
        }
        // reserved bits are for future extensions, so a frame setting them cannot be decoded safely
        if ((flg & FLG_RESERVED_MASK) != 0 || (bd & BD_RESERVED_MASK) != 0) {
            throw new MalformedInputException(descriptorStart, "Corrupt LZ4 frame: reserved bits in the frame descriptor must be zero");
        }

        boolean blockIndependence = (flg & FLG_BLOCK_INDEPENDENCE) != 0;
        boolean blockChecksum = (flg & FLG_BLOCK_CHECKSUM) != 0;
        boolean contentSize = (flg & FLG_CONTENT_SIZE) != 0;
        boolean contentChecksum = (flg & FLG_CONTENT_CHECKSUM) != 0;
        boolean dictionaryId = (flg & FLG_DICTIONARY_ID) != 0;

        // linked blocks reference data from previous blocks, so they cannot be decoded block-by-block
        if (!blockIndependence) {
            throw new MalformedInputException(descriptorStart, "LZ4 frames with linked blocks are not supported");
        }
        // there is no way to supply a dictionary through this API, and the blocks cannot be decoded without it
        if (dictionaryId) {
            throw new MalformedInputException(descriptorStart, "LZ4 frames with a dictionary are not supported");
        }

        int blockMaximumSize = blockMaximumSize((bd >>> 4) & 0b111);
        if (blockMaximumSize < 0) {
            throw new MalformedInputException(descriptorStart + 1, "Invalid LZ4 frame block maximum size");
        }

        long position = descriptorStart + 2;
        if (position + (contentSize ? Long.BYTES : 0) + 1 > inputLength) {
            throw new MalformedInputException(position, "Truncated LZ4 frame header");
        }
        long expectedContentSize = -1;
        if (contentSize) {
            expectedContentSize = input.get(LONG_LE, position);
            position += Long.BYTES;
        }

        int expectedHeaderChecksum = input.get(JAVA_BYTE, position) & 0xFF;
        int actualHeaderChecksum = (XxHash32Hasher.hash(input.asSlice(descriptorStart, position - descriptorStart)) >>> 8) & 0xFF;
        if (expectedHeaderChecksum != actualHeaderChecksum) {
            throw new MalformedInputException(position, "Corrupt LZ4 frame: invalid header checksum");
        }
        position++;

        long outputPosition = outputStart;
        while (true) {
            if (position + Integer.BYTES > inputLength) {
                throw new MalformedInputException(position, "Truncated LZ4 frame: missing block size");
            }
            int blockHeader = input.get(INT_LE, position);
            position += Integer.BYTES;
            if (blockHeader == 0) {
                // end mark
                break;
            }

            boolean uncompressed = (blockHeader & UNCOMPRESSED_BLOCK_FLAG) != 0;
            int blockLength = blockHeader & BLOCK_SIZE_MASK;
            if (blockLength > blockMaximumSize || position + blockLength > inputLength) {
                throw new MalformedInputException(position, "Truncated LZ4 frame: block extends past end of input");
            }

            if (uncompressed) {
                if (outputPosition + blockLength > outputLimit) {
                    throw new MalformedInputException(outputPosition, "Output buffer too small");
                }
                MemorySegment.copy(input, position, output, outputPosition, blockLength);
                outputPosition += blockLength;
            }
            else {
                MemorySegment block = input.asSlice(position, blockLength);
                MemorySegment blockOutput = output.asSlice(outputPosition, outputLimit - outputPosition);
                int decompressedLength = blockDecompressor.decompress(block, blockOutput);
                if (decompressedLength < 0) {
                    throw new MalformedInputException(outputPosition, "Output buffer too small");
                }
                if (decompressedLength > blockMaximumSize) {
                    throw new MalformedInputException(position, "Corrupt LZ4 frame: decompressed block exceeds maximum block size");
                }
                outputPosition += decompressedLength;
            }

            // the block checksum covers the block data as stored (position still points at its start)
            if (blockChecksum) {
                long blockChecksumPosition = position + blockLength;
                if (blockChecksumPosition + Integer.BYTES > inputLength) {
                    throw new MalformedInputException(blockChecksumPosition, "Truncated LZ4 frame: missing block checksum");
                }
                int expectedBlockChecksum = input.get(INT_LE, blockChecksumPosition);
                int actualBlockChecksum = XxHash32Hasher.hash(input.asSlice(position, blockLength));
                if (expectedBlockChecksum != actualBlockChecksum) {
                    throw new MalformedInputException(blockChecksumPosition, "Corrupt LZ4 frame: invalid block checksum");
                }
            }

            position += blockLength;
            if (blockChecksum) {
                position += Integer.BYTES;
            }
        }

        // the content size and checksum cover the content of this frame only
        long contentLength = outputPosition - outputStart;
        if (contentChecksum) {
            if (position + Integer.BYTES > inputLength) {
                throw new MalformedInputException(position, "Truncated LZ4 frame: missing content checksum");
            }
            int expectedContentChecksum = input.get(INT_LE, position);
            int actualContentChecksum = XxHash32Hasher.hash(output.asSlice(outputStart, contentLength));
            if (expectedContentChecksum != actualContentChecksum) {
                throw new MalformedInputException(position, "Corrupt LZ4 frame: invalid content checksum");
            }
            position += Integer.BYTES;
        }

        if (contentSize && contentLength != expectedContentSize) {
            throw new MalformedInputException(position, "Corrupt LZ4 frame: content size does not match frame header");
        }

        return new Frame(position, outputPosition);
    }

    /**
     * Skips a skippable frame, which holds user defined data that is not part of the decoded content.
     */
    private static long skipFrame(MemorySegment input, long frameStart)
            throws MalformedInputException
    {
        long inputLength = input.byteSize();

        long sizePosition = frameStart + Integer.BYTES;
        if (sizePosition + Integer.BYTES > inputLength) {
            throw new MalformedInputException(sizePosition, "Truncated LZ4 skippable frame: missing frame size");
        }
        long frameSize = Integer.toUnsignedLong(input.get(INT_LE, sizePosition));

        long frameEnd = sizePosition + Integer.BYTES + frameSize;
        if (frameEnd > inputLength) {
            throw new MalformedInputException(sizePosition, "Truncated LZ4 skippable frame");
        }
        return frameEnd;
    }

    private static long writeInt(MemorySegment output, long position, long limit, int value)
    {
        ensureCapacity(position, Integer.BYTES, limit);
        output.set(INT_LE, position, value);
        return position + Integer.BYTES;
    }

    private static long writeByte(MemorySegment output, long position, long limit, byte value)
    {
        ensureCapacity(position, 1, limit);
        output.set(JAVA_BYTE, position, value);
        return position + 1;
    }

    private static void ensureCapacity(long position, long length, long limit)
    {
        if (position + length > limit) {
            throw new IllegalArgumentException("Output buffer too small");
        }
    }

    private static void verifyRange(byte[] data, int offset, int length)
    {
        requireNonNull(data, "data is null");
        if (offset < 0 || length < 0 || offset + length > data.length) {
            throw new IllegalArgumentException(format("Invalid offset or length (%s, %s) in array of length %s", offset, length, data.length));
        }
    }
}
