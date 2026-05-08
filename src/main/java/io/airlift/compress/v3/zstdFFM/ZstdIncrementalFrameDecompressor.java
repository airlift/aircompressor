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
package io.airlift.compress.v3.zstdFFM;

import io.airlift.compress.v3.MalformedInputException;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;

import static io.airlift.compress.v3.zstdFFM.Constants.COMPRESSED_BLOCK;
import static io.airlift.compress.v3.zstdFFM.Constants.MAX_BLOCK_SIZE;
import static io.airlift.compress.v3.zstdFFM.Constants.RAW_BLOCK;
import static io.airlift.compress.v3.zstdFFM.Constants.RLE_BLOCK;
import static io.airlift.compress.v3.zstdFFM.Constants.SIZE_OF_BLOCK_HEADER;
import static io.airlift.compress.v3.zstdFFM.Constants.SIZE_OF_INT;
import static io.airlift.compress.v3.zstdFFM.FfmUtil.JAVA_BYTE;
import static io.airlift.compress.v3.zstdFFM.FfmUtil.JAVA_INT;
import static io.airlift.compress.v3.zstdFFM.Util.checkArgument;
import static io.airlift.compress.v3.zstdFFM.Util.checkState;
import static io.airlift.compress.v3.zstdFFM.Util.fail;
import static io.airlift.compress.v3.zstdFFM.Util.verify;
import static io.airlift.compress.v3.zstdFFM.ZstdFrameDecompressor.MAX_WINDOW_SIZE;
import static io.airlift.compress.v3.zstdFFM.ZstdFrameDecompressor.decodeRawBlock;
import static io.airlift.compress.v3.zstdFFM.ZstdFrameDecompressor.decodeRleBlock;
import static io.airlift.compress.v3.zstdFFM.ZstdFrameDecompressor.readFrameHeader;
import static io.airlift.compress.v3.zstdFFM.ZstdFrameDecompressor.verifyMagic;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;

public class ZstdIncrementalFrameDecompressor
{
    private enum State {
        INITIAL,
        READ_FRAME_MAGIC,
        READ_FRAME_HEADER,
        READ_BLOCK_HEADER,
        READ_BLOCK,
        READ_BLOCK_CHECKSUM,
        FLUSH_OUTPUT
    }

    private final ZstdFrameDecompressor frameDecompressor = new ZstdFrameDecompressor();

    private State state = State.INITIAL;
    private FrameHeader frameHeader;
    private int blockHeader = -1;

    private int inputConsumed;
    private int outputBufferUsed;

    private int inputRequired;
    private int requestedOutputSize;

    // current window buffer
    private byte[] windowBase = new byte[0];
    private MemorySegment windowSegment = MemorySegment.ofArray(windowBase);
    private long windowAddress = 0;
    private long windowLimit = 0;
    private long windowPosition = 0;

    private XxHash64 partialHash;

    public boolean isAtStoppingPoint()
    {
        return state == State.READ_FRAME_MAGIC;
    }

    public int getInputConsumed()
    {
        return inputConsumed;
    }

    public int getOutputBufferUsed()
    {
        return outputBufferUsed;
    }

    public int getInputRequired()
    {
        return inputRequired;
    }

    public int getRequestedOutputSize()
    {
        return requestedOutputSize;
    }

    public void partialDecompress(
            final MemorySegment inputBase,
            final long inputAddress,
            final long inputLimit,
            final byte[] outputArray,
            final int outputOffset,
            final int outputLimit)
    {
        if (inputRequired > inputLimit - inputAddress) {
            throw new IllegalArgumentException(format(
                    "Required %s input bytes, but only %s input bytes were supplied",
                    inputRequired,
                    inputLimit - inputAddress));
        }
        if (requestedOutputSize > 0 && outputOffset >= outputLimit) {
            throw new IllegalArgumentException("Not enough space in output buffer to output");
        }

        long input = inputAddress;
        int output = outputOffset;

        while (true) {
            // Flush ready output
            {
                int flushableOutputSize = computeFlushableOutputSize(frameHeader);
                if (flushableOutputSize > 0) {
                    int freeOutputSize = outputLimit - output;
                    if (freeOutputSize > 0) {
                        int copySize = min(freeOutputSize, flushableOutputSize);
                        System.arraycopy(windowBase, toIntExact(windowAddress), outputArray, output, copySize);
                        if (partialHash != null) {
                            partialHash.update(outputArray, output, copySize);
                        }
                        windowAddress += copySize;
                        output += copySize;
                        flushableOutputSize -= copySize;
                    }
                    if (flushableOutputSize > 0) {
                        requestOutput(inputAddress, outputOffset, input, output, flushableOutputSize);
                        return;
                    }
                }
            }
            // verify data was completely flushed
            checkState(computeFlushableOutputSize(frameHeader) == 0, "Expected output to be flushed");

            if (state == State.READ_FRAME_MAGIC || state == State.INITIAL) {
                if (inputLimit - input < 4) {
                    inputRequired(inputAddress, outputOffset, input, output, 4);
                    return;
                }
                input += verifyMagic(inputBase, input, inputLimit);
                state = State.READ_FRAME_HEADER;
            }

            if (state == State.READ_FRAME_HEADER) {
                if (inputLimit - input < 1) {
                    inputRequired(inputAddress, outputOffset, input, output, 1);
                    return;
                }
                int frameHeaderSize = determineFrameHeaderSize(inputBase, input, inputLimit);
                if (inputLimit - input < frameHeaderSize) {
                    inputRequired(inputAddress, outputOffset, input, output, frameHeaderSize);
                    return;
                }
                frameHeader = readFrameHeader(inputBase, input, inputLimit);
                verify(frameHeaderSize == frameHeader.headerSize, input, "Unexpected frame header size");
                input += frameHeaderSize;
                state = State.READ_BLOCK_HEADER;

                reset();
                if (frameHeader.hasChecksum) {
                    partialHash = new XxHash64();
                }
            }
            else {
                verify(frameHeader != null, input, "Frame header is not set");
            }

            if (state == State.READ_BLOCK_HEADER) {
                long inputBufferSize = inputLimit - input;
                if (inputBufferSize < SIZE_OF_BLOCK_HEADER) {
                    inputRequired(inputAddress, outputOffset, input, output, SIZE_OF_BLOCK_HEADER);
                    return;
                }
                if (inputBufferSize >= SIZE_OF_INT) {
                    blockHeader = inputBase.get(JAVA_INT, input) & 0xFF_FFFF;
                }
                else {
                    // FFM enforces bounds, so we cannot read past the buffer end like the Unsafe variant
                    // (which relied on the JVM's array padding to silently return zero for the unread byte).
                    blockHeader = inputBase.get(JAVA_BYTE, input) & 0xFF |
                            (inputBase.get(JAVA_BYTE, input + 1) & 0xFF) << 8 |
                            (inputBase.get(JAVA_BYTE, input + 2) & 0xFF) << 16;
                }
                input += SIZE_OF_BLOCK_HEADER;
                state = State.READ_BLOCK;
            }
            else {
                verify(blockHeader != -1, input, "Block header is not set");
            }

            boolean lastBlock = (blockHeader & 1) != 0;
            if (state == State.READ_BLOCK) {
                int blockType = (blockHeader >>> 1) & 0b11;
                int blockSize = (blockHeader >>> 3) & 0x1F_FFFF;

                resizeWindowBufferIfNecessary(frameHeader, blockType, blockSize);

                int decodedSize;
                switch (blockType) {
                    case RAW_BLOCK: {
                        if (inputLimit - input < blockSize) {
                            inputRequired(inputAddress, outputOffset, input, output, blockSize);
                            return;
                        }
                        verify(windowLimit - windowPosition >= blockSize, input, "window buffer is too small");
                        decodedSize = decodeRawBlock(inputBase, input, blockSize, windowSegment, windowPosition, windowLimit);
                        input += blockSize;
                        break;
                    }
                    case RLE_BLOCK: {
                        if (inputLimit - input < 1) {
                            inputRequired(inputAddress, outputOffset, input, output, 1);
                            return;
                        }
                        verify(windowLimit - windowPosition >= blockSize, input, "window buffer is too small");
                        decodedSize = decodeRleBlock(blockSize, inputBase, input, windowSegment, windowPosition, windowLimit);
                        input += 1;
                        break;
                    }
                    case COMPRESSED_BLOCK: {
                        if (inputLimit - input < blockSize) {
                            inputRequired(inputAddress, outputOffset, input, output, blockSize);
                            return;
                        }
                        verify(windowLimit - windowPosition >= MAX_BLOCK_SIZE, input, "window buffer is too small");
                        decodedSize = frameDecompressor.decodeCompressedBlock(inputBase, input, blockSize, windowSegment, windowPosition, windowLimit, frameHeader.windowSize, windowAddress);
                        input += blockSize;
                        break;
                    }
                    default:
                        throw fail(input, "Invalid block type");
                }
                windowPosition += decodedSize;
                if (lastBlock) {
                    state = State.READ_BLOCK_CHECKSUM;
                }
                else {
                    state = State.READ_BLOCK_HEADER;
                }
            }

            if (state == State.READ_BLOCK_CHECKSUM) {
                if (frameHeader.hasChecksum) {
                    if (inputLimit - input < SIZE_OF_INT) {
                        inputRequired(inputAddress, outputOffset, input, output, SIZE_OF_INT);
                        return;
                    }

                    int checksum = inputBase.get(JAVA_INT, input);
                    input += SIZE_OF_INT;

                    checkState(partialHash != null, "Partial hash not set");

                    int pendingOutputSize = toIntExact(windowPosition - windowAddress);
                    partialHash.update(windowBase, toIntExact(windowAddress), pendingOutputSize);

                    long hash = partialHash.hash();
                    if (checksum != (int) hash) {
                        throw new MalformedInputException(input, format("Bad checksum. Expected: %s, actual: %s", Integer.toHexString(checksum), Integer.toHexString((int) hash)));
                    }
                }
                state = State.READ_FRAME_MAGIC;
                frameHeader = null;
                blockHeader = -1;
            }
        }
    }

    private void reset()
    {
        frameDecompressor.reset();

        windowAddress = 0;
        windowPosition = 0;
    }

    private int computeFlushableOutputSize(FrameHeader frameHeader)
    {
        return max(0, toIntExact(windowPosition - windowAddress - (frameHeader == null ? 0 : frameHeader.computeRequiredOutputBufferLookBackSize())));
    }

    private void resizeWindowBufferIfNecessary(FrameHeader frameHeader, int blockType, int blockSize)
    {
        int maxBlockOutput;
        if (blockType == RAW_BLOCK || blockType == RLE_BLOCK) {
            maxBlockOutput = blockSize;
        }
        else {
            maxBlockOutput = MAX_BLOCK_SIZE;
        }

        if (windowLimit - windowPosition < MAX_BLOCK_SIZE) {
            int requiredWindowSize = frameHeader.computeRequiredOutputBufferLookBackSize();
            checkState(windowPosition - windowAddress <= requiredWindowSize, "Expected output to be flushed");

            int windowContentsSize = toIntExact(windowPosition - windowAddress);

            if (windowAddress != 0) {
                System.arraycopy(windowBase, toIntExact(windowAddress), windowBase, 0, windowContentsSize);
                windowAddress = 0;
                windowPosition = windowAddress + windowContentsSize;
            }
            checkState(windowAddress == 0, "Window should be packed");

            if (windowLimit - windowPosition < maxBlockOutput) {
                int newWindowSize;
                if (frameHeader.contentSize >= 0 && frameHeader.contentSize < requiredWindowSize) {
                    newWindowSize = toIntExact(frameHeader.contentSize);
                }
                else {
                    newWindowSize = (windowContentsSize + maxBlockOutput) * 2;
                    newWindowSize = min(newWindowSize, max(requiredWindowSize, MAX_BLOCK_SIZE) * 4);
                    newWindowSize = min(newWindowSize, MAX_WINDOW_SIZE + MAX_BLOCK_SIZE);
                    newWindowSize = max(windowContentsSize + maxBlockOutput, newWindowSize);
                    checkState(windowContentsSize + maxBlockOutput <= newWindowSize, "Computed new window size buffer is not large enough");
                }
                windowBase = Arrays.copyOf(windowBase, newWindowSize);
                windowSegment = MemorySegment.ofArray(windowBase);
                windowLimit = newWindowSize;
            }

            checkState(windowLimit - windowPosition >= maxBlockOutput, "window buffer is too small");
        }
    }

    private static int determineFrameHeaderSize(final MemorySegment inputBase, final long inputAddress, final long inputLimit)
    {
        verify(inputAddress < inputLimit, inputAddress, "Not enough input bytes");

        int frameHeaderDescriptor = inputBase.get(JAVA_BYTE, inputAddress) & 0xFF;
        boolean singleSegment = (frameHeaderDescriptor & 0b100000) != 0;
        int dictionaryDescriptor = frameHeaderDescriptor & 0b11;
        int contentSizeDescriptor = frameHeaderDescriptor >>> 6;

        return 1 +
                (singleSegment ? 0 : 1) +
                (dictionaryDescriptor == 0 ? 0 : (1 << (dictionaryDescriptor - 1))) +
                (contentSizeDescriptor == 0 ? (singleSegment ? 1 : 0) : (1 << contentSizeDescriptor));
    }

    private void requestOutput(long inputAddress, int outputOffset, long input, int output, int requestedOutputSize)
    {
        updateInputOutputState(inputAddress, outputOffset, input, output);

        checkArgument(requestedOutputSize >= 0, "requestedOutputSize is negative");
        this.requestedOutputSize = requestedOutputSize;

        this.inputRequired = 0;
    }

    private void inputRequired(long inputAddress, int outputOffset, long input, int output, int inputRequired)
    {
        updateInputOutputState(inputAddress, outputOffset, input, output);

        checkState(inputRequired >= 0, "inputRequired is negative");
        this.inputRequired = inputRequired;

        this.requestedOutputSize = 0;
    }

    private void updateInputOutputState(long inputAddress, int outputOffset, long input, int output)
    {
        inputConsumed = (int) (input - inputAddress);
        checkState(inputConsumed >= 0, "inputConsumed is negative");
        outputBufferUsed = output - outputOffset;
        checkState(outputBufferUsed >= 0, "outputBufferUsed is negative");
    }
}
