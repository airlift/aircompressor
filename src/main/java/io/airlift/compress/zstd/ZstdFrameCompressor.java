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
package io.airlift.compress.zstd;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.slice.UnsafeSliceFactory;
import io.airlift.slice.XxHash64;

import static io.airlift.compress.zstd.Constants.COMPRESSED_BLOCK;
import static io.airlift.compress.zstd.Constants.MAGIC_NUMBER;
import static io.airlift.compress.zstd.Constants.MAX_BLOCK_SIZE;
import static io.airlift.compress.zstd.Constants.MIN_BLOCK_SIZE;
import static io.airlift.compress.zstd.Constants.MIN_WINDOW_LOG;
import static io.airlift.compress.zstd.Constants.RAW_BLOCK;
import static io.airlift.compress.zstd.Constants.RAW_LITERALS_BLOCK;
import static io.airlift.compress.zstd.Constants.SIZE_OF_BLOCK_HEADER;
import static io.airlift.compress.zstd.Constants.SIZE_OF_INT;
import static io.airlift.compress.zstd.Constants.SIZE_OF_SHORT;
import static io.airlift.compress.zstd.UnsafeUtil.UNSAFE;
import static io.airlift.compress.zstd.Util.checkArgument;
import static io.airlift.compress.zstd.Util.put24BitLittleEndian;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

class ZstdFrameCompressor
{
    static final int MAX_FRAME_HEADER_SIZE = 14;

    private static final int CHECKSUM_FLAG = 0b100;
    private static final int SINGLE_SEGMENT_FLAG = 0b100000;

    private ZstdFrameCompressor()
    {
    }

    // visible for testing
    static int writeMagic(final Object outputBase, final long outputAddress, final long outputLimit)
    {
        checkArgument(outputLimit - outputAddress >= SIZE_OF_INT, "Output buffer too small");

        UNSAFE.putInt(outputBase, outputAddress, MAGIC_NUMBER);
        return SIZE_OF_INT;
    }

    // visible for testing
    static int writeFrameHeader(final Object outputBase, final long outputAddress, final long outputLimit, int inputSize, int windowSize)
    {
        checkArgument(outputLimit - outputAddress >= MAX_FRAME_HEADER_SIZE, "Output buffer too small");

        long output = outputAddress;

        int contentSizeDescriptor = (inputSize >= 256 ? 1 : 0) + (inputSize >= 65536 + 256 ? 1 : 0);
        int frameHeaderDescriptor = (contentSizeDescriptor << 6) | CHECKSUM_FLAG; // dictionary ID missing

        boolean singleSegment = windowSize >= inputSize;
        if (singleSegment) {
            frameHeaderDescriptor |= SINGLE_SEGMENT_FLAG;
        }

        UNSAFE.putByte(outputBase, output, (byte) frameHeaderDescriptor);
        output++;

        if (!singleSegment) {
            int base = Integer.highestOneBit(windowSize);

            int exponent = 32 - Integer.numberOfLeadingZeros(base) - 1;
            if (exponent < MIN_WINDOW_LOG) {
                throw new IllegalArgumentException("Minimum window size is " + (1 << MIN_WINDOW_LOG));
            }

            int remainder = windowSize - base;
            if (remainder % (base / 8) != 0) {
                throw new IllegalArgumentException("Window size of magnitude 2^" + exponent + " must be multiple of " + (base / 8));
            }

            // mantissa is guaranteed to be between 0-7
            int mantissa = remainder / (base / 8);
            int encoded = ((exponent - MIN_WINDOW_LOG) << 3) | mantissa;

            UNSAFE.putByte(outputBase, output, (byte) encoded);
            output++;
        }

        switch (contentSizeDescriptor) {
            case 0:
                if (singleSegment) {
                    UNSAFE.putByte(outputBase, output++, (byte) inputSize);
                }
                break;
            case 1:
                UNSAFE.putShort(outputBase, output, (short) (inputSize - 256));
                output += SIZE_OF_SHORT;
                break;
            case 2:
                UNSAFE.putInt(outputBase, output, inputSize);
                output += SIZE_OF_INT;
                break;
            default:
                throw new AssertionError();
        }

        return (int) (output - outputAddress);
    }

    // visible for testing
    static int writeChecksum(Object outputBase, long outputAddress, long outputLimit, Object inputBase, long inputAddress, long inputLimit)
    {
        checkArgument(outputLimit - outputAddress >= SIZE_OF_INT, "Output buffer too small");

        int inputSize = (int) (inputLimit - inputAddress);

        Slice slice = Slices.EMPTY_SLICE;
        if (inputSize > 0) {
            slice = UnsafeSliceFactory.getInstance().newSlice(inputBase, inputAddress, inputSize);
        }

        long hash = XxHash64.hash(0, slice);

        UNSAFE.putInt(outputBase, outputAddress, (int) hash);

        return SIZE_OF_INT;
    }

    public static int compress(Object inputBase, long inputAddress, long inputLimit, Object outputBase, long outputAddress, long outputLimit, int compressionLevel)
    {
        int inputSize = (int) (inputLimit - inputAddress);

        CompressionParameters parameters = CompressionParameters.compute(compressionLevel, inputSize);

        long output = outputAddress;

        output += writeMagic(outputBase, output, outputLimit);
        output += writeFrameHeader(outputBase, output, outputLimit, inputSize, 1 << parameters.getWindowLog());
        output += compressFrame(inputBase, inputAddress, inputLimit, outputBase, output, outputLimit, parameters);
        output += writeChecksum(outputBase, output, outputLimit, inputBase, inputAddress, inputLimit);

        return (int) (output - outputAddress);
    }

    private static int compressFrame(Object inputBase, long inputAddress, long inputLimit, Object outputBase, long outputAddress, long outputLimit, CompressionParameters parameters)
    {
        int windowSize = 1 << parameters.getWindowLog(); // TODO: store window size in parameters directly?
        int blockSize = Math.min(MAX_BLOCK_SIZE, windowSize);

        int outputSize = (int) (outputLimit - outputAddress);
        int remaining = (int) (inputLimit - inputAddress);

        long output = outputAddress;
        long input = inputAddress;

        CompressionContext context = new CompressionContext(parameters, inputAddress, remaining);

        do {
            checkArgument(outputSize >= SIZE_OF_BLOCK_HEADER + MIN_BLOCK_SIZE, "Output buffer too small");

            int lastBlockFlag = blockSize >= remaining ? 1 : 0;
            blockSize = Math.min(blockSize, remaining);

            int compressedSize = 0;
            if (remaining > 0) {
                compressedSize = compressBlock(inputBase, input, blockSize, outputBase, output + SIZE_OF_BLOCK_HEADER, outputSize - SIZE_OF_BLOCK_HEADER, context, parameters);
            }

            if (compressedSize == 0) { // block is not compressible
                checkArgument(blockSize + SIZE_OF_BLOCK_HEADER <= outputSize, "Output size too small");

                int blockHeader = lastBlockFlag | (RAW_BLOCK << 1) | (blockSize << 3);
                put24BitLittleEndian(outputBase, output, blockHeader);
                UNSAFE.copyMemory(inputBase, input, outputBase, output + SIZE_OF_BLOCK_HEADER, blockSize);
                compressedSize = SIZE_OF_BLOCK_HEADER + blockSize;
            }
            else {
                int blockHeader = lastBlockFlag | (COMPRESSED_BLOCK << 1) | (compressedSize << 3);
                put24BitLittleEndian(outputBase, output, blockHeader);
                compressedSize += SIZE_OF_BLOCK_HEADER;
            }

            input += blockSize;
            remaining -= blockSize;
            output += compressedSize;
            outputSize -= compressedSize;
        }
        while (remaining > 0);

        return (int) (output - outputAddress);
    }

    private static int compressBlock(Object inputBase, long inputAddress, int inputSize, Object outputBase, long outputAddress, int outputSize, CompressionContext context, CompressionParameters parameters)
    {
        if (inputSize < MIN_BLOCK_SIZE + SIZE_OF_BLOCK_HEADER + 1) {
            //  don't even attempt compression below a certain input size
            return 0;
        }

        context.blockCompressionState.enforceMaxDistance(inputAddress + inputSize, 1 << parameters.getWindowLog());
        context.sequenceStore.reset();

        int lastLiteralsSize = parameters.getStrategy()
                .getCompressor()
                .compressBlock(inputBase, inputAddress, inputSize, context.sequenceStore, context.blockCompressionState, context.offsets, parameters);

        long lastLiteralsAddress = inputAddress + inputSize - lastLiteralsSize;

        // append [lastLiteralsAddress .. lastLiteralsSize] to sequenceStore literals buffer
        context.sequenceStore.appendLiterals(inputBase, lastLiteralsAddress, lastLiteralsSize);

        // convert length/offsets into codes
        context.sequenceStore.generateCodes();

        long outputLimit = outputAddress + outputSize;
        long output = outputAddress;

        int literalsSize = rawLiterals(outputBase, output, (int) (outputLimit - output), context.sequenceStore.literalsBuffer, ARRAY_BYTE_BASE_OFFSET, context.sequenceStore.literalsLength);
        output += literalsSize;

        int compressedSequencesSize = SequenceEncoder.compressSequences(outputBase, output, (int) (outputLimit - output), context.sequenceStore, parameters.getStrategy(), context.sequenceEncodingContext);

        int compressedSize = literalsSize + compressedSequencesSize;
        if (compressedSize == 0) {
            // not compressible
            return compressedSize;
        }

        // Check compressibility
        int maxCompressedSize = inputSize - calculateMinimumGain(inputSize, parameters.getStrategy());
        if (compressedSize > maxCompressedSize) {
            return 0; // not compressed
        }

        // confirm repeated offsets and entropy tables
        context.commit();

        return compressedSize;
    }

    private static int calculateMinimumGain(int inputSize, CompressionParameters.Strategy strategy)
    {
        // TODO: move this to Strategy to avoid hardcoding a specific strategy here
        int minLog = strategy == CompressionParameters.Strategy.BTULTRA ? 7 : 6;
        return (inputSize >>> minLog) + 2;
    }

    private static int rawLiterals(Object outputBase, long outputAddress, int outputSize, Object inputBase, long inputAddress, int inputSize)
    {
        int headerSize = 1;
        if (inputSize >= 32) {
            headerSize++;
        }
        if (inputSize >= 4096) {
            headerSize++;
        }

        checkArgument(inputSize + headerSize <= outputSize, "Output buffer too small");

        switch (headerSize) {
            case 1:
                UNSAFE.putByte(outputBase, outputAddress, (byte) (RAW_LITERALS_BLOCK | (inputSize << 3)));
                break;
            case 2:
                UNSAFE.putShort(outputBase, outputAddress, (short) (RAW_LITERALS_BLOCK | (1 << 2) | (inputSize << 4)));
                break;
            case 3:
                put24BitLittleEndian(outputBase, outputAddress, RAW_LITERALS_BLOCK | (3 << 2) | (inputSize << 4));
                break;
            default:
                throw new AssertionError();
        }

        // TODO: ensure this test is correct
        checkArgument(inputSize + 1 <= outputSize, "Output buffer too small");

        UNSAFE.copyMemory(inputBase, inputAddress, outputBase, outputAddress + headerSize, inputSize);

        return headerSize + inputSize;
    }
}
