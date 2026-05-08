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

import java.lang.foreign.MemorySegment;

import static io.airlift.compress.v3.zstdFFM.Constants.COMPRESSED_BLOCK;
import static io.airlift.compress.v3.zstdFFM.Constants.COMPRESSED_LITERALS_BLOCK;
import static io.airlift.compress.v3.zstdFFM.Constants.MAGIC_NUMBER;
import static io.airlift.compress.v3.zstdFFM.Constants.MIN_BLOCK_SIZE;
import static io.airlift.compress.v3.zstdFFM.Constants.MIN_WINDOW_LOG;
import static io.airlift.compress.v3.zstdFFM.Constants.RAW_BLOCK;
import static io.airlift.compress.v3.zstdFFM.Constants.RAW_LITERALS_BLOCK;
import static io.airlift.compress.v3.zstdFFM.Constants.RLE_LITERALS_BLOCK;
import static io.airlift.compress.v3.zstdFFM.Constants.SIZE_OF_BLOCK_HEADER;
import static io.airlift.compress.v3.zstdFFM.Constants.SIZE_OF_INT;
import static io.airlift.compress.v3.zstdFFM.Constants.SIZE_OF_SHORT;
import static io.airlift.compress.v3.zstdFFM.Constants.TREELESS_LITERALS_BLOCK;
import static io.airlift.compress.v3.zstdFFM.FfmUtil.JAVA_BYTE;
import static io.airlift.compress.v3.zstdFFM.FfmUtil.JAVA_INT;
import static io.airlift.compress.v3.zstdFFM.FfmUtil.JAVA_SHORT;
import static io.airlift.compress.v3.zstdFFM.Huffman.MAX_SYMBOL;
import static io.airlift.compress.v3.zstdFFM.Huffman.MAX_SYMBOL_COUNT;
import static io.airlift.compress.v3.zstdFFM.Util.checkArgument;
import static io.airlift.compress.v3.zstdFFM.Util.put24BitLittleEndian;

final class ZstdFrameCompressor
{
    static final int MAX_FRAME_HEADER_SIZE = 14;

    private static final int CHECKSUM_FLAG = 0b100;
    private static final int SINGLE_SEGMENT_FLAG = 0b100000;

    private static final int MINIMUM_LITERALS_SIZE = 63;

    // the maximum table log allowed for literal encoding per RFC 8478, section 4.2.1
    private static final int MAX_HUFFMAN_TABLE_LOG = 11;

    private ZstdFrameCompressor()
    {
    }

    // visible for testing
    static int writeMagic(final MemorySegment outputBase, final long outputAddress, final long outputLimit)
    {
        checkArgument(outputLimit - outputAddress >= SIZE_OF_INT, "Output buffer too small");

        outputBase.set(JAVA_INT, outputAddress, MAGIC_NUMBER);
        return SIZE_OF_INT;
    }

    // visible for testing
    static int writeFrameHeader(final MemorySegment outputBase, final long outputAddress, final long outputLimit, int inputSize, int windowSize)
    {
        checkArgument(outputLimit - outputAddress >= MAX_FRAME_HEADER_SIZE, "Output buffer too small");

        long output = outputAddress;

        int contentSizeDescriptor = 0;
        if (inputSize != -1) {
            contentSizeDescriptor = (inputSize >= 256 ? 1 : 0) + (inputSize >= 65536 + 256 ? 1 : 0);
        }
        int frameHeaderDescriptor = (contentSizeDescriptor << 6) | CHECKSUM_FLAG;

        boolean singleSegment = inputSize != -1 && windowSize >= inputSize;
        if (singleSegment) {
            frameHeaderDescriptor |= SINGLE_SEGMENT_FLAG;
        }

        outputBase.set(JAVA_BYTE, output, (byte) frameHeaderDescriptor);
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

            int mantissa = remainder / (base / 8);
            int encoded = ((exponent - MIN_WINDOW_LOG) << 3) | mantissa;

            outputBase.set(JAVA_BYTE, output, (byte) encoded);
            output++;
        }

        switch (contentSizeDescriptor) {
            case 0:
                if (singleSegment) {
                    outputBase.set(JAVA_BYTE, output++, (byte) inputSize);
                }
                break;
            case 1:
                outputBase.set(JAVA_SHORT, output, (short) (inputSize - 256));
                output += SIZE_OF_SHORT;
                break;
            case 2:
                outputBase.set(JAVA_INT, output, inputSize);
                output += SIZE_OF_INT;
                break;
            default:
                throw new AssertionError();
        }

        return (int) (output - outputAddress);
    }

    // visible for testing
    static int writeChecksum(MemorySegment outputBase, long outputAddress, long outputLimit, MemorySegment inputBase, long inputAddress, long inputLimit)
    {
        checkArgument(outputLimit - outputAddress >= SIZE_OF_INT, "Output buffer too small");

        int inputSize = (int) (inputLimit - inputAddress);

        long hash = XxHash64.hash(0, inputBase, inputAddress, inputSize);

        outputBase.set(JAVA_INT, outputAddress, (int) hash);

        return SIZE_OF_INT;
    }

    public static int compress(MemorySegment inputBase, long inputAddress, long inputLimit, MemorySegment outputBase, long outputAddress, long outputLimit, int compressionLevel)
    {
        int inputSize = (int) (inputLimit - inputAddress);

        CompressionParameters parameters = CompressionParameters.compute(compressionLevel, inputSize);

        long output = outputAddress;

        output += writeMagic(outputBase, output, outputLimit);
        output += writeFrameHeader(outputBase, output, outputLimit, inputSize, parameters.getWindowSize());
        output += compressFrame(inputBase, inputAddress, inputLimit, outputBase, output, outputLimit, parameters);
        output += writeChecksum(outputBase, output, outputLimit, inputBase, inputAddress, inputLimit);

        return (int) (output - outputAddress);
    }

    private static int compressFrame(MemorySegment inputBase, long inputAddress, long inputLimit, MemorySegment outputBase, long outputAddress, long outputLimit, CompressionParameters parameters)
    {
        int blockSize = parameters.getBlockSize();

        int outputSize = (int) (outputLimit - outputAddress);
        int remaining = (int) (inputLimit - inputAddress);

        long output = outputAddress;
        long input = inputAddress;

        CompressionContext context = new CompressionContext(parameters, inputAddress, remaining);
        do {
            checkArgument(outputSize >= SIZE_OF_BLOCK_HEADER + MIN_BLOCK_SIZE, "Output buffer too small");

            boolean lastBlock = blockSize >= remaining;
            blockSize = Math.min(blockSize, remaining);

            int compressedSize = writeCompressedBlock(inputBase, input, blockSize, outputBase, output, outputSize, context, lastBlock);

            input += blockSize;
            remaining -= blockSize;
            output += compressedSize;
            outputSize -= compressedSize;
        }
        while (remaining > 0);

        return (int) (output - outputAddress);
    }

    static int writeCompressedBlock(MemorySegment inputBase, long input, int blockSize, MemorySegment outputBase, long output, int outputSize, CompressionContext context, boolean lastBlock)
    {
        checkArgument(lastBlock || blockSize == context.parameters.getBlockSize(), "Only last block can be smaller than block size");

        int compressedSize = 0;
        if (blockSize > 0) {
            compressedSize = compressBlock(inputBase, input, blockSize, outputBase, output + SIZE_OF_BLOCK_HEADER, outputSize - SIZE_OF_BLOCK_HEADER, context);
        }

        if (compressedSize == 0) {
            checkArgument(blockSize + SIZE_OF_BLOCK_HEADER <= outputSize, "Output size too small");

            int blockHeader = (lastBlock ? 1 : 0) | (RAW_BLOCK << 1) | (blockSize << 3);
            put24BitLittleEndian(outputBase, output, blockHeader);
            MemorySegment.copy(inputBase, input, outputBase, output + SIZE_OF_BLOCK_HEADER, blockSize);
            compressedSize = SIZE_OF_BLOCK_HEADER + blockSize;
        }
        else {
            int blockHeader = (lastBlock ? 1 : 0) | (COMPRESSED_BLOCK << 1) | (compressedSize << 3);
            put24BitLittleEndian(outputBase, output, blockHeader);
            compressedSize += SIZE_OF_BLOCK_HEADER;
        }
        return compressedSize;
    }

    private static int compressBlock(MemorySegment inputBase, long inputAddress, int inputSize, MemorySegment outputBase, long outputAddress, int outputSize, CompressionContext context)
    {
        if (inputSize < MIN_BLOCK_SIZE + SIZE_OF_BLOCK_HEADER + 1) {
            return 0;
        }

        CompressionParameters parameters = context.parameters;
        context.blockCompressionState.enforceMaxDistance(inputAddress + inputSize, parameters.getWindowSize());
        context.sequenceStore.reset();

        int lastLiteralsSize = parameters.getStrategy()
                .getCompressor()
                .compressBlock(inputBase, inputAddress, inputSize, context.sequenceStore, context.blockCompressionState, context.offsets, parameters);

        long lastLiteralsAddress = inputAddress + inputSize - lastLiteralsSize;

        context.sequenceStore.appendLiterals(inputBase, lastLiteralsAddress, lastLiteralsSize);

        context.sequenceStore.generateCodes();

        long outputLimit = outputAddress + outputSize;
        long output = outputAddress;

        int compressedLiteralsSize = encodeLiterals(
                context.huffmanContext,
                parameters,
                outputBase,
                output,
                (int) (outputLimit - output),
                context.sequenceStore.literalsBuffer,
                context.sequenceStore.literalsLength);
        output += compressedLiteralsSize;

        int compressedSequencesSize = SequenceEncoder.compressSequences(outputBase, output, (int) (outputLimit - output), context.sequenceStore, parameters.getStrategy(), context.sequenceEncodingContext);

        int compressedSize = compressedLiteralsSize + compressedSequencesSize;
        if (compressedSize == 0) {
            return compressedSize;
        }

        int maxCompressedSize = inputSize - calculateMinimumGain(inputSize, parameters.getStrategy());
        if (compressedSize > maxCompressedSize) {
            return 0;
        }

        context.commit();

        return compressedSize;
    }

    private static int encodeLiterals(
            HuffmanCompressionContext context,
            CompressionParameters parameters,
            MemorySegment outputBase,
            long outputAddress,
            int outputSize,
            byte[] literals,
            int literalsSize)
    {
        boolean bypassCompression = (parameters.getStrategy() == CompressionParameters.Strategy.FAST) && (parameters.getTargetLength() > 0);
        if (bypassCompression || literalsSize <= MINIMUM_LITERALS_SIZE) {
            return rawLiterals(outputBase, outputAddress, outputSize, MemorySegment.ofArray(literals), 0, literalsSize);
        }

        int headerSize = 3 + (literalsSize >= 1024 ? 1 : 0) + (literalsSize >= 16384 ? 1 : 0);

        checkArgument(headerSize + 1 <= outputSize, "Output buffer too small");

        int[] counts = new int[MAX_SYMBOL_COUNT];
        Histogram.count(literals, literalsSize, counts);
        int maxSymbol = Histogram.findMaxSymbol(counts, MAX_SYMBOL);
        int largestCount = Histogram.findLargestCount(counts, maxSymbol);

        MemorySegment literalsBase = MemorySegment.ofArray(literals);
        long literalsAddress = 0;
        if (largestCount == literalsSize) {
            return rleLiterals(outputBase, outputAddress, outputSize, literalsBase, literalsAddress, literalsSize);
        }
        else if (largestCount <= (literalsSize >>> 7) + 4) {
            return rawLiterals(outputBase, outputAddress, outputSize, literalsBase, literalsAddress, literalsSize);
        }

        HuffmanCompressionTable previousTable = context.getPreviousTable();
        HuffmanCompressionTable table;
        int serializedTableSize;
        boolean reuseTable;

        boolean canReuse = previousTable.isValid(counts, maxSymbol);

        boolean preferReuse = parameters.getStrategy().ordinal() < CompressionParameters.Strategy.LAZY.ordinal() && literalsSize <= 1024;
        if (preferReuse && canReuse) {
            table = previousTable;
            reuseTable = true;
            serializedTableSize = 0;
        }
        else {
            HuffmanCompressionTable newTable = context.borrowTemporaryTable();

            newTable.initialize(
                    counts,
                    maxSymbol,
                    HuffmanCompressionTable.optimalNumberOfBits(MAX_HUFFMAN_TABLE_LOG, literalsSize, maxSymbol),
                    context.getCompressionTableWorkspace());

            serializedTableSize = newTable.write(outputBase, outputAddress + headerSize, outputSize - headerSize, context.getTableWriterWorkspace());

            if (canReuse && previousTable.estimateCompressedSize(counts, maxSymbol) <= serializedTableSize + newTable.estimateCompressedSize(counts, maxSymbol)) {
                table = previousTable;
                reuseTable = true;
                serializedTableSize = 0;
                context.discardTemporaryTable();
            }
            else {
                table = newTable;
                reuseTable = false;
            }
        }

        int compressedSize;
        boolean singleStream = literalsSize < 256;
        if (singleStream) {
            compressedSize = HuffmanCompressor.compressSingleStream(outputBase, outputAddress + headerSize + serializedTableSize, outputSize - headerSize - serializedTableSize, literalsBase, literalsAddress, literalsSize, table);
        }
        else {
            compressedSize = HuffmanCompressor.compress4streams(outputBase, outputAddress + headerSize + serializedTableSize, outputSize - headerSize - serializedTableSize, literalsBase, literalsAddress, literalsSize, table);
        }

        int totalSize = serializedTableSize + compressedSize;
        int minimumGain = calculateMinimumGain(literalsSize, parameters.getStrategy());

        if (compressedSize == 0 || totalSize >= literalsSize - minimumGain) {
            context.discardTemporaryTable();

            return rawLiterals(outputBase, outputAddress, outputSize, literalsBase, 0, literalsSize);
        }

        int encodingType = reuseTable ? TREELESS_LITERALS_BLOCK : COMPRESSED_LITERALS_BLOCK;

        switch (headerSize) {
            case 3: {
                int header = encodingType | ((singleStream ? 0 : 1) << 2) | (literalsSize << 4) | (totalSize << 14);
                put24BitLittleEndian(outputBase, outputAddress, header);
                break;
            }
            case 4: {
                int header = encodingType | (2 << 2) | (literalsSize << 4) | (totalSize << 18);
                outputBase.set(JAVA_INT, outputAddress, header);
                break;
            }
            case 5: {
                int header = encodingType | (3 << 2) | (literalsSize << 4) | (totalSize << 22);
                outputBase.set(JAVA_INT, outputAddress, header);
                outputBase.set(JAVA_BYTE, outputAddress + SIZE_OF_INT, (byte) (totalSize >>> 10));
                break;
            }
            default:
                throw new IllegalStateException();
        }

        return headerSize + totalSize;
    }

    private static int rleLiterals(MemorySegment outputBase, long outputAddress, int outputSize, MemorySegment inputBase, long inputAddress, int inputSize)
    {
        int headerSize = 1 + (inputSize > 31 ? 1 : 0) + (inputSize > 4095 ? 1 : 0);

        switch (headerSize) {
            case 1:
                outputBase.set(JAVA_BYTE, outputAddress, (byte) (RLE_LITERALS_BLOCK | (inputSize << 3)));
                break;
            case 2:
                outputBase.set(JAVA_SHORT, outputAddress, (short) (RLE_LITERALS_BLOCK | (1 << 2) | (inputSize << 4)));
                break;
            case 3:
                outputBase.set(JAVA_INT, outputAddress, RLE_LITERALS_BLOCK | 3 << 2 | inputSize << 4);
                break;
            default:
                throw new IllegalStateException();
        }

        outputBase.set(JAVA_BYTE, outputAddress + headerSize, inputBase.get(JAVA_BYTE, inputAddress));

        return headerSize + 1;
    }

    private static int calculateMinimumGain(int inputSize, CompressionParameters.Strategy strategy)
    {
        int minLog = strategy == CompressionParameters.Strategy.BTULTRA ? 7 : 6;
        return (inputSize >>> minLog) + 2;
    }

    private static int rawLiterals(MemorySegment outputBase, long outputAddress, int outputSize, MemorySegment inputBase, long inputAddress, int inputSize)
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
                outputBase.set(JAVA_BYTE, outputAddress, (byte) (RAW_LITERALS_BLOCK | (inputSize << 3)));
                break;
            case 2:
                outputBase.set(JAVA_SHORT, outputAddress, (short) (RAW_LITERALS_BLOCK | (1 << 2) | (inputSize << 4)));
                break;
            case 3:
                put24BitLittleEndian(outputBase, outputAddress, RAW_LITERALS_BLOCK | (3 << 2) | (inputSize << 4));
                break;
            default:
                throw new AssertionError();
        }

        checkArgument(inputSize + 1 <= outputSize, "Output buffer too small");

        MemorySegment.copy(inputBase, inputAddress, outputBase, outputAddress + headerSize, inputSize);

        return headerSize + inputSize;
    }
}
