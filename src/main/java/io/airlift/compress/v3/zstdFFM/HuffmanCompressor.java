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

import static io.airlift.compress.v3.zstdFFM.Constants.SIZE_OF_LONG;
import static io.airlift.compress.v3.zstdFFM.Constants.SIZE_OF_SHORT;
import static io.airlift.compress.v3.zstdFFM.FfmUtil.JAVA_BYTE;
import static io.airlift.compress.v3.zstdFFM.FfmUtil.JAVA_SHORT;

final class HuffmanCompressor
{
    private HuffmanCompressor()
    {
    }

    public static int compress4streams(MemorySegment outputBase, long outputAddress, int outputSize, MemorySegment inputBase, long inputAddress, int inputSize, HuffmanCompressionTable table)
    {
        long input = inputAddress;
        long inputLimit = inputAddress + inputSize;
        long output = outputAddress;
        long outputLimit = outputAddress + outputSize;

        int segmentSize = (inputSize + 3) / 4;

        if (outputSize < 6 + 1 + 1 + 1 + 8) {
            return 0;
        }

        if (inputSize <= 6 + 1 + 1 + 1) {
            return 0;
        }

        output += SIZE_OF_SHORT + SIZE_OF_SHORT + SIZE_OF_SHORT;

        int compressedSize;

        // first segment
        compressedSize = compressSingleStream(outputBase, output, (int) (outputLimit - output), inputBase, input, segmentSize, table);
        if (compressedSize == 0) {
            return 0;
        }
        outputBase.set(JAVA_SHORT, outputAddress, (short) compressedSize);
        output += compressedSize;
        input += segmentSize;

        // second segment
        compressedSize = compressSingleStream(outputBase, output, (int) (outputLimit - output), inputBase, input, segmentSize, table);
        if (compressedSize == 0) {
            return 0;
        }
        outputBase.set(JAVA_SHORT, outputAddress + SIZE_OF_SHORT, (short) compressedSize);
        output += compressedSize;
        input += segmentSize;

        // third segment
        compressedSize = compressSingleStream(outputBase, output, (int) (outputLimit - output), inputBase, input, segmentSize, table);
        if (compressedSize == 0) {
            return 0;
        }
        outputBase.set(JAVA_SHORT, outputAddress + SIZE_OF_SHORT + SIZE_OF_SHORT, (short) compressedSize);
        output += compressedSize;
        input += segmentSize;

        // fourth segment
        compressedSize = compressSingleStream(outputBase, output, (int) (outputLimit - output), inputBase, input, (int) (inputLimit - input), table);
        if (compressedSize == 0) {
            return 0;
        }
        output += compressedSize;

        return (int) (output - outputAddress);
    }

    public static int compressSingleStream(MemorySegment outputBase, long outputAddress, int outputSize, MemorySegment inputBase, long inputAddress, int inputSize, HuffmanCompressionTable table)
    {
        if (outputSize < SIZE_OF_LONG) {
            return 0;
        }

        BitOutputStream bitstream = new BitOutputStream(outputBase, outputAddress, outputSize);
        long input = inputAddress;

        int n = inputSize & ~3;

        switch (inputSize & 3) {
            case 3:
                table.encodeSymbol(bitstream, inputBase.get(JAVA_BYTE, input + n + 2) & 0xFF);
                if (SIZE_OF_LONG * 8 < Huffman.MAX_TABLE_LOG * 4 + 7) {
                    bitstream.flush();
                }
                // fall-through
            case 2:
                table.encodeSymbol(bitstream, inputBase.get(JAVA_BYTE, input + n + 1) & 0xFF);
                if (SIZE_OF_LONG * 8 < Huffman.MAX_TABLE_LOG * 2 + 7) {
                    bitstream.flush();
                }
                // fall-through
            case 1:
                table.encodeSymbol(bitstream, inputBase.get(JAVA_BYTE, input + n + 0) & 0xFF);
                bitstream.flush();
                // fall-through
            case 0: /* fall-through */
            default:
                break;
        }

        for (; n > 0; n -= 4) {
            table.encodeSymbol(bitstream, inputBase.get(JAVA_BYTE, input + n - 1) & 0xFF);
            if (SIZE_OF_LONG * 8 < Huffman.MAX_TABLE_LOG * 2 + 7) {
                bitstream.flush();
            }
            table.encodeSymbol(bitstream, inputBase.get(JAVA_BYTE, input + n - 2) & 0xFF);
            if (SIZE_OF_LONG * 8 < Huffman.MAX_TABLE_LOG * 4 + 7) {
                bitstream.flush();
            }
            table.encodeSymbol(bitstream, inputBase.get(JAVA_BYTE, input + n - 3) & 0xFF);
            if (SIZE_OF_LONG * 8 < Huffman.MAX_TABLE_LOG * 2 + 7) {
                bitstream.flush();
            }
            table.encodeSymbol(bitstream, inputBase.get(JAVA_BYTE, input + n - 4) & 0xFF);
            bitstream.flush();
        }

        return bitstream.close();
    }
}
