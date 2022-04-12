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
package io.airlift.compress.lz4;

import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHashFactory;

import static io.airlift.compress.lz4.UnsafeUtil.UNSAFE;
import static java.lang.Math.toIntExact;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

/**
 * Implementation of <a href="https://github.com/lz4/lz4/blob/3e99d07bc09e1b82ee6191527bb3e555052c55ac/doc/lz4_Frame_format.md">LZ4 Frame format</a>.
 */
final class Lz4FrameRawCompressor
{
    private Lz4FrameRawCompressor() {}

    private static final byte[] MAGIC = {0x04, 0x22, 0x4D, 0x18};

    private static final int FRAME_DESCRIPTOR_SIZE =
            2 + // FLG byte, BD byte
                    8 + // content size
                    1; // HC (Header Checksum)

    private static final int FRAME_START_SIZE = 4 /* magic */ + FRAME_DESCRIPTOR_SIZE;
    private static final int FRAME_END_SIZE = 4 /* EndMark */;

    private static final int BLOCK_MAX_SIZE = 4 * 1024 * 1024;
    private static final int BLOCK_MAX_SIZE_MARKER = 7; // per "Block Maximum Size" spec, 7 means 4 MB

    public static int maxCompressedLength(int uncompressedSize)
    {
        return FRAME_START_SIZE + Lz4RawCompressor.maxCompressedLength(uncompressedSize) + FRAME_END_SIZE +
                // block sizes
                4 * (uncompressedSize / BLOCK_MAX_SIZE + 2);
    }

    public static int compress(
            Object inputBase,
            long inputAddress,
            int inputLength,
            Object outputBase,
            long outputAddress,
            int maxOutputLength,
            int[] table)
    {
        long originalOutputAddress = outputAddress;

        if (maxOutputLength < maxCompressedLength(inputLength)) {
            throw new IllegalArgumentException("Max output length must be larger than " + maxCompressedLength(inputLength));
        }

        UNSAFE.copyMemory(MAGIC, ARRAY_BYTE_BASE_OFFSET, outputBase, outputAddress, MAGIC.length);
        outputAddress += MAGIC.length;
        maxOutputLength -= MAGIC.length;

        byte[] frameDescriptor = new byte[FRAME_DESCRIPTOR_SIZE];

        // FLG byte
        frameDescriptor[0] =
                0b01 << 6 | // Version: 1
                        1 << 5 | // B.Indep: blocks are independent
                        0 << 4 | // B.Checksum: no checksum
                        1 << 3 | // C.Size: content size present
                        0 << 2 | // C.Checksum: no checksum
                        0 << 1 | // Reserved
                        0 << 0; // DictID: no dictionary

        // BD byte
        frameDescriptor[1] = (BLOCK_MAX_SIZE_MARKER << 4);

        // content size
        UNSAFE.putLong(frameDescriptor, ARRAY_BYTE_BASE_OFFSET + 2L, inputLength);

        // HC (Header Checksum)
        XXHash32 xxHash32 = XXHashFactory.fastestInstance().hash32();
        byte hc = (byte) ((xxHash32.hash(frameDescriptor, 0, frameDescriptor.length - 1, 0) >> 8) & 0xFF);
        frameDescriptor[frameDescriptor.length - 1] = hc;
        UNSAFE.copyMemory(frameDescriptor, ARRAY_BYTE_BASE_OFFSET, outputBase, outputAddress, frameDescriptor.length);
        outputAddress += frameDescriptor.length;
        maxOutputLength -= frameDescriptor.length;

        while (inputLength > 0) {
            int blockSize = Math.min(inputLength, BLOCK_MAX_SIZE);
            int blockHeaderSize = 4;
            int compressedSize = Lz4RawCompressor.compress(
                    inputBase,
                    inputAddress,
                    blockSize,
                    outputBase,
                    outputAddress + blockHeaderSize,
                    maxOutputLength - blockHeaderSize,
                    table);
            int uncompressed;
            if (compressedSize >= blockSize) {
                // incompressible data
                uncompressed = 1;
                compressedSize = blockSize;
                UNSAFE.copyMemory(inputBase, inputAddress, outputBase, outputAddress + blockHeaderSize, blockSize);
                UNSAFE.putInt(outputBase, outputAddress, (1 << 31) | blockSize);
            }
            else {
                // compressed data, already written to the output
                uncompressed = 0;
            }

            UNSAFE.putInt(outputBase, outputAddress, (uncompressed << 31) | compressedSize);
            outputAddress += blockHeaderSize + compressedSize;
            maxOutputLength -= blockHeaderSize + compressedSize;

            inputAddress += blockSize;
            inputLength -= blockSize;
        }

        // EndMark
        UNSAFE.putInt(outputBase, outputAddress, 0);
        outputAddress += 4;
        maxOutputLength -= 4;

        return toIntExact(outputAddress - originalOutputAddress);
    }
}
