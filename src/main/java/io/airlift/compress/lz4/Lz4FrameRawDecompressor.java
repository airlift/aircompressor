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

import static io.airlift.compress.lz4.UnsafeUtil.UNSAFE;
import static java.lang.Math.toIntExact;

final class Lz4FrameRawDecompressor
{
    private Lz4FrameRawDecompressor() {}

    private static final int SUPPORTED_FLAGS = 0b01101000;
    private static final int SUPPORTED_BD = 0b01110000;

    static int decompress(
            Object inputBase,
            long inputAddress,
            int inputLimit,
            Object outputBase,
            long outputAddress,
            int outputLimit)
    {
        long originalOutputAddress = outputAddress;

        checkArgument(inputLimit >= 6, "Not enough input bytes");
        checkArgument(UNSAFE.getInt(inputBase, inputAddress) == 0x184D2204, "Invalid magic number");
        inputAddress += 4;
        inputLimit -= 4;

        byte flags = UNSAFE.getByte(inputBase, inputAddress);
        byte bd = UNSAFE.getByte(inputBase, inputAddress + 1);
        inputAddress += 2;
        inputLimit -= 2;

        checkArgument(getVersion(flags) == 1, "Unsupported version");
        long contentSize = -1;
        if (hasContentSize(flags)) {
            contentSize = UNSAFE.getLong(inputBase, inputAddress);
            inputAddress += 8;
            inputLimit -= 8;
            checkArgument(contentSize <= outputLimit, "Output buffer too small");
        }

        checkArgument(inputLimit >= 1, "Not enough input bytes");
        byte hc = UNSAFE.getByte(inputBase, inputAddress); // header checksum
        inputAddress += 1;
        inputLimit -= 1;

        checkArgument((flags & ~SUPPORTED_FLAGS) == 0 && areBlocksIndependent(flags), "Unsupported flags");
        checkArgument((bd & ~SUPPORTED_BD) == 0, "Invalid BD byte");

        while (true) {
            checkArgument(inputLimit >= 4, "Not enough input bytes");
            int blockSize = UNSAFE.getInt(inputBase, inputAddress);
            inputAddress += 4;
            inputLimit -= 4;

            if (blockSize == 0) {
                // EndMark
                break;
            }

            if ((blockSize & (1 << 31)) != 0) {
                // uncompressed
                blockSize = blockSize & Integer.MAX_VALUE;
                checkArgument(inputLimit >= blockSize, "Not enough input bytes");
                checkArgument(outputLimit >= blockSize, "Output buffer too small");
                UNSAFE.copyMemory(inputBase, inputAddress, outputBase, outputAddress, blockSize);
                inputAddress += blockSize;
                inputLimit -= blockSize;
                outputAddress += blockSize;
                outputLimit -= blockSize;
            }
            else {
                // compressed
                checkArgument(inputLimit >= blockSize, "Not enough input bytes");
                int decompressed = Lz4RawDecompressor.decompress(
                        inputBase,
                        inputAddress,
                        inputAddress + blockSize,
                        outputBase,
                        outputAddress,
                        outputAddress + outputLimit);
                inputAddress += blockSize;
                inputLimit -= blockSize;
                outputAddress += decompressed;
                outputLimit -= decompressed;
            }
        }

        checkArgument(inputLimit == 0, "Some input not consumed");
        int decompressed = toIntExact(outputAddress - originalOutputAddress);
        checkArgument(contentSize == -1 || decompressed == contentSize, "Decompressed wrong number of bytes");
        return decompressed;
    }

    public static long getDecompressedSize(Object inputBase, long inputAddress, int inputLimit)
    {
        checkArgument(inputLimit >= 6, "Not enough input bytes");
        checkArgument(UNSAFE.getInt(inputBase, inputAddress) == 0x184D2204, "Invalid magic number");
        inputAddress += 4;
        inputLimit -= 4;

        byte flags = UNSAFE.getByte(inputBase, inputAddress);
        // BD byte not read
        inputAddress += 2;
        inputLimit -= 2;
        checkArgument(hasContentSize(flags), "Content size (C.Size) not present");

        checkArgument(inputLimit >= 8, "Not enough input bytes");
        return UNSAFE.getLong(inputBase, inputAddress);
    }

    private static int getVersion(byte flags)
    {
        return flags >> 6;
    }

    private static boolean hasContentSize(byte flags)
    {
        return (flags & (1 << 3)) != 0;
    }

    private static boolean areBlocksIndependent(byte flags)
    {
        return (flags & (1 << 5)) != 0;
    }

    private static void checkArgument(boolean condition, String message)
    {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
