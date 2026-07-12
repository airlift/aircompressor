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

/**
 * Constants describing the LZ4 frame format.
 * See <a href="https://github.com/lz4/lz4/blob/dev/doc/lz4_Frame_format.md">lz4 frame format specification</a>.
 */
final class Lz4FrameFormat
{
    private Lz4FrameFormat() {}

    // Frame magic number, stored little-endian
    public static final int MAGIC = 0x184D2204;

    // Skippable frames use a magic number of 0x184D2A5X, followed by a 4 byte frame size
    public static final int SKIPPABLE_MAGIC = 0x184D2A50;
    public static final int SKIPPABLE_MAGIC_MASK = 0xFFFF_FFF0;

    // Frame descriptor FLG byte: version number is always 01 in bits 7-6
    public static final int FLG_VERSION = 0b01 << 6;
    public static final int FLG_BLOCK_INDEPENDENCE = 1 << 5;
    public static final int FLG_BLOCK_CHECKSUM = 1 << 4;
    public static final int FLG_CONTENT_SIZE = 1 << 3;
    public static final int FLG_CONTENT_CHECKSUM = 1 << 2;
    public static final int FLG_DICTIONARY_ID = 1;

    // Reserved bits, which the specification requires to be zero: FLG bit 1, and BD bit 7 and bits 3-0
    public static final int FLG_RESERVED_MASK = 0b0000_0010;
    public static final int BD_RESERVED_MASK = 0b1000_1111;

    // Frame descriptor BD byte: block maximum size id (4MB) is stored in bits 6-4
    public static final int BD_4MB = 7 << 4;
    public static final int BLOCK_MAX_SIZE_4MB = 4 * 1024 * 1024;

    // magic number (4 bytes) + FLG (1 byte) + BD (1 byte) + header checksum (1 byte)
    public static final int HEADER_SIZE = 7;
    // end mark is a block size of zero (4 bytes)
    public static final int END_MARK_SIZE = 4;

    // Highest bit of the block size indicates that the block is stored uncompressed
    public static final int UNCOMPRESSED_BLOCK_FLAG = 0x8000_0000;
    public static final int BLOCK_SIZE_MASK = 0x7FFF_FFFF;

    /**
     * Maximum size of a data block for the given block maximum size id (4-7), or -1 if the id is invalid.
     */
    public static int blockMaximumSize(int blockSizeId)
    {
        return switch (blockSizeId) {
            case 4 -> 64 * 1024;
            case 5 -> 256 * 1024;
            case 6 -> 1024 * 1024;
            case 7 -> 4 * 1024 * 1024;
            default -> -1;
        };
    }
}
