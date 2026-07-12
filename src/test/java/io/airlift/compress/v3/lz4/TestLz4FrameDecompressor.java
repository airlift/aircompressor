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

import com.google.common.primitives.Bytes;
import io.airlift.compress.v3.MalformedInputException;
import io.airlift.compress.v3.xxhash.XxHash32Hasher;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;

import static io.airlift.compress.v3.lz4.Lz4FrameFormat.FLG_BLOCK_CHECKSUM;
import static io.airlift.compress.v3.lz4.Lz4FrameFormat.FLG_BLOCK_INDEPENDENCE;
import static io.airlift.compress.v3.lz4.Lz4FrameFormat.FLG_CONTENT_CHECKSUM;
import static io.airlift.compress.v3.lz4.Lz4FrameFormat.FLG_CONTENT_SIZE;
import static io.airlift.compress.v3.lz4.Lz4FrameFormat.FLG_DICTIONARY_ID;
import static io.airlift.compress.v3.lz4.Lz4FrameFormat.FLG_RESERVED_MASK;
import static io.airlift.compress.v3.lz4.Lz4FrameFormat.FLG_VERSION;
import static io.airlift.compress.v3.lz4.Lz4FrameFormat.MAGIC;
import static io.airlift.compress.v3.lz4.Lz4FrameFormat.SKIPPABLE_MAGIC;
import static io.airlift.compress.v3.lz4.Lz4FrameFormat.UNCOMPRESSED_BLOCK_FLAG;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the frame parsing and integrity validation of the LZ4 frame decompressor against
 * hand-crafted frames that exercise features (concatenated and skippable frames, linked blocks,
 * dictionaries, content size, block and content checksums) that the frame compressors do not emit
 * themselves.
 */
class TestLz4FrameDecompressor
{
    private static final byte[] CONTENT = "the quick brown fox jumps over the lazy dog".getBytes(UTF_8);

    private static byte[] decompress(byte[] frame, int outputSize)
    {
        byte[] output = new byte[outputSize];
        int size = new Lz4FrameJavaDecompressor().decompress(frame, 0, frame.length, output, 0, output.length);
        return Arrays.copyOf(output, size);
    }

    private static byte[] decompress(byte[] frame)
    {
        return decompress(frame, CONTENT.length);
    }

    @Test
    void testValidContentChecksum()
    {
        byte[] frame = frame(FLG_BLOCK_INDEPENDENCE | FLG_CONTENT_CHECKSUM).build();
        assertThat(decompress(frame)).isEqualTo(CONTENT);
    }

    @Test
    void testInvalidContentChecksumIsRejected()
    {
        byte[] frame = frame(FLG_BLOCK_INDEPENDENCE | FLG_CONTENT_CHECKSUM).contentChecksum(xxHash32(CONTENT) ^ 0x1).build();
        assertThatThrownBy(() -> decompress(frame))
                .isInstanceOf(MalformedInputException.class)
                .hasMessageContaining("invalid content checksum");
    }

    @Test
    void testValidBlockChecksum()
    {
        byte[] frame = frame(FLG_BLOCK_INDEPENDENCE | FLG_BLOCK_CHECKSUM).build();
        assertThat(decompress(frame)).isEqualTo(CONTENT);
    }

    @Test
    void testInvalidBlockChecksumIsRejected()
    {
        byte[] frame = frame(FLG_BLOCK_INDEPENDENCE | FLG_BLOCK_CHECKSUM).blockChecksum(xxHash32(CONTENT) ^ 0x1).build();
        assertThatThrownBy(() -> decompress(frame))
                .isInstanceOf(MalformedInputException.class)
                .hasMessageContaining("invalid block checksum");
    }

    @Test
    void testValidContentSize()
    {
        byte[] frame = frame(FLG_BLOCK_INDEPENDENCE | FLG_CONTENT_SIZE).contentSize(CONTENT.length).build();
        assertThat(decompress(frame)).isEqualTo(CONTENT);
    }

    @Test
    void testMismatchedContentSizeIsRejected()
    {
        byte[] frame = frame(FLG_BLOCK_INDEPENDENCE | FLG_CONTENT_SIZE).contentSize(CONTENT.length + 1).build();
        assertThatThrownBy(() -> decompress(frame))
                .isInstanceOf(MalformedInputException.class)
                .hasMessageContaining("content size does not match");
    }

    @Test
    void testLinkedBlocksAreRejected()
    {
        // block independence flag not set
        byte[] frame = frame(0).build();
        assertThatThrownBy(() -> decompress(frame))
                .isInstanceOf(MalformedInputException.class)
                .hasMessageContaining("linked blocks are not supported");
    }

    @Test
    void testDictionaryIsRejected()
    {
        byte[] frame = frame(FLG_BLOCK_INDEPENDENCE | FLG_DICTIONARY_ID).build();
        assertThatThrownBy(() -> decompress(frame))
                .isInstanceOf(MalformedInputException.class)
                .hasMessageContaining("dictionary are not supported");
    }

    @Test
    void testReservedFlgBitIsRejected()
    {
        byte[] frame = frame(FLG_BLOCK_INDEPENDENCE | FLG_RESERVED_MASK).build();
        assertThatThrownBy(() -> decompress(frame))
                .isInstanceOf(MalformedInputException.class)
                .hasMessageContaining("reserved bits");
    }

    @Test
    void testReservedBdBitIsRejected()
    {
        byte[] frame = frame(FLG_BLOCK_INDEPENDENCE).blockDescriptor(0x71).build();
        assertThatThrownBy(() -> decompress(frame))
                .isInstanceOf(MalformedInputException.class)
                .hasMessageContaining("reserved bits");
    }

    @Test
    void testInvalidMagicIsRejected()
    {
        byte[] frame = frame(FLG_BLOCK_INDEPENDENCE).build();
        frame[0] ^= 0xFF;
        assertThatThrownBy(() -> decompress(frame))
                .isInstanceOf(MalformedInputException.class)
                .hasMessageContaining("magic number");
    }

    @Test
    void testUnsupportedVersionIsRejected()
    {
        byte[] frame = frame(FLG_BLOCK_INDEPENDENCE).build();
        // clear the version bits (bits 7-6) of the FLG byte, which follows the 4 byte magic
        frame[4] &= 0x3F;
        assertThatThrownBy(() -> decompress(frame))
                .isInstanceOf(MalformedInputException.class)
                .hasMessageContaining("Unsupported LZ4 frame version");
    }

    @Test
    void testInvalidBlockMaximumSizeIsRejected()
    {
        // block maximum size ids 0-3 are invalid; only 4-7 are defined
        byte[] frame = frame(FLG_BLOCK_INDEPENDENCE).blockDescriptor(0x10).build();
        assertThatThrownBy(() -> decompress(frame))
                .isInstanceOf(MalformedInputException.class)
                .hasMessageContaining("block maximum size");
    }

    @Test
    void testInvalidHeaderChecksumIsRejected()
    {
        byte[] frame = frame(FLG_BLOCK_INDEPENDENCE).build();
        // the header checksum byte follows the 4 byte magic and the 2 byte descriptor
        frame[6] ^= 0xFF;
        assertThatThrownBy(() -> decompress(frame))
                .isInstanceOf(MalformedInputException.class)
                .hasMessageContaining("invalid header checksum");
    }

    @Test
    void testConcatenatedFramesAreAllDecoded()
    {
        byte[] frame = frame(FLG_BLOCK_INDEPENDENCE | FLG_CONTENT_CHECKSUM).build();
        byte[] concatenated = Bytes.concat(frame, frame, frame);

        assertThat(decompress(concatenated, CONTENT.length * 3))
                .isEqualTo(Bytes.concat(CONTENT, CONTENT, CONTENT));
    }

    @Test
    void testSkippableFramesAreIgnored()
    {
        byte[] frame = frame(FLG_BLOCK_INDEPENDENCE).build();
        byte[] skippable = skippableFrame("ignored metadata".getBytes(UTF_8));

        // before, between and after the content frames
        byte[] concatenated = Bytes.concat(skippable, frame, skippable, frame, skippable);

        assertThat(decompress(concatenated, CONTENT.length * 2))
                .isEqualTo(Bytes.concat(CONTENT, CONTENT));
    }

    @Test
    void testEmptySkippableFrameIsIgnored()
    {
        byte[] concatenated = Bytes.concat(skippableFrame(new byte[0]), frame(FLG_BLOCK_INDEPENDENCE).build());
        assertThat(decompress(concatenated)).isEqualTo(CONTENT);
    }

    @Test
    void testTruncatedSkippableFrameIsRejected()
    {
        byte[] skippable = skippableFrame("ignored metadata".getBytes(UTF_8));
        byte[] truncated = Arrays.copyOf(skippable, skippable.length - 1);
        byte[] concatenated = Bytes.concat(frame(FLG_BLOCK_INDEPENDENCE).build(), truncated);

        assertThatThrownBy(() -> decompress(concatenated))
                .isInstanceOf(MalformedInputException.class)
                .hasMessageContaining("Truncated LZ4 skippable frame");
    }

    @Test
    void testTrailingGarbageIsRejected()
    {
        byte[] concatenated = Bytes.concat(frame(FLG_BLOCK_INDEPENDENCE).build(), new byte[] {1, 2, 3, 4, 5});
        assertThatThrownBy(() -> decompress(concatenated))
                .isInstanceOf(MalformedInputException.class)
                .hasMessageContaining("magic number");
    }

    private static byte[] skippableFrame(byte[] content)
    {
        ByteArrayOutputStream frame = new ByteArrayOutputStream();
        writeInt(frame, SKIPPABLE_MAGIC);
        writeInt(frame, content.length);
        frame.write(content, 0, content.length);
        return frame.toByteArray();
    }

    private static FrameBuilder frame(int flags)
    {
        return new FrameBuilder(flags);
    }

    private static int xxHash32(byte[] data)
    {
        return XxHash32Hasher.hash(data);
    }

    private static void writeInt(ByteArrayOutputStream out, int value)
    {
        for (int i = 0; i < Integer.BYTES; i++) {
            out.write((value >>> (8 * i)) & 0xFF);
        }
    }

    private static void writeLong(ByteArrayOutputStream out, long value)
    {
        for (int i = 0; i < Long.BYTES; i++) {
            out.write((int) ((value >>> (8 * i)) & 0xFF));
        }
    }

    /**
     * Builds a single-block LZ4 frame holding {@link #CONTENT} as an uncompressed block, with a
     * valid header checksum and, by default, valid optional fields for whichever flags are set.
     * Individual fields can be overridden to exercise rejection paths.
     */
    private static final class FrameBuilder
    {
        private final int flg;
        private int bd = 0x70; // 4 MB block maximum size
        private long contentSize = CONTENT.length;
        private Integer blockChecksum;
        private Integer contentChecksum;

        private FrameBuilder(int flags)
        {
            this.flg = FLG_VERSION | flags;
        }

        FrameBuilder blockDescriptor(int bd)
        {
            this.bd = bd;
            return this;
        }

        FrameBuilder contentSize(long contentSize)
        {
            this.contentSize = contentSize;
            return this;
        }

        FrameBuilder blockChecksum(int blockChecksum)
        {
            this.blockChecksum = blockChecksum;
            return this;
        }

        FrameBuilder contentChecksum(int contentChecksum)
        {
            this.contentChecksum = contentChecksum;
            return this;
        }

        byte[] build()
        {
            // frame descriptor bytes (FLG, BD, optional content size and dictionary id),
            // over which the header checksum is computed
            ByteArrayOutputStream descriptor = new ByteArrayOutputStream();
            descriptor.write(flg);
            descriptor.write(bd);
            if ((flg & FLG_CONTENT_SIZE) != 0) {
                writeLong(descriptor, contentSize);
            }
            if ((flg & FLG_DICTIONARY_ID) != 0) {
                writeInt(descriptor, 0xCAFEBABE);
            }
            byte[] descriptorBytes = descriptor.toByteArray();
            int headerChecksum = (xxHash32(descriptorBytes) >>> 8) & 0xFF;

            ByteArrayOutputStream frame = new ByteArrayOutputStream();
            writeInt(frame, MAGIC);
            frame.write(descriptorBytes, 0, descriptorBytes.length);
            frame.write(headerChecksum);

            // single uncompressed block
            writeInt(frame, CONTENT.length | UNCOMPRESSED_BLOCK_FLAG);
            frame.write(CONTENT, 0, CONTENT.length);
            if ((flg & FLG_BLOCK_CHECKSUM) != 0) {
                writeInt(frame, blockChecksum != null ? blockChecksum : xxHash32(CONTENT));
            }

            writeInt(frame, 0); // end mark
            if ((flg & FLG_CONTENT_CHECKSUM) != 0) {
                writeInt(frame, contentChecksum != null ? contentChecksum : xxHash32(CONTENT));
            }
            return frame.toByteArray();
        }
    }
}
