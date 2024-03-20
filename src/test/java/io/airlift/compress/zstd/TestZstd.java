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

import com.google.common.io.Resources;
import io.airlift.compress.AbstractTestCompression;
import io.airlift.compress.Compressor;
import io.airlift.compress.Decompressor;
import io.airlift.compress.MalformedInputException;
import io.airlift.compress.TestingData;
import io.airlift.compress.benchmark.DataSet;
import io.airlift.compress.thirdparty.ZstdJniCompressor;
import io.airlift.compress.thirdparty.ZstdJniDecompressor;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestZstd
        extends AbstractTestCompression
{
    @Override
    protected Compressor getCompressor()
    {
        return new ZstdCompressor();
    }

    @Override
    protected Decompressor getDecompressor()
    {
        return new ZstdDecompressor();
    }

    @Override
    protected Compressor getVerifyCompressor()
    {
        return new ZstdJniCompressor(3);
    }

    @Override
    protected Decompressor getVerifyDecompressor()
    {
        return new ZstdJniDecompressor();
    }

    // Ideally, this should be covered by super.testDecompressWithOutputPadding(...), but the data written by the native
    // compressor doesn't include checksums, so it's not a comprehensive test. The dataset for this test has a checksum.
    @Test
    void testDecompressWithOutputPaddingAndChecksum()
            throws IOException
    {
        int padding = 1021;

        byte[] compressed = Resources.toByteArray(Resources.getResource("data/zstd/with-checksum.zst"));
        byte[] uncompressed = Resources.toByteArray(Resources.getResource("data/zstd/with-checksum"));

        byte[] output = new byte[uncompressed.length + padding * 2]; // pre + post padding
        int decompressedSize = getDecompressor().decompress(compressed, 0, compressed.length, output, padding, output.length - padding);

        assertByteArraysEqual(uncompressed, 0, uncompressed.length, output, padding, decompressedSize);
    }

    @Test
    void testConcatenatedFrames()
            throws IOException
    {
        byte[] compressed = Resources.toByteArray(Resources.getResource("data/zstd/multiple-frames.zst"));
        byte[] uncompressed = Resources.toByteArray(Resources.getResource("data/zstd/multiple-frames"));

        byte[] output = new byte[uncompressed.length];
        getDecompressor().decompress(compressed, 0, compressed.length, output, 0, output.length);

        assertByteArraysEqual(uncompressed, 0, uncompressed.length, output, 0, output.length);
    }

    @Test
    void testInvalidSequenceOffset()
            throws IOException
    {
        byte[] compressed = Resources.toByteArray(Resources.getResource("data/zstd/offset-before-start.zst"));
        byte[] output = new byte[compressed.length * 10];

        assertThatThrownBy(() -> getDecompressor().decompress(compressed, 0, compressed.length, output, 0, output.length))
                .isInstanceOf(MalformedInputException.class)
                .hasMessageStartingWith("Input is corrupted: offset=894");
    }

    @Test
    void testSmallLiteralsAfterIncompressibleLiterals()
            throws IOException
    {
        // Ensure the compressor doesn't try to reuse a huffman table that was created speculatively for a previous block
        // which ended up emitting raw literals due to insufficient gain
        Compressor compressor = getCompressor();

        byte[] original = Resources.toByteArray(Resources.getResource("data/zstd/small-literals-after-incompressible-literals"));
        int maxCompressLength = compressor.maxCompressedLength(original.length);

        byte[] compressed = new byte[maxCompressLength];
        int compressedSize = compressor.compress(original, 0, original.length, compressed, 0, compressed.length);

        byte[] decompressed = new byte[original.length];
        int decompressedSize = getDecompressor().decompress(compressed, 0, compressedSize, decompressed, 0, decompressed.length);

        assertByteArraysEqual(original, 0, original.length, decompressed, 0, decompressedSize);
    }

    @Test
    void testLargeRle()
            throws IOException
    {
        // Dataset that produces an RLE block with 3-byte header

        Compressor compressor = getCompressor();

        byte[] original = Resources.toByteArray(Resources.getResource("data/zstd/large-rle"));
        int maxCompressLength = compressor.maxCompressedLength(original.length);

        byte[] compressed = new byte[maxCompressLength];
        int compressedSize = compressor.compress(original, 0, original.length, compressed, 0, compressed.length);

        byte[] decompressed = new byte[original.length];
        int decompressedSize = getDecompressor().decompress(compressed, 0, compressedSize, decompressed, 0, decompressed.length);

        assertByteArraysEqual(original, 0, original.length, decompressed, 0, decompressedSize);
    }

    @Test
    void testIncompressibleData()
            throws IOException
    {
        // Incompressible data that would require more than maxCompressedLength(...) to store

        Compressor compressor = getCompressor();

        byte[] original = Resources.toByteArray(Resources.getResource("data/zstd/incompressible"));
        int maxCompressLength = compressor.maxCompressedLength(original.length);

        byte[] compressed = new byte[maxCompressLength];
        int compressedSize = compressor.compress(original, 0, original.length, compressed, 0, compressed.length);

        byte[] decompressed = new byte[original.length];
        int decompressedSize = getDecompressor().decompress(compressed, 0, compressedSize, decompressed, 0, decompressed.length);

        assertByteArraysEqual(original, 0, original.length, decompressed, 0, decompressedSize);
    }

    @Test
    void testMaxCompressedSize()
    {
        assertThat(new ZstdCompressor().maxCompressedLength(0)).isEqualTo(64);
        assertThat(new ZstdCompressor().maxCompressedLength(64 * 1024)).isEqualTo(65_824);
        assertThat(new ZstdCompressor().maxCompressedLength(128 * 1024)).isEqualTo(131_584);
        assertThat(new ZstdCompressor().maxCompressedLength(128 * 1024 + 1)).isEqualTo(131_585);
    }

    // test over data sets, should the result depend on input size or its compressibility
    @Test
    void testGetDecompressedSize()
    {
        for (DataSet dataSet : TestingData.DATA_SETS) {
            testGetDecompressedSize(dataSet);
        }
    }

    private void testGetDecompressedSize(DataSet dataSet)
    {
        Compressor compressor = getCompressor();
        byte[] originalUncompressed = dataSet.getUncompressed();
        byte[] compressed = new byte[compressor.maxCompressedLength(originalUncompressed.length)];

        int compressedLength = compressor.compress(originalUncompressed, 0, originalUncompressed.length, compressed, 0, compressed.length);

        assertThat(ZstdDecompressor.getDecompressedSize(compressed, 0, compressedLength)).isEqualTo(originalUncompressed.length);

        int padding = 10;
        byte[] compressedWithPadding = new byte[compressedLength + padding];
        Arrays.fill(compressedWithPadding, (byte) 42);
        System.arraycopy(compressed, 0, compressedWithPadding, padding, compressedLength);
        assertThat(ZstdDecompressor.getDecompressedSize(compressedWithPadding, padding, compressedLength)).isEqualTo(originalUncompressed.length);
    }

    @Test
    void testVerifyMagicInAllFrames()
            throws IOException
    {
        byte[] compressed = Resources.toByteArray(Resources.getResource("data/zstd/bad-second-frame.zst"));
        byte[] uncompressed = Resources.toByteArray(Resources.getResource("data/zstd/multiple-frames"));
        byte[] output = new byte[uncompressed.length];
        assertThatThrownBy(() -> getDecompressor().decompress(compressed, 0, compressed.length, output, 0, output.length))
                .isInstanceOf(MalformedInputException.class)
                .hasMessageStartingWith("Invalid magic prefix");
    }

    @Test
    void testDecompressIsMissingData()
    {
        byte[] input = new byte[]{40, -75, 47, -3, 32, 0, 1, 0};
        byte[] output = new byte[1024];
        assertThatThrownBy(() -> getDecompressor().decompress(input, 0, input.length, output, 0, output.length))
                .matches(e -> e instanceof MalformedInputException || e instanceof UncheckedIOException)
                .hasMessageContaining("Not enough input bytes");
    }

    @Test
    void testBadHuffmanData()
            throws IOException
    {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        // Magic
        buffer.write(new byte[] {
                (byte) 0b0010_1000,
                (byte) 0b1011_0101,
                (byte) 0b0010_1111,
                (byte) 0b1111_1101,
        });
        // Frame header
        buffer.write(0);
        buffer.write(0);
        // Block header COMPRESSED_BLOCK
        buffer.write(new byte[] {
                (byte) 0b1111_0100,
                (byte) 0b0000_0000,
                (byte) 0b0000_0000,
        });
        // Literals header
        buffer.write(new byte[] {
                // literalsBlockType COMPRESSED_LITERALS_BLOCK
                // + literals type
                0b0000_1010,
                // ... header remainder
                0b0000_0000,
                // compressedSize
                0b0011_1100,
                0b0000_0000,
        });
        // Huffman inputSize
        buffer.write(128);
        // weight value
        buffer.write(0b0001_0000);
        // Bad start values
        buffer.write(new byte[] {(byte) 255, (byte) 255});
        buffer.write(new byte[] {(byte) 255, (byte) 255});
        buffer.write(new byte[] {(byte) 255, (byte) 255});

        buffer.write(new byte[10]);

        byte[] data = buffer.toByteArray();

        assertThatThrownBy(() -> new ZstdDecompressor().decompress(data, 0, data.length, new byte[10], 0, 10))
                .isInstanceOf(MalformedInputException.class)
                .hasMessageStartingWith("Not enough input bytes");
    }
}
