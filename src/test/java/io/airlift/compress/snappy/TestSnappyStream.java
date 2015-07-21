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
package io.airlift.compress.snappy;

import com.google.common.base.Charsets;
import io.airlift.compress.TestingModule;
import io.airlift.compress.benchmark.DataSet;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;

import static com.google.common.io.ByteStreams.toByteArray;
import static com.google.common.primitives.UnsignedBytes.toInt;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Tests the functionality of {@link org.iq80.snappy.SnappyFramedInputStream}
 * and {@link org.iq80.snappy.SnappyFramedOutputStream}.
 */
public class TestSnappyStream
{
    protected static byte[] getRandom(double compressionRatio, int length)
    {
        RandomGenerator gen = new RandomGenerator(compressionRatio);
        gen.getNextPosition(length);
        byte[] random = Arrays.copyOf(gen.data, length);
        assertEquals(random.length, length);
        return random;
    }

    protected byte[] getMarkerFrame()
    {
        return SnappyFramed.HEADER_BYTES;
    }

    @Test
    public void testSimple()
            throws Exception
    {
        byte[] original = "aaaaaaaaaaaabbbbbbbaaaaaa".getBytes(Charsets.UTF_8);

        byte[] compressed = compress(original);
        byte[] uncompressed = uncompress(compressed);

        assertEquals(uncompressed, original);
        // 10 byte stream header, 4 byte block header, 4 byte crc, 19 bytes
        assertEquals(compressed.length, 37);

        // stream header
        Assert.assertEquals(Arrays.copyOf(compressed, 10), SnappyFramed.HEADER_BYTES);

        // flag: compressed
        Assert.assertEquals(toInt(compressed[10]), SnappyFramed.COMPRESSED_DATA_FLAG);

        // length: 23 = 0x000017
        assertEquals(toInt(compressed[11]), 0x17);
        assertEquals(toInt(compressed[12]), 0x00);
        assertEquals(toInt(compressed[13]), 0x00);

        // crc32c: 0x9274cda8
        assertEquals(toInt(compressed[17]), 0x92);
        assertEquals(toInt(compressed[16]), 0x74);
        assertEquals(toInt(compressed[15]), 0xCD);
        assertEquals(toInt(compressed[14]), 0xA8);
    }

    @Test
    public void testUncompressible()
            throws Exception
    {
        byte[] random = getRandom(1, 5000);

        byte[] compressed = compress(random);
        byte[] uncompressed = uncompress(compressed);

        assertEquals(uncompressed, random);
        assertEquals(compressed.length, random.length + 10 + 4 + 4);

        // flag: uncompressed
        Assert.assertEquals(toInt(compressed[10]), SnappyFramed.UNCOMPRESSED_DATA_FLAG);

        // length: 5004 = 0x138c
        assertEquals(toInt(compressed[13]), 0x00);
        assertEquals(toInt(compressed[12]), 0x13);
        assertEquals(toInt(compressed[11]), 0x8c);
    }

    @Test
    public void testEmptyCompression()
            throws Exception
    {
        byte[] empty = new byte[0];
        Assert.assertEquals(compress(empty), SnappyFramed.HEADER_BYTES);
        assertEquals(uncompress(SnappyFramed.HEADER_BYTES), empty);
    }

    @Test(expectedExceptions = EOFException.class, expectedExceptionsMessageRegExp = ".*block header.*")
    public void testShortBlockHeader()
            throws Exception
    {
        uncompressBlock(new byte[] {0});
    }

    @Test(expectedExceptions = EOFException.class, expectedExceptionsMessageRegExp = ".*reading frame.*")
    public void testShortBlockData()
            throws Exception
    {
        // flag = 0, size = 8, crc32c = 0, block data= [x, x]
        uncompressBlock(new byte[] {1, 8, 0, 0, 0, 0, 0, 0, 'x', 'x'});
    }

    @Test
    public void testUnskippableChunkFlags()
            throws Exception
    {
        for (int i = 2; i <= 0x7f; i++) {
            try {
                uncompressBlock(new byte[] {(byte) i, 5, 0, 0, 0, 0, 0, 0, 0});
                fail("no exception thrown with flag: " + Integer.toHexString(i));
            }
            catch (IOException expected) {
            }
        }
    }

    @Test
    public void testSkippableChunkFlags()
            throws Exception
    {
        for (int i = 0x80; i <= 0xfe; i++) {
            try {
                uncompressBlock(new byte[] {(byte) i, 5, 0, 0, 0, 0, 0, 0, 0});
            }
            catch (IOException e) {
                fail("exception thrown with flag: " + Integer.toHexString(i), e);
            }
        }
    }

    @Test(expectedExceptions = IOException.class, expectedExceptionsMessageRegExp = "invalid length.*4.*")
    public void testInvalidBlockSizeZero()
            throws Exception
    {
        // flag = '0', block size = 4, crc32c = 0
        uncompressBlock(new byte[] {1, 4, 0, 0, 0, 0, 0, 0});
    }

    @Test(expectedExceptions = IOException.class, expectedExceptionsMessageRegExp = "Corrupt input: invalid checksum")
    public void testInvalidChecksum()
            throws Exception
    {
        // flag = 0, size = 5, crc32c = 0, block data = [a]
        uncompressBlock(new byte[] {1, 5, 0, 0, 0, 0, 0, 0, 'a'});
    }

    @Test
    public void testInvalidChecksumIgnoredWhenVerificationDisabled()
            throws Exception
    {
        // flag = 0, size = 4, crc32c = 0, block data = [a]
        byte[] block = {1, 5, 0, 0, 0, 0, 0, 0, 'a'};
        ByteArrayInputStream inputData = new ByteArrayInputStream(blockToStream(block));
        assertEquals(toByteArray(new SnappyFramedInputStream(inputData, false)), new byte[] {'a'});
    }

    @Test
    public void testLargerFrames_raw_()
            throws IOException
    {
        byte[] random = getRandom(0.5, 100000);

        byte[] stream = new byte[SnappyFramed.HEADER_BYTES.length + 8 + random.length];
        System.arraycopy(SnappyFramed.HEADER_BYTES, 0, stream, 0, SnappyFramed.HEADER_BYTES.length);

        stream[10] = SnappyFramed.UNCOMPRESSED_DATA_FLAG;

        int length = random.length + 4;
        stream[11] = (byte) length;
        stream[12] = (byte) (length >>> 8);
        stream[13] = (byte) (length >>> 16);

        int crc32c = Crc32C.maskedCrc32c(random);
        stream[14] = (byte) crc32c;
        stream[15] = (byte) (crc32c >>> 8);
        stream[16] = (byte) (crc32c >>> 16);
        stream[17] = (byte) (crc32c >>> 24);

        System.arraycopy(random, 0, stream, 18, random.length);

        byte[] uncompressed = uncompress(stream);

        assertEquals(random, uncompressed);
    }

    @Test
    public void testLargerFrames_compressed_()
            throws IOException
    {
        byte[] random = getRandom(0.5, 500000);

        byte[] compressed = blockCompress(random);

        byte[] stream = new byte[SnappyFramed.HEADER_BYTES.length + 8 + compressed.length];
        System.arraycopy(SnappyFramed.HEADER_BYTES, 0, stream, 0, SnappyFramed.HEADER_BYTES.length);

        stream[10] = SnappyFramed.COMPRESSED_DATA_FLAG;

        int length = compressed.length + 4;
        stream[11] = (byte) length;
        stream[12] = (byte) (length >>> 8);
        stream[13] = (byte) (length >>> 16);

        int crc32c = Crc32C.maskedCrc32c(random);
        stream[14] = (byte) crc32c;
        stream[15] = (byte) (crc32c >>> 8);
        stream[16] = (byte) (crc32c >>> 16);
        stream[17] = (byte) (crc32c >>> 24);

        System.arraycopy(compressed, 0, stream, 18, compressed.length);

        byte[] uncompressed = uncompress(stream);

        assertEquals(random, uncompressed);
    }

    @Test
    public void testLargerFrames_compressed_smaller_raw_larger()
            throws IOException
    {
        byte[] random = getRandom(0.5, 100000);

        byte[] compressed = blockCompress(random);

        byte[] stream = new byte[SnappyFramed.HEADER_BYTES.length + 8 + compressed.length];
        System.arraycopy(SnappyFramed.HEADER_BYTES, 0, stream, 0, SnappyFramed.HEADER_BYTES.length);

        stream[10] = SnappyFramed.COMPRESSED_DATA_FLAG;

        int length = compressed.length + 4;
        stream[11] = (byte) length;
        stream[12] = (byte) (length >>> 8);
        stream[13] = (byte) (length >>> 16);

        int crc32c = Crc32C.maskedCrc32c(random);
        stream[14] = (byte) crc32c;
        stream[15] = (byte) (crc32c >>> 8);
        stream[16] = (byte) (crc32c >>> 16);
        stream[17] = (byte) (crc32c >>> 24);

        System.arraycopy(compressed, 0, stream, 18, compressed.length);

        byte[] uncompressed = uncompress(stream);

        assertEquals(random, uncompressed);
    }

    private byte[] uncompressBlock(byte[] block)
            throws IOException
    {
        return uncompress(blockToStream(block));
    }

    private static byte[] blockToStream(byte[] block)
    {
        byte[] stream = new byte[SnappyFramed.HEADER_BYTES.length + block.length];
        System.arraycopy(SnappyFramed.HEADER_BYTES, 0, stream, 0, SnappyFramed.HEADER_BYTES.length);
        System.arraycopy(block, 0, stream, SnappyFramed.HEADER_BYTES.length, block.length);
        return stream;
    }

    @Test
    public void testLargeWrites()
            throws Exception
    {
        byte[] random = getRandom(0.5, 500000);

        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        OutputStream snappyOut = new SnappyFramedOutputStream(out);

        // partially fill buffer
        int small = 1000;
        snappyOut.write(random, 0, small);

        // write more than the buffer size
        snappyOut.write(random, small, random.length - small);

        // get compressed data
        snappyOut.close();
        byte[] compressed = out.toByteArray();
        assertTrue(compressed.length < random.length);

        // decompress
        byte[] uncompressed = uncompress(compressed);
        assertEquals(uncompressed, random);

        // decompress byte at a time
        InputStream in = new SnappyFramedInputStream(new ByteArrayInputStream(compressed), true);
        int i = 0;
        int c;
        while ((c = in.read()) != -1) {
            uncompressed[i++] = (byte) c;
        }
        assertEquals(i, random.length);
        assertEquals(uncompressed, random);
    }

    @Test
    public void testSingleByteWrites()
            throws Exception
    {
        byte[] random = getRandom(0.5, 500000);

        java.io.ByteArrayOutputStream out = new ByteArrayOutputStream();
        OutputStream snappyOut = new SnappyFramedOutputStream(out);

        for (byte b : random) {
            snappyOut.write(b);
        }

        snappyOut.close();
        byte[] compressed = out.toByteArray();
        assertTrue(compressed.length < random.length);

        byte[] uncompressed = uncompress(compressed);
        assertEquals(uncompressed, random);
    }

    @Test
    public void testExtraFlushes()
            throws Exception
    {
        byte[] random = getRandom(0.5, 500000);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        OutputStream snappyOut = new SnappyFramedOutputStream(out);

        snappyOut.write(random);

        for (int i = 0; i < 10; i++) {
            snappyOut.flush();
        }

        snappyOut.close();
        byte[] compressed = out.toByteArray();
        assertTrue(compressed.length < random.length);

        byte[] uncompressed = uncompress(compressed);
        assertEquals(uncompressed, random);
    }

    @Test
    public void testUncompressibleRange()
            throws Exception
    {
        int max = 128 * 1024;
        byte[] random = getRandom(1, max);

        for (int i = 1; i <= max; i += 102) {
            byte[] original = Arrays.copyOfRange(random, 0, i);

            byte[] compressed = compress(original);
            byte[] uncompressed = uncompress(compressed);

            assertEquals(uncompressed, original);
            // assertEquals(compressed.length, original.length + overhead);
        }
    }

    @Test
    public void testByteForByteTestData()
            throws Exception
    {
        List<DataSet> dataSets = new TestingModule().dataSets();
        for (DataSet dataSet : dataSets) {
            byte[] original = dataSet.getUncompressed();
            byte[] compressed = compress(original);
            byte[] uncompressed = uncompress(compressed);
            assertEquals(uncompressed, original);
        }
    }

    @Test(expectedExceptions = EOFException.class, expectedExceptionsMessageRegExp = ".*stream header.*")
    public void testEmptyStream()
            throws Exception
    {
        uncompress(new byte[0]);
    }

    @Test(expectedExceptions = IOException.class, expectedExceptionsMessageRegExp = ".*stream header.*")
    public void testInvalidStreamHeader()
            throws Exception
    {
        uncompress(new byte[] {'b', 0, 0, 'g', 'u', 's', 0});
    }

    @Test
    public void testCloseIsIdempotent()
            throws Exception
    {
        byte[] random = getRandom(0.5, 500000);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        OutputStream snappyOut = new SnappyFramedOutputStream(out);

        snappyOut.write(random);

        snappyOut.close();
        snappyOut.close();

        byte[] compressed = out.toByteArray();

        InputStream snappyIn = new SnappyFramedInputStream(new ByteArrayInputStream(compressed), true);
        byte[] uncompressed = toByteArray(snappyIn);
        assertEquals(uncompressed, random);

        snappyIn.close();
        snappyIn.close();
    }

    /**
     * Tests that the presence of the marker bytes can appear as a valid frame
     * anywhere in stream.
     */
    @Test
    public void testMarkerFrameInStream()
            throws IOException
    {
        int size = 500000;
        byte[] random = getRandom(0.5, size);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        OutputStream os = new SnappyFramedOutputStream(out);

        byte[] markerFrame = getMarkerFrame();

        for (int i = 0; i < size; ) {
            int toWrite = Math.max((size - i) / 4, 512);

            // write some data to be compressed
            os.write(random, i, Math.min(size - i, toWrite));
            // force the write of a frame
            os.flush();

            // write the marker frame to the underlying byte array output stream
            out.write(markerFrame);

            // this is not accurate for the final write, but at that point it
            // does not matter
            // as we will be exiting the for loop now
            i += toWrite;
        }

        byte[] compressed = out.toByteArray();
        byte[] uncompressed = uncompress(compressed);

        assertEquals(random, uncompressed);
    }

    public static byte[] blockCompress(byte[] data)
    {
        SnappyCompressor compressor = new SnappyCompressor();
        byte[] compressedOut = new byte[compressor.maxCompressedLength(data.length)];
        int compressedSize = compressor.compress(data, 0, data.length, compressedOut, 0, compressedOut.length);
        byte[] trimmedBuffer = Arrays.copyOf(compressedOut, compressedSize);
        return trimmedBuffer;
    }

    private static byte[] compress(byte[] original)
            throws IOException
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        OutputStream snappyOut = new SnappyFramedOutputStream(out);
        snappyOut.write(original);
        snappyOut.close();
        return out.toByteArray();
    }

    private static byte[] uncompress(byte[] compressed)
            throws IOException
    {
        return toByteArray(new SnappyFramedInputStream(new ByteArrayInputStream(compressed)));
    }

    static File[] getTestFiles()
    {
        File[] testFiles = TEST_DATA_DIR.listFiles();
        Assert.assertTrue(testFiles != null && testFiles.length > 0, "No test files at " + TEST_DATA_DIR.getAbsolutePath());
        return testFiles;
    }

    private static final File TEST_DATA_DIR = new File("testdata");
}
