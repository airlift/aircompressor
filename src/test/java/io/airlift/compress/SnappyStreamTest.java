/*
 * Copyright (C) 2011 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
 *
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
package org.iq80.snappy;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static com.google.common.io.ByteStreams.toByteArray;
import static com.google.common.primitives.UnsignedBytes.toInt;
import static org.iq80.snappy.SnappyOutputStream.STREAM_HEADER;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class SnappyStreamTest
{
    @Test
    public void testSimple()
            throws Exception
    {
        byte[] original = "aaaaaaaaaaaabbbbbbbaaaaaa".getBytes(Charsets.UTF_8);

        byte[] compressed = compress(original);
        byte[] uncompressed = uncompress(compressed);

        assertEquals(uncompressed, original);
        assertEquals(compressed.length, 33);      // 7 byte stream header, 7 byte block header, 19 bytes compressed data
        assertEquals(Arrays.copyOf(compressed, 7), STREAM_HEADER); // stream header
        assertEquals(toInt(compressed[7]), 0x01); // flag: compressed
        assertEquals(toInt(compressed[8]), 0x00); // length: 19 = 0x0013
        assertEquals(toInt(compressed[9]), 0x13);
        assertEquals(toInt(compressed[10]), 0x92); // crc32c: 0x9274cda8
        assertEquals(toInt(compressed[11]), 0x74);
        assertEquals(toInt(compressed[12]), 0xCD);
        assertEquals(toInt(compressed[13]), 0xA8);
    }

    @Test
    public void testLargeWrites()
            throws Exception
    {
        byte[] random = getRandom(0.5, 500000);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        OutputStream snappyOut = new SnappyOutputStream(out);

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
        SnappyInputStream in = new SnappyInputStream(new ByteArrayInputStream(compressed));
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

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        OutputStream snappyOut = new SnappyOutputStream(out);

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
        OutputStream snappyOut = new SnappyOutputStream(out);

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
    public void testUncompressable()
            throws Exception
    {
        byte[] random = getRandom(1, 5000);
        int crc32c = Crc32C.maskedCrc32c(random);

        byte[] compressed = compress(random);
        byte[] uncompressed = uncompress(compressed);

        assertEquals(uncompressed, random);
        assertEquals(compressed.length, random.length + 7 + 7);
        assertEquals(toInt(compressed[7]), 0x00); // flag: uncompressed
        assertEquals(toInt(compressed[8]), 0x13); // length: 5000 = 0x1388
        assertEquals(toInt(compressed[9]), 0x88);
        assertEquals(ByteBuffer.wrap(compressed, 10, 4).getInt(), crc32c); // crc: see above
    }

    @Test
    public void testUncompressableRange()
            throws Exception
    {
        int max = 35000;
        byte[] random = getRandom(1, max);

        for (int i = 1; i <= max; i++) {
            byte[] original = Arrays.copyOfRange(random, 0, i);

            byte[] compressed = compress(original);
            byte[] uncompressed = uncompress(compressed);

            // Stream header plus one or two blocks
            int overhead = 7 + ((i <= 32768) ? 7 : 14);

            assertEquals(uncompressed, original);
            assertEquals(compressed.length, original.length + overhead);
        }
    }

    @Test
    public void testByteForByteTestData()
            throws Exception
    {
        for (File testFile : SnappyTest.getTestFiles()) {
            byte[] original = Files.toByteArray(testFile);
            byte[] compressed = compress(original);
            byte[] uncompressed = uncompress(compressed);
            assertEquals(uncompressed, original);
        }
    }

    @Test
    public void testEmptyCompression()
            throws Exception
    {
        byte[] empty = new byte[0];
        assertEquals(compress(empty), STREAM_HEADER);
        assertEquals(uncompress(STREAM_HEADER), empty);
    }

    @Test(expectedExceptions = EOFException.class, expectedExceptionsMessageRegExp = ".*stream header.*")
    public void testEmptyStream()
            throws Exception
    {
        uncompress(new byte[0]);
    }

    @Test(expectedExceptions = IOException.class, expectedExceptionsMessageRegExp = "invalid stream header")
    public void testInvalidStreamHeader()
            throws Exception
    {
        uncompress(new byte[] {'b', 0, 0, 'g', 'u', 's', 0});
    }

    @Test(expectedExceptions = EOFException.class, expectedExceptionsMessageRegExp = ".*block header.*")
    public void testShortBlockHeader()
            throws Exception
    {
        uncompressBlock(new byte[]{0});
    }

    @Test(expectedExceptions = EOFException.class, expectedExceptionsMessageRegExp = ".*block data.*")
    public void testShortBlockData()
            throws Exception
    {
        uncompressBlock(new byte[]{0, 0, 4, 0, 0, 0, 0, 'x', 'x'}); // flag = 0, size = 4, crc32c = 0, block data = [x, x]
    }

    @Test(expectedExceptions = IOException.class, expectedExceptionsMessageRegExp = "invalid compressed flag in header: 0x41")
    public void testInvalidBlockHeaderCompressedFlag()
            throws Exception
    {
        uncompressBlock(new byte[]{'A', 0, 1, 0, 0, 0, 0, 0}); // flag = 'A', block size = 1, crc32c = 0
    }

    @Test(expectedExceptions = IOException.class, expectedExceptionsMessageRegExp = "invalid block size in header: 0")
    public void testInvalidBlockSizeZero()
            throws Exception
    {
        uncompressBlock(new byte[]{0, 0, 0, 0, 0, 0, 0}); // flag = '0', block size = 0, crc32c = 0
    }

    @Test(expectedExceptions = IOException.class, expectedExceptionsMessageRegExp = "invalid block size in header: 55555")
    public void testInvalidBlockSizeLarge()
            throws Exception
    {
        uncompressBlock(new byte[]{0, (byte) 0xD9, 0x03, 0, 0, 0, 0}); // flag = 0, block size = 55555, crc32c = 0
    }

    @Test(expectedExceptions = IOException.class, expectedExceptionsMessageRegExp = "Corrupt input: invalid checksum")
    public void testInvalidChecksum()
            throws Exception
    {
        uncompressBlock(new byte[]{0, 0, 1, 0, 0, 0, 0, 'a'}); // flag = 0, size = 4, crc32c = 0, block data = [a]
    }

    @Test
    public void testInvalidChecksumIgnoredWhenVerificationDisabled()
            throws Exception
    {
        byte[] block = {0, 0, 1, 0, 0, 0, 0, 'a'}; // flag = 0, size = 4, crc32c = 0, block data = [a]
        ByteArrayInputStream inputData = new ByteArrayInputStream(blockToStream(block));
        assertEquals(toByteArray(new SnappyInputStream(inputData, false)), new byte[] {'a'});
    }

    @Test
    public void testCloseIsIdempotent()
            throws Exception
    {
        byte[] random = getRandom(0.5, 500000);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        OutputStream snappyOut = new SnappyOutputStream(out);

        snappyOut.write(random);

        snappyOut.close();
        snappyOut.close();

        byte[] compressed = out.toByteArray();

        InputStream snappyIn = new SnappyInputStream(new ByteArrayInputStream(compressed));
        byte[] uncompressed = toByteArray(snappyIn);
        assertEquals(uncompressed, random);

        snappyIn.close();
        snappyIn.close();
    }

    private static byte[] getRandom(double compressionRatio, int length)
    {
        SnappyTest.RandomGenerator gen = new SnappyTest.RandomGenerator(compressionRatio);
        gen.getNextPosition(length);
        byte[] random = Arrays.copyOf(gen.data, length);
        assertEquals(random.length, length);
        return random;
    }

    private static byte[] compress(byte[] original)
            throws IOException
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        OutputStream snappyOut = new SnappyOutputStream(out);
        snappyOut.write(original);
        snappyOut.close();
        return out.toByteArray();
    }

    private static byte[] uncompressBlock(byte[] block)
            throws IOException
    {
        return uncompress(blockToStream(block));
    }

    private static byte[] blockToStream(byte[] block)
    {
        byte[] stream = new byte[STREAM_HEADER.length + block.length];
        System.arraycopy(STREAM_HEADER, 0, stream, 0, STREAM_HEADER.length);
        System.arraycopy(block, 0, stream, STREAM_HEADER.length, block.length);
        return stream;
    }

    private static byte[] uncompress(byte[] compressed)
            throws IOException
    {
        return toByteArray(new SnappyInputStream(new ByteArrayInputStream(compressed)));
    }
}
