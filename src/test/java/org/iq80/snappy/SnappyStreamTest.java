package org.iq80.snappy;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import static com.google.common.io.ByteStreams.toByteArray;
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
        assertTrue(compressed.length < original.length);
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

        byte[] compressed = compress(random);
        byte[] uncompressed = uncompress(compressed);

        assertEquals(uncompressed, random);
        assertEquals(compressed.length, random.length + 2);
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

            int overhead = (i <= 32768) ? 2 : 4;
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

    private static byte[] uncompress(byte[] compressed)
            throws IOException
    {
        return toByteArray(new SnappyInputStream(new ByteArrayInputStream(compressed)));
    }
}
