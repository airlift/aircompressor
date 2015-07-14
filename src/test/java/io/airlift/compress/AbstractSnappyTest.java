package io.airlift.compress;

import com.google.common.io.Files;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Random;

public abstract class AbstractSnappyTest
{
    private static final File TEST_DATA_DIR = new File("testdata");
    private RandomGenerator randomGenerator = new RandomGenerator(0.5);


    protected abstract void verifyCompression(byte[] input, int position, int size)
            throws Exception;

    protected abstract void verifyUncompress(byte[] input, int position, int size)
            throws Exception;

    static File[] getTestFiles()
    {
        File[] testFiles = TEST_DATA_DIR.listFiles();
        Assert.assertTrue(testFiles != null && testFiles.length > 0, "No test files at " + TEST_DATA_DIR.getAbsolutePath());
        return testFiles;
    }

    @Test
    public void testByteForByteOutputSyntheticData()
            throws Exception
    {
        for (int i = 1; i < 65 * 1024; i++) {
            try {
                verifyCompression(i);
            }
            catch (Error e) {
                Assert.fail(i + " byte block", e);
            }
        }
    }

    @Test
    public void testByteForByteTestData()
            throws Exception
    {
        for (File testFile : getTestFiles()) {
            byte[] data = Files.toByteArray(testFile);
            try {
                verifyCompression(data, 0, data.length);
            }
            catch (Throwable e) {
                Assert.fail("Test data: " + testFile.getName(), e);

            }
        }
    }

    @Test
    public void testDecompressTestData()
            throws Exception
    {
        for (int i = 0; i < 100; i++) {
            for (File testFile : getTestFiles()) {
                byte[] data = Files.toByteArray(testFile);
                try {
                    verifyUncompress(data, 0, data.length);
                }
                catch (Throwable e) {
                    Assert.fail("Test data: " + testFile.getName(), e);

                }
            }
        }
    }

    private void verifyCompression(int size)
            throws Exception
    {
        byte[] input = randomGenerator.data;
        int position = randomGenerator.getNextPosition(size);

        verifyCompression(input, position, size);
    }

    public static class RandomGenerator
    {
        public final byte[] data;
        public int position;

        public RandomGenerator(double compressionRatio)
        {
            // We use a limited amount of data over and over again and ensure
            // that it is larger than the compression window (32KB), and also
            // large enough to serve all typical value sizes we want to write.
            Random rnd = new Random(301);
            data = new byte[1048576 + 100];
            for (int i = 0; i < 1048576; i += 100) {
                // Add a short fragment that is as compressible as specified ratio
                System.arraycopy(compressibleData(rnd, compressionRatio, 100), 0, data, i, 100);
            }
        }

        public int getNextPosition(int length)
        {
            if (position + length > data.length) {
                position = 0;
                assert (length < data.length);
            }
            int result = position;
            position += length;
            return result;
        }

        private static byte[] compressibleData(Random random, double compressionRatio, int length)
        {
            int raw = (int) (length * compressionRatio);
            if (raw < 1) {
                raw = 1;
            }
            byte[] rawData = generateRandomData(random, raw);

            // Duplicate the random data until we have filled "length" bytes
            byte[] dest = new byte[length];
            for (int i = 0; i < length; ) {
                int chunkLength = Math.min(rawData.length, length - i);
                System.arraycopy(rawData, 0, dest, i, chunkLength);
                i += chunkLength;
            }
            return dest;
        }

        private static byte[] generateRandomData(Random random, int length)
        {
            byte[] rawData = new byte[length];
            for (int i = 0; i < rawData.length; i++) {
                rawData[i] = (byte) random.nextInt(256);
            }
            return rawData;
        }
    }
}
