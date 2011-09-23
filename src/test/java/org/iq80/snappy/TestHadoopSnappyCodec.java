package org.iq80.snappy;

import com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import static com.google.common.io.ByteStreams.toByteArray;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestHadoopSnappyCodec
{
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
    public void testGetCodec()
    {
        Path path = new Path("test.json.snappy");

        Configuration conf = new Configuration();
        conf.set("io.compression.codecs", HadoopSnappyCodec.class.getName());

        CompressionCodec codec = new CompressionCodecFactory(conf).getCodec(path);

        assertNotNull(codec);
        assertTrue(codec instanceof HadoopSnappyCodec);
    }

    private static byte[] compress(byte[] original)
            throws IOException
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        HadoopSnappyCodec codec = new HadoopSnappyCodec();
        OutputStream snappyOut = codec.createOutputStream(out);
        snappyOut.write(original);
        snappyOut.close();
        return out.toByteArray();
    }

    private static byte[] uncompress(byte[] compressed)
            throws IOException
    {
        HadoopSnappyCodec codec = new HadoopSnappyCodec();
        return toByteArray(codec.createInputStream(new ByteArrayInputStream(compressed)));
    }
}
