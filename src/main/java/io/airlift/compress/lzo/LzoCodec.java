package io.airlift.compress.lzo;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DoNotPool;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static org.apache.hadoop.fs.CommonConfigurationKeys.IO_COMPRESSION_CODEC_LZO_BUFFERSIZE_KEY;

public class LzoCodec
        implements Configurable, CompressionCodec
{
    // Hadoop has a constant for this, but the LZO codebase uses a different value
    public static final int LZO_BUFFER_SIZE_DEFAULT = 256 * 1024;

    private Configuration conf;

    @Override
    public Configuration getConf()
    {
        return conf;
    }

    @Override
    public void setConf(Configuration conf)
    {
        this.conf = conf;
    }

    @Override
    public CompressionOutputStream createOutputStream(OutputStream out)
            throws IOException
    {
        throw new UnsupportedOperationException("LZO compression not supported");
    }

    @Override
    public CompressionOutputStream createOutputStream(OutputStream out, Compressor compressor)
            throws IOException
    {
        throw new UnsupportedOperationException("LZO compression not supported");
    }

    @Override
    public Class<? extends Compressor> getCompressorType()
    {
        throw new UnsupportedOperationException("LZO compression not supported");
    }

    @Override
    public Compressor createCompressor()
    {
        throw new UnsupportedOperationException("LZO compression not supported");
    }

    @Override
    public CompressionInputStream createInputStream(InputStream in)
            throws IOException
    {
        return new HadoopLzoInputStream(in, getBufferSize());
    }

    @Override
    public CompressionInputStream createInputStream(InputStream in, Decompressor decompressor)
            throws IOException
    {
        if (!(decompressor instanceof HadoopLzoDecompressor)) {
            throw new IllegalArgumentException("Decompressor is not the LZO decompressor");
        }
        return new HadoopLzoInputStream(in, getBufferSize());
    }

    @Override
    public Class<? extends Decompressor> getDecompressorType()
    {
        return HadoopLzoDecompressor.class;
    }

    @Override
    public Decompressor createDecompressor()
    {
        return new HadoopLzoDecompressor();
    }

    @Override
    public String getDefaultExtension()
    {
        return ".lzo";
    }

    private int getBufferSize()
    {
        //
        // To decode a LZO block we must preallocate an output buffer, but
        // the Hadoop block stream format does not include the uncompressed
        // size of chunks.  Instead, we must rely on the "configured"
        // maximum buffer size used by the writer of the file.
        //

        int maxUncompressedLength;
        if (conf != null) {
            maxUncompressedLength = conf.getInt(IO_COMPRESSION_CODEC_LZO_BUFFERSIZE_KEY, LZO_BUFFER_SIZE_DEFAULT);
        }
        else {
            maxUncompressedLength = LZO_BUFFER_SIZE_DEFAULT;
        }
        return maxUncompressedLength;
    }

    /**
     * No Hadoop code seems to actually use the decompressor, so just return a dummy one so the createInputStream method
     * with a decompressor can function.  This interface can be implemented if needed, but would require modifying the
     * LZO decompress method to resize the output buffer, since the Hadoop block decompressor does not get the uncompressed
     * size.
     */
    @DoNotPool
    private static class HadoopLzoDecompressor
            implements Decompressor
    {
        @Override
        public void setInput(byte[] b, int off, int len)
        {
            throw new UnsupportedOperationException("LZO block decompressor is not supported");
        }

        @Override
        public boolean needsInput()
        {
            throw new UnsupportedOperationException("LZO block decompressor is not supported");
        }

        @Override
        public void setDictionary(byte[] b, int off, int len)
        {
            throw new UnsupportedOperationException("LZO block decompressor is not supported");
        }

        @Override
        public boolean needsDictionary()
        {
            throw new UnsupportedOperationException("LZO block decompressor is not supported");
        }

        @Override
        public boolean finished()
        {
            throw new UnsupportedOperationException("LZO block decompressor is not supported");
        }

        @Override
        public int decompress(byte[] b, int off, int len)
                throws IOException
        {
            throw new UnsupportedOperationException("LZO block decompressor is not supported");
        }

        @Override
        public void reset()
        {
            throw new UnsupportedOperationException("LZO block decompressor is not supported");
        }

        @Override
        public int getRemaining()
        {
            throw new UnsupportedOperationException("LZO block decompressor is not supported");
        }

        @Override
        public void end()
        {
            throw new UnsupportedOperationException("LZO block decompressor is not supported");
        }
    }
}
