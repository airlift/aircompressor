package io.airlift.compress.lz4;

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

import static org.apache.hadoop.fs.CommonConfigurationKeys.IO_COMPRESSION_CODEC_LZ4_BUFFERSIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IO_COMPRESSION_CODEC_LZ4_BUFFERSIZE_KEY;

public class Lz4Codec
        implements Configurable, CompressionCodec
{
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
        return new HadoopLz4OutputStream(out, getBufferSize());
    }

    @Override
    public CompressionOutputStream createOutputStream(OutputStream out, Compressor compressor)
            throws IOException
    {
        if (!(compressor instanceof HadoopLz4Compressor)) {
            throw new IllegalArgumentException("Compressor is not the LZ4 compressor");
        }
        return new HadoopLz4OutputStream(out, getBufferSize());
    }

    @Override
    public Class<? extends Compressor> getCompressorType()
    {
        throw new UnsupportedOperationException("LZ4 compression not supported");
    }

    @Override
    public Compressor createCompressor()
    {
        throw new UnsupportedOperationException("LZ4 compression not supported");
    }

    @Override
    public CompressionInputStream createInputStream(InputStream in)
            throws IOException
    {
        return new HadoopLz4InputStream(in, getBufferSize());
    }

    @Override
    public CompressionInputStream createInputStream(InputStream in, Decompressor decompressor)
            throws IOException
    {
        if (!(decompressor instanceof HadoopLz4Decompressor)) {
            throw new IllegalArgumentException("Decompressor is not the LZ4 decompressor");
        }
        return new HadoopLz4InputStream(in, getBufferSize());
    }

    @Override
    public Class<? extends Decompressor> getDecompressorType()
    {
        return HadoopLz4Decompressor.class;
    }

    @Override
    public Decompressor createDecompressor()
    {
        return new HadoopLz4Decompressor();
    }

    private int getBufferSize()
    {
        //
        // To decode a LZ4 block we must preallocate an output buffer, but
        // the Hadoop block stream format does not include the uncompressed
        // size of chunks.  Instead, we must rely on the "configured"
        // maximum buffer size used by the writer of the file.
        //

        int maxUncompressedLength;
        if (conf != null) {
            maxUncompressedLength = conf.getInt(IO_COMPRESSION_CODEC_LZ4_BUFFERSIZE_KEY, IO_COMPRESSION_CODEC_LZ4_BUFFERSIZE_DEFAULT);
        }
        else {
            maxUncompressedLength = IO_COMPRESSION_CODEC_LZ4_BUFFERSIZE_DEFAULT;
        }
        return maxUncompressedLength;
    }

    @Override
    public String getDefaultExtension()
    {
        return ".lz4";
    }

    /**
     * No Hadoop code seems to actually use the compressor, so just return a dummy one so the createOutputStream method
     * with a compressor can function.  This interface can be implemented if needed.
     */
    @DoNotPool
    private static class HadoopLz4Compressor
            implements Compressor
    {
        @Override
        public void setInput(byte[] b, int off, int len)
        {
            throw new UnsupportedOperationException("LZ4 block compressor is not supported");
        }

        @Override
        public boolean needsInput()
        {
            throw new UnsupportedOperationException("LZ4 block compressor is not supported");
        }

        @Override
        public void setDictionary(byte[] b, int off, int len)
        {
            throw new UnsupportedOperationException("LZ4 block compressor is not supported");
        }

        @Override
        public long getBytesRead()
        {
            throw new UnsupportedOperationException("LZ4 block compressor is not supported");
        }

        @Override
        public long getBytesWritten()
        {
            throw new UnsupportedOperationException("LZ4 block compressor is not supported");
        }

        @Override
        public void finish()
        {
            throw new UnsupportedOperationException("LZ4 block compressor is not supported");
        }

        @Override
        public boolean finished()
        {
            throw new UnsupportedOperationException("LZ4 block compressor is not supported");
        }

        @Override
        public int compress(byte[] b, int off, int len)
                throws IOException
        {
            throw new UnsupportedOperationException("LZ4 block compressor is not supported");
        }

        @Override
        public void reset()
        {
            throw new UnsupportedOperationException("LZ4 block compressor is not supported");
        }

        @Override
        public void end()
        {
            throw new UnsupportedOperationException("LZ4 block compressor is not supported");
        }

        @Override
        public void reinit(Configuration conf)
        {
        }
    }

    /**
     * No Hadoop code seems to actually use the decompressor, so just return a dummy one so the createInputStream method
     * with a decompressor can function.  This interface can be implemented if needed, but would require modifying the
     * LZ4 decompress method to resize the output buffer, since the Hadoop block decompressor does not get the uncompressed
     * size.
     */
    @DoNotPool
    private static class HadoopLz4Decompressor
            implements Decompressor
    {
        @Override
        public void setInput(byte[] b, int off, int len)
        {
            throw new UnsupportedOperationException("LZ4 block decompressor is not supported");
        }

        @Override
        public boolean needsInput()
        {
            throw new UnsupportedOperationException("LZ4 block decompressor is not supported");
        }

        @Override
        public void setDictionary(byte[] b, int off, int len)
        {
            throw new UnsupportedOperationException("LZ4 block decompressor is not supported");
        }

        @Override
        public boolean needsDictionary()
        {
            throw new UnsupportedOperationException("LZ4 block decompressor is not supported");
        }

        @Override
        public boolean finished()
        {
            throw new UnsupportedOperationException("LZ4 block decompressor is not supported");
        }

        @Override
        public int decompress(byte[] b, int off, int len)
                throws IOException
        {
            throw new UnsupportedOperationException("LZ4 block decompressor is not supported");
        }

        @Override
        public void reset()
        {
            throw new UnsupportedOperationException("LZ4 block decompressor is not supported");
        }

        @Override
        public int getRemaining()
        {
            throw new UnsupportedOperationException("LZ4 block decompressor is not supported");
        }

        @Override
        public void end()
        {
            throw new UnsupportedOperationException("LZ4 block decompressor is not supported");
        }
    }
}
