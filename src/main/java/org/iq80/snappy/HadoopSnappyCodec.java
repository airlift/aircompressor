package org.iq80.snappy;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class HadoopSnappyCodec
        implements CompressionCodec
{
    @Override
    public CompressionOutputStream createOutputStream(OutputStream outputStream)
            throws IOException
    {
        return new SnappyCompressionOutputStream(outputStream);
    }

    @Override
    public CompressionOutputStream createOutputStream(OutputStream outputStream, Compressor compressor)
            throws IOException
    {
        throw new UnsupportedOperationException("Snappy Compressor is not supported");
    }

    @Override
    public Class<? extends Compressor> getCompressorType()
    {
        throw new UnsupportedOperationException("Snappy Compressor is not supported");
    }

    @Override
    public Compressor createCompressor()
    {
        throw new UnsupportedOperationException("Snappy Compressor is not supported");
    }

    @Override
    public CompressionInputStream createInputStream(InputStream inputStream)
            throws IOException
    {
        return new SnappyCompressionInputStream(inputStream);
    }

    @Override
    public CompressionInputStream createInputStream(InputStream inputStream, Decompressor decompressor)
            throws IOException
    {
        throw new UnsupportedOperationException("Snappy Decompressor is not supported");
    }

    @Override
    public Class<? extends Decompressor> getDecompressorType()
    {
        throw new UnsupportedOperationException("Snappy Decompressor is not supported");
    }

    @Override
    public Decompressor createDecompressor()
    {
        throw new UnsupportedOperationException("Snappy Decompressor is not supported");
    }

    @Override
    public String getDefaultExtension()
    {
        return ".snappy";
    }

    private static class SnappyCompressionOutputStream
            extends CompressionOutputStream
    {
        public SnappyCompressionOutputStream(OutputStream outputStream)
                throws IOException
        {
            super(new SnappyOutputStream(outputStream));
        }

        @Override
        public void write(byte[] b, int off, int len)
                throws IOException
        {
            out.write(b, off, len);
        }

        @Override
        public void finish()
                throws IOException
        {
            out.flush();
        }

        @Override
        public void resetState()
                throws IOException
        {
            out.flush();
        }

        @Override
        public void write(int b)
                throws IOException
        {
            out.write(b);
        }
    }

    private static class SnappyCompressionInputStream
            extends CompressionInputStream
    {
        public SnappyCompressionInputStream(InputStream inputStream)
        {
            super(new SnappyInputStream(inputStream));
        }

        @Override
        public int read(byte[] b, int off, int len)
                throws IOException
        {
            return in.read(b, off, len);
        }

        @Override
        public void resetState()
                throws IOException
        {
            throw new UnsupportedOperationException("resetState not supported for Snappy");
        }

        @Override
        public int read()
                throws IOException
        {
            return in.read();
        }
    }
}
