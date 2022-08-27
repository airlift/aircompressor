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
package io.airlift.compress.bzip2;

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

public class BZip2Codec
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
    {
        return new BZip2CompressionOutputStream(out);
    }

    @Override
    public CompressionOutputStream createOutputStream(OutputStream out, Compressor compressor)
    {
        if (!(compressor instanceof HadoopBZip2Compressor)) {
            throw new IllegalArgumentException("Compressor is not the BZip2 compressor");
        }
        return new BZip2CompressionOutputStream(out);
    }

    @Override
    public Class<? extends Compressor> getCompressorType()
    {
        return HadoopBZip2Compressor.class;
    }

    @Override
    public Compressor createCompressor()
    {
        return new HadoopBZip2Compressor();
    }

    @Override
    public CompressionInputStream createInputStream(InputStream in)
            throws IOException
    {
        return new BZip2CompressionInputStream(in);
    }

    @Override
    public CompressionInputStream createInputStream(InputStream in, Decompressor decompressor)
            throws IOException
    {
        if (!(decompressor instanceof HadoopBZip2Decompressor)) {
            throw new IllegalArgumentException("Decompressor is not the BZip2 decompressor");
        }
        return new BZip2CompressionInputStream(in);
    }

    @Override
    public Class<? extends Decompressor> getDecompressorType()
    {
        return HadoopBZip2Decompressor.class;
    }

    @Override
    public Decompressor createDecompressor()
    {
        return new HadoopBZip2Decompressor();
    }

    @Override
    public String getDefaultExtension()
    {
        return ".bz2";
    }

    /**
     * No Hadoop code seems to actually use the compressor, so just return a dummy one so the createOutputStream method
     * with a compressor can function.  This interface can be implemented if needed.
     */
    @DoNotPool
    private static class HadoopBZip2Compressor
            implements Compressor
    {
        @Override
        public void setInput(byte[] b, int off, int len)
        {
            throw new UnsupportedOperationException("BZip2 block compressor is not supported");
        }

        @Override
        public boolean needsInput()
        {
            throw new UnsupportedOperationException("BZip2 block compressor is not supported");
        }

        @Override
        public void setDictionary(byte[] b, int off, int len)
        {
            throw new UnsupportedOperationException("BZip2 block compressor is not supported");
        }

        @Override
        public long getBytesRead()
        {
            throw new UnsupportedOperationException("BZip2 block compressor is not supported");
        }

        @Override
        public long getBytesWritten()
        {
            throw new UnsupportedOperationException("BZip2 block compressor is not supported");
        }

        @Override
        public void finish()
        {
            throw new UnsupportedOperationException("BZip2 block compressor is not supported");
        }

        @Override
        public boolean finished()
        {
            throw new UnsupportedOperationException("BZip2 block compressor is not supported");
        }

        @Override
        public int compress(byte[] b, int off, int len)
                throws IOException
        {
            throw new UnsupportedOperationException("BZip2 block compressor is not supported");
        }

        @Override
        public void reset() {}

        @Override
        public void end() {}

        @Override
        public void reinit(Configuration conf) {}
    }

    /**
     * No Hadoop code seems to actually use the decompressor, so just return a dummy one so the createInputStream method
     * with a decompressor can function.  This interface can be implemented if needed, but would require modifying the
     * BZip2 decompress method to resize the output buffer, since the Hadoop block decompressor does not get the uncompressed
     * size.
     */
    @DoNotPool
    private static class HadoopBZip2Decompressor
            implements Decompressor
    {
        @Override
        public void setInput(byte[] b, int off, int len)
        {
            throw new UnsupportedOperationException("BZip2 block decompressor is not supported");
        }

        @Override
        public boolean needsInput()
        {
            throw new UnsupportedOperationException("BZip2 block decompressor is not supported");
        }

        @Override
        public void setDictionary(byte[] b, int off, int len)
        {
            throw new UnsupportedOperationException("BZip2 block decompressor is not supported");
        }

        @Override
        public boolean needsDictionary()
        {
            throw new UnsupportedOperationException("BZip2 block decompressor is not supported");
        }

        @Override
        public boolean finished()
        {
            throw new UnsupportedOperationException("BZip2 block decompressor is not supported");
        }

        @Override
        public int decompress(byte[] b, int off, int len)
                throws IOException
        {
            throw new UnsupportedOperationException("BZip2 block decompressor is not supported");
        }

        @Override
        public int getRemaining()
        {
            throw new UnsupportedOperationException("BZip2 block decompressor is not supported");
        }

        @Override
        public void reset() {}

        @Override
        public void end() {}
    }
}
