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
        return new HadoopLzoOutputStream(out, getBufferSize());
    }

    @Override
    public CompressionOutputStream createOutputStream(OutputStream out, Compressor compressor)
            throws IOException
    {
        if (!(compressor instanceof HadoopLzoCompressor)) {
            throw new IllegalArgumentException("Compressor is not the LZO compressor");
        }
        return new HadoopLzoOutputStream(out, getBufferSize());
    }

    @Override
    public Class<? extends Compressor> getCompressorType()
    {
        return HadoopLzoCompressor.class;
    }

    @Override
    public Compressor createCompressor()
    {
        return new HadoopLzoCompressor();
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
        return ".lzo_deflate";
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
     * No Hadoop code seems to actually use the compressor, so just return a dummy one so the createOutputStream method
     * with a compressor can function.  This interface can be implemented if needed.
     */
    @DoNotPool
    static class HadoopLzoCompressor
            implements Compressor
    {
        @Override
        public void setInput(byte[] b, int off, int len)
        {
            throw new UnsupportedOperationException("LZO block compressor is not supported");
        }

        @Override
        public boolean needsInput()
        {
            throw new UnsupportedOperationException("LZO block compressor is not supported");
        }

        @Override
        public void setDictionary(byte[] b, int off, int len)
        {
            throw new UnsupportedOperationException("LZO block compressor is not supported");
        }

        @Override
        public long getBytesRead()
        {
            throw new UnsupportedOperationException("LZO block compressor is not supported");
        }

        @Override
        public long getBytesWritten()
        {
            throw new UnsupportedOperationException("LZO block compressor is not supported");
        }

        @Override
        public void finish()
        {
            throw new UnsupportedOperationException("LZO block compressor is not supported");
        }

        @Override
        public boolean finished()
        {
            throw new UnsupportedOperationException("LZO block compressor is not supported");
        }

        @Override
        public int compress(byte[] b, int off, int len)
                throws IOException
        {
            throw new UnsupportedOperationException("LZO block compressor is not supported");
        }

        @Override
        public void reset()
        {
        }

        @Override
        public void end()
        {
            throw new UnsupportedOperationException("LZO block compressor is not supported");
        }

        @Override
        public void reinit(Configuration conf)
        {
        }
    }

    /**
     * No Hadoop code seems to actually use the decompressor, so just return a dummy one so the createInputStream method
     * with a decompressor can function.  This interface can be implemented if needed, but would require modifying the
     * LZO decompress method to resize the output buffer, since the Hadoop block decompressor does not get the uncompressed
     * size.
     */
    @DoNotPool
    static class HadoopLzoDecompressor
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
