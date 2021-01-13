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
package io.airlift.compress.gzip;

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

import static io.airlift.compress.gzip.JdkGzipConstants.GZIP_BUFFER_SIZE;

public class JdkGzipCodec
        implements CompressionCodec
{
    @Override
    public CompressionOutputStream createOutputStream(OutputStream outputStream)
            throws IOException
    {
        return new HadoopJdkGzipOutputStream(outputStream, GZIP_BUFFER_SIZE);
    }

    @Override
    public CompressionOutputStream createOutputStream(OutputStream outputStream, Compressor compressor)
            throws IOException
    {
        if (!(compressor instanceof HadoopGzipCompressor)) {
            throw new IllegalArgumentException("Compressor is not the Gzip decompressor");
        }
        return new HadoopJdkGzipOutputStream(outputStream, GZIP_BUFFER_SIZE);
    }

    @Override
    public Class<? extends Compressor> getCompressorType()
    {
        return HadoopGzipCompressor.class;
    }

    @Override
    public Compressor createCompressor()
    {
        return new HadoopGzipCompressor();
    }

    @Override
    public CompressionInputStream createInputStream(InputStream inputStream)
            throws IOException
    {
        return new HadoopJdkGzipInputStream(inputStream, GZIP_BUFFER_SIZE);
    }

    @Override
    public CompressionInputStream createInputStream(InputStream inputStream, Decompressor decompressor)
            throws IOException
    {
        if (!(decompressor instanceof HadoopGzipDecompressor)) {
            throw new IllegalArgumentException("Decompressor is not the Gzip decompressor");
        }
        return new HadoopJdkGzipInputStream(inputStream, GZIP_BUFFER_SIZE);
    }

    @Override
    public Class<? extends Decompressor> getDecompressorType()
    {
        return HadoopGzipDecompressor.class;
    }

    @Override
    public Decompressor createDecompressor()
    {
        return new HadoopGzipDecompressor();
    }

    @Override
    public String getDefaultExtension()
    {
        return ".gz";
    }

    /**
     * No Hadoop code seems to actually use the compressor, so just return a dummy one so the createOutputStream method
     * with a compressor can function.  This interface can be implemented if needed.
     */
    @DoNotPool
    private static class HadoopGzipCompressor
            implements Compressor
    {
        @Override
        public void setInput(byte[] b, int off, int len)
        {
            throw new UnsupportedOperationException("Gzip block compressor is not supported");
        }

        @Override
        public boolean needsInput()
        {
            throw new UnsupportedOperationException("Gzip block compressor is not supported");
        }

        @Override
        public void setDictionary(byte[] b, int off, int len)
        {
            throw new UnsupportedOperationException("Gzip block compressor is not supported");
        }

        @Override
        public long getBytesRead()
        {
            throw new UnsupportedOperationException("Gzip block compressor is not supported");
        }

        @Override
        public long getBytesWritten()
        {
            throw new UnsupportedOperationException("Gzip block compressor is not supported");
        }

        @Override
        public void finish()
        {
            throw new UnsupportedOperationException("Gzip block compressor is not supported");
        }

        @Override
        public boolean finished()
        {
            throw new UnsupportedOperationException("Gzip block compressor is not supported");
        }

        @Override
        public int compress(byte[] b, int off, int len)
                throws IOException
        {
            throw new UnsupportedOperationException("Gzip block compressor is not supported");
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
     * with a decompressor can function.  This interface can be implemented if needed.
     */
    @DoNotPool
    private static class HadoopGzipDecompressor
            implements Decompressor
    {
        @Override
        public void setInput(byte[] b, int off, int len)
        {
            throw new UnsupportedOperationException("Gzip block decompressor is not supported");
        }

        @Override
        public boolean needsInput()
        {
            throw new UnsupportedOperationException("Gzip block decompressor is not supported");
        }

        @Override
        public void setDictionary(byte[] b, int off, int len)
        {
            throw new UnsupportedOperationException("Gzip block decompressor is not supported");
        }

        @Override
        public boolean needsDictionary()
        {
            throw new UnsupportedOperationException("Gzip block decompressor is not supported");
        }

        @Override
        public boolean finished()
        {
            throw new UnsupportedOperationException("Gzip block decompressor is not supported");
        }

        @Override
        public int decompress(byte[] b, int off, int len)
                throws IOException
        {
            throw new UnsupportedOperationException("Gzip block decompressor is not supported");
        }

        @Override
        public int getRemaining()
        {
            throw new UnsupportedOperationException("Gzip block decompressor is not supported");
        }

        @Override
        public void reset() {}

        @Override
        public void end() {}
    }
}
