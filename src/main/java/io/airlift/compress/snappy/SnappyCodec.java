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
package io.airlift.compress.snappy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class SnappyCodec
        implements CompressionCodec
{
    @Override
    public CompressionOutputStream createOutputStream(OutputStream outputStream)
            throws IOException
    {
        return new HadoopSnappyOutputStream(outputStream);
    }

    @Override
    public CompressionOutputStream createOutputStream(OutputStream outputStream, Compressor compressor)
            throws IOException
    {
        if (!(compressor instanceof HadoopSnappyCompressor)) {
            throw new IllegalArgumentException("Compressor is not the Snappy decompressor");
        }
        return new HadoopSnappyOutputStream(outputStream);
    }

    @Override
    public Class<? extends Compressor> getCompressorType()
    {
        return HadoopSnappyCompressor.class;
    }

    @Override
    public Compressor createCompressor()
    {
        return new HadoopSnappyCompressor();
    }

    @Override
    public CompressionInputStream createInputStream(InputStream inputStream)
            throws IOException
    {
        return new HadoopSnappyInputStream(inputStream);
    }

    @Override
    public CompressionInputStream createInputStream(InputStream in, Decompressor decompressor)
            throws IOException
    {
        if (!(decompressor instanceof HadoopSnappyDecompressor)) {
            throw new IllegalArgumentException("Decompressor is not the Snappy decompressor");
        }
        return new HadoopSnappyInputStream(in);
    }

    @Override
    public Class<? extends Decompressor> getDecompressorType()
    {
        return HadoopSnappyDecompressor.class;
    }

    @Override
    public Decompressor createDecompressor()
    {
        return new HadoopSnappyDecompressor();
    }

    @Override
    public String getDefaultExtension()
    {
        return ".snappy";
    }

    /**
     * No Hadoop code seems to actually use the compressor, so just return a dummy one so the createOutputStream method
     * with a compressor can function.  This interface can be implemented if needed.
     */
    private static class HadoopSnappyCompressor
            implements Compressor
    {
        @Override
        public void setInput(byte[] b, int off, int len)
        {
            throw new UnsupportedOperationException("Snappy block compressor is not supported");
        }

        @Override
        public boolean needsInput()
        {
            throw new UnsupportedOperationException("Snappy block compressor is not supported");
        }

        @Override
        public void setDictionary(byte[] b, int off, int len)
        {
            throw new UnsupportedOperationException("Snappy block compressor is not supported");
        }

        @Override
        public long getBytesRead()
        {
            throw new UnsupportedOperationException("Snappy block compressor is not supported");
        }

        @Override
        public long getBytesWritten()
        {
            throw new UnsupportedOperationException("Snappy block compressor is not supported");
        }

        @Override
        public void finish()
        {
            throw new UnsupportedOperationException("Snappy block compressor is not supported");
        }

        @Override
        public boolean finished()
        {
            throw new UnsupportedOperationException("Snappy block compressor is not supported");
        }

        @Override
        public int compress(byte[] b, int off, int len)
                throws IOException
        {
            throw new UnsupportedOperationException("Snappy block compressor is not supported");
        }

        @Override
        public void reset()
        {
            throw new UnsupportedOperationException("Snappy block compressor is not supported");
        }

        @Override
        public void end()
        {
            throw new UnsupportedOperationException("Snappy block compressor is not supported");
        }

        @Override
        public void reinit(Configuration conf)
        {
        }
    }

    /**
     * No Hadoop code seems to actually use the decompressor, so just return a dummy one so the createInputStream method
     * with a decompressor can function.  This interface can be implemented if needed.
     */
    private static class HadoopSnappyDecompressor
            implements Decompressor
    {
        @Override
        public void setInput(byte[] b, int off, int len)
        {
            throw new UnsupportedOperationException("Snappy block decompressor is not supported");
        }

        @Override
        public boolean needsInput()
        {
            throw new UnsupportedOperationException("Snappy block decompressor is not supported");
        }

        @Override
        public void setDictionary(byte[] b, int off, int len)
        {
            throw new UnsupportedOperationException("Snappy block decompressor is not supported");
        }

        @Override
        public boolean needsDictionary()
        {
            throw new UnsupportedOperationException("Snappy block decompressor is not supported");
        }

        @Override
        public boolean finished()
        {
            throw new UnsupportedOperationException("Snappy block decompressor is not supported");
        }

        @Override
        public int decompress(byte[] b, int off, int len)
                throws IOException
        {
            throw new UnsupportedOperationException("Snappy block decompressor is not supported");
        }

        @Override
        public void reset()
        {
            throw new UnsupportedOperationException("Snappy block decompressor is not supported");
        }

        @Override
        public int getRemaining()
        {
            throw new UnsupportedOperationException("Snappy block decompressor is not supported");
        }

        @Override
        public void end()
        {
            throw new UnsupportedOperationException("Snappy block decompressor is not supported");
        }
    }
}
