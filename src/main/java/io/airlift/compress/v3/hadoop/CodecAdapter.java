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
package io.airlift.compress.v3.hadoop;

import io.airlift.compress.v3.hadoop.CompressionInputStreamAdapter.PositionSupplier;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DoNotPool;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Optional;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class CodecAdapter
        implements Configurable, CompressionCodec
{
    private final Function<Optional<Configuration>, HadoopStreams> streamsFactory;
    private HadoopStreams hadoopStreams;
    private Configuration conf;

    public CodecAdapter(Function<Optional<Configuration>, HadoopStreams> streamsFactory)
    {
        this.streamsFactory = requireNonNull(streamsFactory, "streamsFactory is null");
        hadoopStreams = streamsFactory.apply(Optional.empty());
    }

    @Override
    public final Configuration getConf()
    {
        return conf;
    }

    @Override
    public final void setConf(Configuration conf)
    {
        this.conf = conf;
        hadoopStreams = streamsFactory.apply(Optional.of(conf));
    }

    @Override
    public final CompressionOutputStream createOutputStream(OutputStream out)
            throws IOException
    {
        return new CompressionOutputStreamAdapter(hadoopStreams.createOutputStream(out));
    }

    @Override
    public final CompressionOutputStream createOutputStream(OutputStream out, Compressor compressor)
            throws IOException
    {
        if (!(compressor instanceof CompressorAdapter)) {
            throw new IllegalArgumentException("Compressor is not the compressor adapter");
        }
        return new CompressionOutputStreamAdapter(hadoopStreams.createOutputStream(out));
    }

    @Override
    public final Class<? extends Compressor> getCompressorType()
    {
        return CompressorAdapter.class;
    }

    @Override
    public Compressor createCompressor()
    {
        return new CompressorAdapter();
    }

    @Override
    public final CompressionInputStream createInputStream(InputStream in)
            throws IOException
    {
        return new CompressionInputStreamAdapter(hadoopStreams.createInputStream(in), getPositionSupplier(in));
    }

    @Override
    public final CompressionInputStream createInputStream(InputStream in, Decompressor decompressor)
            throws IOException
    {
        if (!(decompressor instanceof DecompressorAdapter)) {
            throw new IllegalArgumentException("Decompressor is not the decompressor adapter");
        }
        return new CompressionInputStreamAdapter(hadoopStreams.createInputStream(in), getPositionSupplier(in));
    }

    private static PositionSupplier getPositionSupplier(InputStream inputStream)
    {
        if (inputStream instanceof Seekable) {
            return ((Seekable) inputStream)::getPos;
        }
        return () -> 0;
    }

    @Override
    public final Class<? extends Decompressor> getDecompressorType()
    {
        return DecompressorAdapter.class;
    }

    @Override
    public final Decompressor createDecompressor()
    {
        return new DecompressorAdapter();
    }

    @Override
    public final String getDefaultExtension()
    {
        return hadoopStreams.getDefaultFileExtension();
    }

    /**
     * No Hadoop code seems to actually use the compressor, so just return a dummy one so the createOutputStream method
     * with a compressor can function.  This interface can be implemented if needed.
     */
    @DoNotPool
    private static class CompressorAdapter
            implements Compressor
    {
        @Override
        public void setInput(byte[] b, int off, int len)
        {
            throw new UnsupportedOperationException("Block compressor is not supported");
        }

        @Override
        public boolean needsInput()
        {
            throw new UnsupportedOperationException("Block compressor is not supported");
        }

        @Override
        public void setDictionary(byte[] b, int off, int len)
        {
            throw new UnsupportedOperationException("Block compressor is not supported");
        }

        @Override
        public long getBytesRead()
        {
            throw new UnsupportedOperationException("Block compressor is not supported");
        }

        @Override
        public long getBytesWritten()
        {
            throw new UnsupportedOperationException("Block compressor is not supported");
        }

        @Override
        public void finish()
        {
            throw new UnsupportedOperationException("Block compressor is not supported");
        }

        @Override
        public boolean finished()
        {
            throw new UnsupportedOperationException("Block compressor is not supported");
        }

        @Override
        public int compress(byte[] b, int off, int len)
                throws IOException
        {
            throw new UnsupportedOperationException("Block compressor is not supported");
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
     * with a decompressor can function.
     */
    @DoNotPool
    private static class DecompressorAdapter
            implements Decompressor
    {
        @Override
        public void setInput(byte[] b, int off, int len)
        {
            throw new UnsupportedOperationException("Block decompressor is not supported");
        }

        @Override
        public boolean needsInput()
        {
            throw new UnsupportedOperationException("Block decompressor is not supported");
        }

        @Override
        public void setDictionary(byte[] b, int off, int len)
        {
            throw new UnsupportedOperationException("Block decompressor is not supported");
        }

        @Override
        public boolean needsDictionary()
        {
            throw new UnsupportedOperationException("Block decompressor is not supported");
        }

        @Override
        public boolean finished()
        {
            throw new UnsupportedOperationException("Block decompressor is not supported");
        }

        @Override
        public int decompress(byte[] b, int off, int len)
                throws IOException
        {
            throw new UnsupportedOperationException("Block decompressor is not supported");
        }

        @Override
        public int getRemaining()
        {
            throw new UnsupportedOperationException("Block decompressor is not supported");
        }

        @Override
        public void reset() {}

        @Override
        public void end() {}
    }
}
