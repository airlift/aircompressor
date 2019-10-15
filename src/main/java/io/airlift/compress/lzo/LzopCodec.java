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

import io.airlift.compress.lzo.LzoCodec.HadoopLzoCompressor;
import io.airlift.compress.lzo.LzoCodec.HadoopLzoDecompressor;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static io.airlift.compress.lzo.LzoCodec.LZO_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IO_COMPRESSION_CODEC_LZO_BUFFERSIZE_KEY;

public class LzopCodec
        implements Configurable, CompressionCodec
{
    static final byte[] LZOP_MAGIC = new byte[] {(byte) 0x89, 0x4c, 0x5a, 0x4f, 0x00, 0x0d, 0x0a, 0x1a, 0x0a};
    static final byte LZO_1X_VARIANT = 1;

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
        return new HadoopLzopOutputStream(out, getBufferSize());
    }

    @Override
    public CompressionOutputStream createOutputStream(OutputStream out, Compressor compressor)
            throws IOException
    {
        return new HadoopLzopOutputStream(out, getBufferSize());
    }

    @Override
    public Class<? extends Compressor> getCompressorType()
    {
        throw new UnsupportedOperationException();
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
        return new HadoopLzopInputStream(in, getBufferSize());
    }

    @Override
    public CompressionInputStream createInputStream(InputStream in, Decompressor decompressor)
            throws IOException
    {
        return new HadoopLzopInputStream(in, getBufferSize());
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
}
