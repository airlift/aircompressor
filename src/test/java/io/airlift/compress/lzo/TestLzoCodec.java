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

import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import io.airlift.compress.AbstractTestCompression;
import io.airlift.compress.Compressor;
import io.airlift.compress.Decompressor;
import io.airlift.compress.HadoopCodecDecompressor;
import io.airlift.compress.HadoopNative;
import io.airlift.compress.benchmark.DataSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionOutputStream;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class TestLzoCodec
    extends AbstractTestCompression
{
    static {
        HadoopNative.requireHadoopNative();
    }

    private static final Configuration HADOOP_CONF = new Configuration();

    protected byte[] prepareCompressedData(byte[] uncompressed)
    {
        com.hadoop.compression.lzo.LzoCodec codec = new com.hadoop.compression.lzo.LzoCodec();
        codec.setConf(HADOOP_CONF);

        try {
            java.io.ByteArrayOutputStream buffer = new java.io.ByteArrayOutputStream();
            CompressionOutputStream out = codec.createOutputStream(buffer);
            ByteStreams.copy(new ByteArrayInputStream(uncompressed, 0, uncompressed.length), out);
            out.close();
            return buffer.toByteArray();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void testCompress(DataSet testCase)
            throws Exception
    {
        // not yet supported
    }

    @Override
    protected Compressor getCompressor()
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    protected Decompressor getDecompressor()
    {
        return new HadoopCodecDecompressor(new LzoCodec());
    }
}
