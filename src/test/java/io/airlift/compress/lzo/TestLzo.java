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
import com.hadoop.compression.lzo.LzoCodec;
import io.airlift.compress.AbstractTestCompression;
import io.airlift.compress.Decompressor;
import io.airlift.compress.HadoopNative;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Compressor;

import java.io.IOException;
import java.util.Arrays;

import static org.testng.Assert.assertTrue;

public class TestLzo
    extends AbstractTestCompression
{
    static {
        HadoopNative.initialize();
    }

    protected byte[] prepareCompressedData(byte[] uncompressed)
    {
        LzoCodec codec = new LzoCodec();
        codec.setConf(new Configuration());
        Compressor compressor = codec.createCompressor();
        compressor.setInput(uncompressed, 0, uncompressed.length);
        compressor.finish();

        byte[] compressed = new byte[uncompressed.length * 10];
        int compressedOffset = 0;
        while (!compressor.finished() && compressedOffset < compressed.length) {
            try {
                compressedOffset += compressor.compress(compressed, compressedOffset, compressed.length - compressedOffset);
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        if (!compressor.finished()) {
            assertTrue(compressor.finished());
        }
        return Arrays.copyOf(compressed, compressedOffset);
    }

    @Override
    public void testCompress(AbstractTestCompression.TestCase testCase)
            throws Exception
    {
        // not yet supported
    }

    @Override
    protected io.airlift.compress.Compressor getCompressor()
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    protected Decompressor getDecompressor()
    {
        return new LzoDecompressor();
    }
}
