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
package io.airlift.compress.thirdparty;

import com.hadoop.compression.lzo.LzoCodec;
import io.airlift.compress.Compressor;
import io.airlift.compress.HadoopNative;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.nio.ByteBuffer;

public class HadoopLzoCompressor
    implements Compressor
{
    static {
        HadoopNative.requireHadoopNative();
    }

    private static final Configuration HADOOP_CONF = new Configuration();

    private final org.apache.hadoop.io.compress.Compressor compressor;

    public HadoopLzoCompressor()
    {
        LzoCodec codec = new LzoCodec();
        codec.setConf(HADOOP_CONF);
        compressor = codec.createCompressor();
    }

    @Override
    public int maxCompressedLength(int uncompressedSize)
    {
        return uncompressedSize + (uncompressedSize / 16) + 64 + 3;
    }

    @Override
    public int compress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
    {
        compressor.reset();
        compressor.setInput(input, inputOffset, inputLength);
        compressor.finish();

        int offset = outputOffset;
        int outputLimit = outputOffset + maxOutputLength;
        while (!compressor.finished() && offset < outputLimit) {
            try {
                offset += compressor.compress(output, offset, outputLimit - offset);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        if (!compressor.finished()) {
            throw new RuntimeException("not enough space in output buffer");
        }

        return offset - outputOffset;
    }

    @Override
    public void compress(ByteBuffer input, ByteBuffer output)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }
}
