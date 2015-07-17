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
import io.airlift.compress.Decompressor;
import io.airlift.compress.HadoopNative;
import io.airlift.compress.MalformedInputException;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.nio.ByteBuffer;

public class HadoopLzoDecompressor
    implements Decompressor
{
    static {
        HadoopNative.requireHadoopNative();
    }

    private static final Configuration HADOOP_CONF = new Configuration();

    private final org.apache.hadoop.io.compress.Decompressor decompressor;

    public HadoopLzoDecompressor()
    {
        LzoCodec codec = new LzoCodec();
        codec.setConf(HADOOP_CONF);
        decompressor = codec.createDecompressor();
    }

    @Override
    public int decompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
            throws MalformedInputException
    {
        decompressor.reset();
        decompressor.setInput(input, inputOffset, inputLength);

        int offset = outputOffset;
        int outputLimit = outputOffset + maxOutputLength;
        while (!decompressor.finished() && offset < outputLimit) {
            try {
                offset += decompressor.decompress(output, offset, outputLimit - offset);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        return offset - outputOffset;
    }

    @Override
    public void decompress(ByteBuffer input, ByteBuffer output)
            throws MalformedInputException
    {
        throw new UnsupportedOperationException("not yet implemented");
    }
}
