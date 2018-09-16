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
import io.airlift.compress.MalformedInputException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkArgument;

public class HadoopLzoDecompressor
        implements Decompressor
{
    private static final int MAX_OUTPUT_BUFFER_SIZE = 128 * 1024 * 1024;
    private final org.apache.hadoop.io.compress.Decompressor decompressor;

    public HadoopLzoDecompressor()
    {
        Configuration hadoopConf = new Configuration();
        hadoopConf.set("io.compression.codec.lzo.class", "com.hadoop.compression.lzo.LzoCodec");
        hadoopConf.set("io.compression.codec.lzo.compressor", "LZO1X_999");
        hadoopConf.set("io.compression.codec.lzo.compression.level", "3");
        CompressionCodec codec = ReflectionUtils.newInstance(
                LzoCodec.class, hadoopConf);
        decompressor = CodecPool.getDecompressor(codec);
    }

    @Override
    public int decompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
            throws MalformedInputException
    {
        checkArgument(maxOutputLength < MAX_OUTPUT_BUFFER_SIZE, "output size " + maxOutputLength + " exceed maximum size : " + maxOutputLength);
        // nothing decompress to nothing
        if (inputLength == 0) {
            return 0;
        }

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
