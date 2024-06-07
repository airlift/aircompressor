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

import io.airlift.compress.AbstractTestCompression;
import io.airlift.compress.Compressor;
import io.airlift.compress.Decompressor;
import io.airlift.compress.HadoopCodecCompressor;
import io.airlift.compress.HadoopCodecDecompressor;
import io.airlift.compress.HadoopNative;
import io.airlift.compress.thirdparty.HadoopLzoCompressor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;

public class TestLzoCodec
        extends AbstractTestCompression
{
    static {
        HadoopNative.requireHadoopNative();
    }

    private final CompressionCodec verifyCodec;

    TestLzoCodec()
    {
        com.hadoop.compression.lzo.LzoCodec codec = new com.hadoop.compression.lzo.LzoCodec();
        codec.setConf(new Configuration());
        this.verifyCodec = codec;
    }

    @Override
    protected boolean isMemorySegmentSupported()
    {
        return false;
    }

    @Override
    protected Compressor getCompressor()
    {
        return new HadoopCodecCompressor(new LzoCodec(), new LzoCompressor());
    }

    @Override
    protected Decompressor getDecompressor()
    {
        return new HadoopCodecDecompressor(new LzoCodec());
    }

    @Override
    protected Compressor getVerifyCompressor()
    {
        return new HadoopCodecCompressor(verifyCodec, new HadoopLzoCompressor());
    }

    @Override
    protected Decompressor getVerifyDecompressor()
    {
        return new HadoopCodecDecompressor(verifyCodec);
    }
}
