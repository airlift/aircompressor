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
package io.airlift.compress.v2.lz4;

import io.airlift.compress.v2.AbstractTestCompression;
import io.airlift.compress.v2.Compressor;
import io.airlift.compress.v2.Decompressor;
import io.airlift.compress.v2.HadoopCodecCompressor;
import io.airlift.compress.v2.HadoopCodecDecompressor;
import io.airlift.compress.v2.HadoopNative;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;

class TestLz4Codec
        extends AbstractTestCompression
{
    static {
        HadoopNative.requireHadoopNative();
    }

    private final CompressionCodec verifyCodec;

    TestLz4Codec()
    {
        org.apache.hadoop.io.compress.Lz4Codec codec = new org.apache.hadoop.io.compress.Lz4Codec();
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
        return new HadoopCodecCompressor(new Lz4Codec(), new Lz4JavaCompressor());
    }

    @Override
    protected Decompressor getDecompressor()
    {
        return new HadoopCodecDecompressor(new Lz4Codec());
    }

    @Override
    protected Compressor getVerifyCompressor()
    {
        return new HadoopCodecCompressor(verifyCodec, new Lz4JavaCompressor());
    }

    @Override
    protected Decompressor getVerifyDecompressor()
    {
        return new HadoopCodecDecompressor(verifyCodec);
    }
}
