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
package io.airlift.compress.deflate;

import io.airlift.compress.AbstractTestCompression;
import io.airlift.compress.Compressor;
import io.airlift.compress.Decompressor;
import io.airlift.compress.HadoopCodecCompressor;
import io.airlift.compress.HadoopCodecDecompressor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;

public class TestJdkDeflateCodec
        extends AbstractTestCompression
{
    private final CompressionCodec verifyCodec;

    public TestJdkDeflateCodec()
    {
        DefaultCodec codec = new DefaultCodec();
        codec.setConf(new Configuration());
        this.verifyCodec = codec;
    }

    @Override
    protected boolean isByteBufferSupported()
    {
        return false;
    }

    @Override
    protected Compressor getCompressor()
    {
        return new HadoopCodecCompressor(new JdkDeflateCodec(), new MockJdkDeflateCompressor());
    }

    @Override
    protected Decompressor getDecompressor()
    {
        return new HadoopCodecDecompressor(new JdkDeflateCodec());
    }

    @Override
    protected Compressor getVerifyCompressor()
    {
        return new HadoopCodecCompressor(verifyCodec, new MockJdkDeflateCompressor());
    }

    @Override
    protected Decompressor getVerifyDecompressor()
    {
        return new HadoopCodecDecompressor(verifyCodec);
    }
}
