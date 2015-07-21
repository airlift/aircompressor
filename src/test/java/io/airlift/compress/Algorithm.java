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
package io.airlift.compress;

import io.airlift.compress.lz4.Lz4Codec;
import io.airlift.compress.lz4.Lz4Compressor;
import io.airlift.compress.lz4.Lz4Decompressor;
import io.airlift.compress.lzo.LzoCodec;
import io.airlift.compress.lzo.LzoCompressor;
import io.airlift.compress.lzo.LzoDecompressor;
import io.airlift.compress.snappy.SnappyCodec;
import io.airlift.compress.snappy.SnappyCompressor;
import io.airlift.compress.snappy.SnappyDecompressor;
import io.airlift.compress.thirdparty.HadoopLzoCompressor;
import io.airlift.compress.thirdparty.HadoopLzoDecompressor;
import io.airlift.compress.thirdparty.JPountzLz4JniCompressor;
import io.airlift.compress.thirdparty.JPountzLz4JniDecompressor;
import io.airlift.compress.thirdparty.XerialSnappyCompressor;
import io.airlift.compress.thirdparty.XerialSnappyDecompressor;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;

public enum Algorithm
{
    airlift_lz4(new Lz4Decompressor(), new Lz4Compressor()),
    airlift_snappy(new SnappyDecompressor(), new SnappyCompressor()),
    airlift_lzo(new LzoDecompressor(), new LzoCompressor()),
    airlift_lz4_stream(new Lz4Codec(), new Lz4Compressor()),
    airlift_snappy_stream(new SnappyCodec(), new SnappyCompressor()),
    airlift_lzo_stream(new LzoCodec(), new LzoCompressor()),
    jpountz_lz4_jni(new JPountzLz4JniDecompressor(), new JPountzLz4JniCompressor()),
    xerial_snappy(new XerialSnappyDecompressor(), new XerialSnappyCompressor()),
    hadoop_lzo(new HadoopLzoDecompressor(), new HadoopLzoCompressor()),
    hadoop_lz4_stream(new org.apache.hadoop.io.compress.Lz4Codec(), new Lz4Compressor()),
    hadoop_snappy_stream(new org.apache.hadoop.io.compress.SnappyCodec(), new SnappyCompressor()),
    hadoop_lzo_stream(new org.anarres.lzo.hadoop.codec.LzoCodec(), new LzoCompressor());

    private final Decompressor decompressor;
    private final Compressor compressor;

    Algorithm(CompressionCodec compressionCodec, Compressor compressor)
    {
        if (compressionCodec instanceof Configurable) {
            ((Configurable) compressionCodec).setConf(new Configuration());
        }
        this.decompressor = new HadoopCodecDecompressor(compressionCodec);
        this.compressor = new HadoopCodecCompressor(compressionCodec, compressor);
    }

    Algorithm(Decompressor decompressor, Compressor compressor)
    {
        this.decompressor = decompressor;
        this.compressor = compressor;
    }

    public Compressor getCompressor()
    {
        return compressor;
    }

    public Decompressor getDecompressor()
    {
        return decompressor;
    }
}
