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
package io.airlift.compress.v3.benchmark;

import io.airlift.compress.v3.Compressor;
import io.airlift.compress.v3.Decompressor;
import io.airlift.compress.v3.HadoopCodecCompressor;
import io.airlift.compress.v3.HadoopCodecDecompressor;
import io.airlift.compress.v3.lz4.Lz4Codec;
import io.airlift.compress.v3.lz4.Lz4JavaCompressor;
import io.airlift.compress.v3.lz4.Lz4JavaDecompressor;
import io.airlift.compress.v3.lz4.Lz4NativeCompressor;
import io.airlift.compress.v3.lz4.Lz4NativeDecompressor;
import io.airlift.compress.v3.lzo.LzoCodec;
import io.airlift.compress.v3.lzo.LzoCompressor;
import io.airlift.compress.v3.lzo.LzoDecompressor;
import io.airlift.compress.v3.snappy.SnappyCodec;
import io.airlift.compress.v3.snappy.SnappyJavaCompressor;
import io.airlift.compress.v3.snappy.SnappyJavaDecompressor;
import io.airlift.compress.v3.snappy.SnappyNativeCompressor;
import io.airlift.compress.v3.snappy.SnappyNativeDecompressor;
import io.airlift.compress.v3.thirdparty.HadoopLzoCompressor;
import io.airlift.compress.v3.thirdparty.HadoopLzoDecompressor;
import io.airlift.compress.v3.thirdparty.JPountzLz4Compressor;
import io.airlift.compress.v3.thirdparty.JPountzLz4Decompressor;
import io.airlift.compress.v3.thirdparty.JdkDeflateCompressor;
import io.airlift.compress.v3.thirdparty.JdkInflateDecompressor;
import io.airlift.compress.v3.thirdparty.XerialSnappyCompressor;
import io.airlift.compress.v3.thirdparty.XerialSnappyDecompressor;
import io.airlift.compress.v3.thirdparty.ZstdJniCompressor;
import io.airlift.compress.v3.thirdparty.ZstdJniDecompressor;
import io.airlift.compress.v3.zstd.ZstdJavaCompressor;
import io.airlift.compress.v3.zstd.ZstdJavaDecompressor;
import io.airlift.compress.v3.zstd.ZstdNativeCompressor;
import io.airlift.compress.v3.zstd.ZstdNativeDecompressor;
import net.jpountz.lz4.LZ4Factory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;

public enum Algorithm
{
    airlift_lz4(new Lz4JavaDecompressor(), new Lz4JavaCompressor()),
    airlift_lz4_native(new Lz4NativeDecompressor(), new Lz4NativeCompressor()),
    airlift_snappy(new SnappyJavaDecompressor(), new SnappyJavaCompressor()),
    airlift_snappy_native(new SnappyNativeDecompressor(), new SnappyNativeCompressor()),
    airlift_lzo(new LzoDecompressor(), new LzoCompressor()),
    airlift_zstd(new ZstdJavaDecompressor(), new ZstdJavaCompressor()),
    airlift_zstd_native(new ZstdNativeDecompressor(), new ZstdNativeCompressor()),

    airlift_lz4_stream(new Lz4Codec(), new Lz4JavaCompressor()),
    airlift_snappy_stream(new SnappyCodec(), new SnappyJavaCompressor()),
    airlift_lzo_stream(new LzoCodec(), new LzoCompressor()),

    jpountz_lz4_jni(new JPountzLz4Decompressor(LZ4Factory.nativeInstance()), new JPountzLz4Compressor(LZ4Factory.nativeInstance())),
    jpountz_lz4_safe(new JPountzLz4Decompressor(LZ4Factory.safeInstance()), new JPountzLz4Compressor(LZ4Factory.safeInstance())),
    jpountz_lz4_unsafe(new JPountzLz4Decompressor(LZ4Factory.unsafeInstance()), new JPountzLz4Compressor(LZ4Factory.unsafeInstance())),
    xerial_snappy(new XerialSnappyDecompressor(), new XerialSnappyCompressor()),
    hadoop_lzo(new HadoopLzoDecompressor(), new HadoopLzoCompressor()),
    zstd_jni(new ZstdJniDecompressor(), new ZstdJniCompressor(3)),

    hadoop_lz4_stream(new org.apache.hadoop.io.compress.Lz4Codec(), new Lz4JavaCompressor()),
    hadoop_snappy_stream(new org.apache.hadoop.io.compress.SnappyCodec(), new SnappyJavaCompressor()),
    hadoop_lzo_stream(new org.anarres.lzo.hadoop.codec.LzoCodec(), new LzoCompressor()),

    java_zip_stream(new JdkInflateDecompressor(), new JdkDeflateCompressor()),
    hadoop_gzip_stream(new org.apache.hadoop.io.compress.GzipCodec(), new LzoCompressor());

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
