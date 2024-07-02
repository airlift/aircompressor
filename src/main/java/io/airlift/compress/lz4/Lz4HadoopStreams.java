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
package io.airlift.compress.lz4;

import io.airlift.compress.hadoop.HadoopInputStream;
import io.airlift.compress.hadoop.HadoopOutputStream;
import io.airlift.compress.hadoop.HadoopStreams;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import static java.util.Collections.singletonList;

public class Lz4HadoopStreams
        implements HadoopStreams
{
    private static final int DEFAULT_OUTPUT_BUFFER_SIZE = 256 * 1024;
    private final boolean useNative;
    private final int bufferSize;

    public Lz4HadoopStreams()
    {
        this(true, DEFAULT_OUTPUT_BUFFER_SIZE);
    }

    public Lz4HadoopStreams(boolean useNative, int bufferSize)
    {
        this.useNative = useNative && Lz4Native.isEnabled();
        this.bufferSize = bufferSize;
    }

    @Override
    public String getDefaultFileExtension()
    {
        return ".lz4";
    }

    @Override
    public List<String> getHadoopCodecName()
    {
        return singletonList("org.apache.hadoop.io.compress.Lz4Codec");
    }

    @Override
    public HadoopInputStream createInputStream(InputStream in)
    {
        Lz4Decompressor decompressor = useNative ? new Lz4NativeDecompressor() : new Lz4JavaDecompressor();
        return new Lz4HadoopInputStream(decompressor, in, bufferSize);
    }

    @Override
    public HadoopOutputStream createOutputStream(OutputStream out)
    {
        Lz4Compressor compressor = useNative ? new Lz4NativeCompressor() : new Lz4JavaCompressor();
        return new Lz4HadoopOutputStream(compressor, out, bufferSize);
    }
}
