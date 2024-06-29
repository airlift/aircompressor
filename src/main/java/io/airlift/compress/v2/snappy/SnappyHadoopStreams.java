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
package io.airlift.compress.v2.snappy;

import io.airlift.compress.v2.hadoop.HadoopInputStream;
import io.airlift.compress.v2.hadoop.HadoopOutputStream;
import io.airlift.compress.v2.hadoop.HadoopStreams;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import static java.util.Collections.singletonList;

public class SnappyHadoopStreams
        implements HadoopStreams
{
    private static final int DEFAULT_OUTPUT_BUFFER_SIZE = 256 * 1024;
    private final boolean useNative;
    private final int bufferSize;

    public SnappyHadoopStreams()
    {
        this(true, DEFAULT_OUTPUT_BUFFER_SIZE);
    }

    public SnappyHadoopStreams(boolean useNative, int bufferSize)
    {
        this.useNative = useNative && SnappyNative.isEnabled();
        this.bufferSize = bufferSize;
    }

    @Override
    public String getDefaultFileExtension()
    {
        return ".snappy";
    }

    @Override
    public List<String> getHadoopCodecName()
    {
        return singletonList("org.apache.hadoop.io.compress.SnappyCodec");
    }

    @Override
    public HadoopInputStream createInputStream(InputStream in)
    {
        SnappyDecompressor decompressor = useNative ? new SnappyNativeDecompressor() : new SnappyJavaDecompressor();
        return new SnappyHadoopInputStream(decompressor, in);
    }

    @Override
    public HadoopOutputStream createOutputStream(OutputStream out)
    {
        SnappyCompressor compressor = useNative ? new SnappyNativeCompressor() : new SnappyJavaCompressor();
        return new SnappyHadoopOutputStream(compressor, out, bufferSize);
    }
}
