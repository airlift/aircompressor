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
package io.airlift.compress.v2.lzo;

import io.airlift.compress.v2.hadoop.HadoopInputStream;
import io.airlift.compress.v2.hadoop.HadoopOutputStream;
import io.airlift.compress.v2.hadoop.HadoopStreams;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import static java.util.Collections.singletonList;

public class LzopHadoopStreams
        implements HadoopStreams
{
    private static final int DEFAULT_OUTPUT_BUFFER_SIZE = 256 * 1024;
    private final int bufferSize;

    public LzopHadoopStreams()
    {
        this(DEFAULT_OUTPUT_BUFFER_SIZE);
    }

    public LzopHadoopStreams(int bufferSize)
    {
        this.bufferSize = bufferSize;
    }

    @Override
    public String getDefaultFileExtension()
    {
        return ".lzo";
    }

    @Override
    public List<String> getHadoopCodecName()
    {
        return singletonList("com.hadoop.compression.lzo.LzopCodec");
    }

    @Override
    public HadoopInputStream createInputStream(InputStream in)
            throws IOException
    {
        return new LzopHadoopInputStream(in, bufferSize);
    }

    @Override
    public HadoopOutputStream createOutputStream(OutputStream out)
            throws IOException
    {
        return new LzopHadoopOutputStream(out, bufferSize);
    }
}
