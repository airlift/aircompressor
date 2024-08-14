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
package io.airlift.compress.v3.bzip2;

import io.airlift.compress.v3.hadoop.HadoopInputStream;
import io.airlift.compress.v3.hadoop.HadoopOutputStream;
import io.airlift.compress.v3.hadoop.HadoopStreams;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import static java.util.Collections.singletonList;

public class BZip2HadoopStreams
        implements HadoopStreams
{
    @Override
    public String getDefaultFileExtension()
    {
        return ".bz2";
    }

    @Override
    public List<String> getHadoopCodecName()
    {
        return singletonList("org.apache.hadoop.io.compress.BZip2Codec");
    }

    @Override
    public HadoopInputStream createInputStream(InputStream in)
    {
        return new BZip2HadoopInputStream(in);
    }

    @Override
    public HadoopOutputStream createOutputStream(OutputStream out)
    {
        return new BZip2HadoopOutputStream(out);
    }
}
