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
package io.airlift.compress.v3.zstd;

import io.airlift.compress.v3.hadoop.CodecAdapter;
import org.apache.hadoop.io.compress.CompressionCodec;

// ZstdCodec uses the native streams when they are available, so run the same suite against the Java streams
class TestZstdJavaCodecByteAtATime
        extends TestZstdCodecByteAtATime
{
    @Override
    protected CompressionCodec createCodec()
    {
        return new CodecAdapter(configuration -> new ZstdHadoopStreams(false));
    }
}
