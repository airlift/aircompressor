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

import io.airlift.compress.hadoop.CodecAdapter;
import org.apache.hadoop.conf.Configuration;

import java.util.Optional;

import static org.apache.hadoop.fs.CommonConfigurationKeys.IO_COMPRESSION_CODEC_LZ4_BUFFERSIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IO_COMPRESSION_CODEC_LZ4_BUFFERSIZE_KEY;

public class Lz4Codec
        extends CodecAdapter
{
    public Lz4Codec()
    {
        super(configuration -> new Lz4HadoopStreams(getBufferSize(configuration)));
    }

    private static int getBufferSize(Optional<Configuration> configuration)
    {
        // To decode a LZ4 block we must preallocate an output buffer, but
        // the Hadoop block stream format does not include the uncompressed
        // size of chunks.  Instead, we must rely on the "configured"
        // maximum buffer size used by the writer of the file.
        return configuration
                .map(conf -> conf.getInt(IO_COMPRESSION_CODEC_LZ4_BUFFERSIZE_KEY, IO_COMPRESSION_CODEC_LZ4_BUFFERSIZE_DEFAULT))
                .orElse(IO_COMPRESSION_CODEC_LZ4_BUFFERSIZE_DEFAULT);
    }
}
