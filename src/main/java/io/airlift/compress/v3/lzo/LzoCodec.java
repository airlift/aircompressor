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
package io.airlift.compress.v3.lzo;

import io.airlift.compress.v3.hadoop.CodecAdapter;
import org.apache.hadoop.conf.Configuration;

import java.util.Optional;

import static org.apache.hadoop.fs.CommonConfigurationKeys.IO_COMPRESSION_CODEC_LZO_BUFFERSIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IO_COMPRESSION_CODEC_LZO_BUFFERSIZE_KEY;

public class LzoCodec
        extends CodecAdapter
{
    public LzoCodec()
    {
        super(configuration -> new LzoHadoopStreams(getBufferSize(configuration)));
    }

    static int getBufferSize(Optional<Configuration> configuration)
    {
        // The configured size is the writer's block size and the reader's initial buffer.
        // Readers grow the buffer when a file was written with larger blocks.
        return configuration
            .map(conf -> conf.getInt(IO_COMPRESSION_CODEC_LZO_BUFFERSIZE_KEY, IO_COMPRESSION_CODEC_LZO_BUFFERSIZE_DEFAULT))
            .orElse(IO_COMPRESSION_CODEC_LZO_BUFFERSIZE_DEFAULT);
    }
}
