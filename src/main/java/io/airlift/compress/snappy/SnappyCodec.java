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
package io.airlift.compress.snappy;

import io.airlift.compress.hadoop.CodecAdapter;
import org.apache.hadoop.conf.Configuration;

import java.util.Optional;

import static org.apache.hadoop.fs.CommonConfigurationKeys.IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_KEY;

public class SnappyCodec
        extends CodecAdapter
{
    public SnappyCodec()
    {
        super(configuration -> new SnappyHadoopStreams(true, getBufferSize(configuration)));
    }

    private static int getBufferSize(Optional<Configuration> configuration)
    {
        // Favor using the configured buffer size.  This is not as critical for Snappy
        // since Snappy always writes the compressed chunk size, so we always know the
        // correct buffer size to create.
        return configuration
                .map(conf -> conf.getInt(IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_KEY, IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_DEFAULT))
                .orElse(IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_DEFAULT);
    }
}
