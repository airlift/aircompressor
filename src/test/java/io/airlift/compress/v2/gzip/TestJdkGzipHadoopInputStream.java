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
package io.airlift.compress.v2.gzip;

import com.google.common.io.ByteStreams;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.util.zip.GZIPOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

class TestJdkGzipHadoopInputStream
{
    @Test
    void testGzipInputStreamBug()
            throws IOException
    {
        byte[] part1 = zip("hello ".getBytes());
        byte[] part2 = zip("world".getBytes());

        InputStream compressed = new SequenceInputStream(new ByteArrayInputStream(part1), new ByteArrayInputStream(part2));
        byte[] data = ByteStreams.toByteArray(new JdkGzipHadoopStreams().createInputStream(compressed));

        assertThat(data).isEqualTo("hello world".getBytes());
    }

    private static byte[] zip(byte[] data)
            throws IOException
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (OutputStream gzipOut = new GZIPOutputStream(out)) {
            gzipOut.write(data);
        }
        return out.toByteArray();
    }
}
