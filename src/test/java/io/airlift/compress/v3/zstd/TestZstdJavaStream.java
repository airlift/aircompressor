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

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class TestZstdJavaStream
        extends AbstractTestZstdStream
{
    @Override
    protected ZstdOutputStream createCompressingOutputStream(OutputStream out)
            throws IOException
    {
        return new ZstdOutputStream(out);
    }

    @Override
    protected ZstdInputStream createDecompressingInputStream(InputStream in)
    {
        return new ZstdInputStream(in);
    }

    @Test
    void testPublicStreamsAreExtensible()
            throws Exception
    {
        byte[] original = new byte[10_000];
        new Random(5).nextBytes(original);

        // every write of a subclass goes through its write(byte[], int, int)
        AtomicInteger writes = new AtomicInteger();
        ByteArrayOutputStream compressed = new ByteArrayOutputStream();
        try (OutputStream out = new ZstdOutputStream(compressed)
        {
            @Override
            public void write(byte[] buffer, int offset, int length)
                    throws IOException
            {
                writes.incrementAndGet();
                super.write(buffer, offset, length);
            }
        }) {
            out.write(original);
            out.write(original, 0, 10);
            out.write(42);
        }
        assertThat(writes).hasValue(3);

        AtomicInteger reads = new AtomicInteger();
        try (InputStream in = new ZstdInputStream(new ByteArrayInputStream(compressed.toByteArray()))
        {
            @Override
            public int read(byte[] buffer, int offset, int length)
                    throws IOException
            {
                reads.incrementAndGet();
                return super.read(buffer, offset, length);
            }
        }) {
            assertThat(in.read()).isEqualTo(original[0] & 0xFF);
            assertThat(reads).hasValue(1);
            assertThat(in.readNBytes(original.length - 1)).isEqualTo(Arrays.copyOfRange(original, 1, original.length));
        }
    }

    @Test
    void testDefaultCompressionLevelCanBeRequested()
            throws Exception
    {
        byte[] original = new byte[10_000];
        new Random(6).nextBytes(original);

        // level 0 means the default level, whichever implementation the factory picks
        for (int level : new int[] {0, 3}) {
            ByteArrayOutputStream compressed = new ByteArrayOutputStream();
            try (OutputStream out = ZstdOutputStream.create(compressed, level)) {
                out.write(original);
            }
            try (InputStream in = new ZstdInputStream(new ByteArrayInputStream(compressed.toByteArray()))) {
                assertThat(in.readAllBytes()).isEqualTo(original);
            }
        }
    }
}
