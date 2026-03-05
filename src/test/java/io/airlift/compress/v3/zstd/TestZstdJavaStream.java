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
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the pure Java Zstd stream implementation (ZstdJavaInputStream/ZstdJavaOutputStream).
 */
class TestZstdJavaStream
        extends AbstractTestZstdStream
{
    @Override
    protected OutputStream createCompressingOutputStream(OutputStream out)
            throws IOException
    {
        return new ZstdJavaOutputStream(out);
    }

    @Override
    protected InputStream createDecompressingInputStream(InputStream in)
    {
        return new ZstdJavaInputStream(in);
    }

    @Test
    void testInteropWithNativeStreams()
            throws Exception
    {
        if (!ZstdNative.isEnabled()) {
            return;
        }

        byte[] original = new byte[50000];
        new Random(111).nextBytes(original);

        // Compress with Java, decompress with native
        byte[] compressed = compress(original);
        byte[] uncompressed = nativeDecompress(compressed);
        assertThat(uncompressed).isEqualTo(original);

        // Compress with native, decompress with Java
        compressed = nativeCompress(original);
        uncompressed = decompress(compressed);
        assertThat(uncompressed).isEqualTo(original);
    }

    private static byte[] nativeCompress(byte[] original)
            throws IOException
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (OutputStream zstdOut = new ZstdNativeOutputStream(out)) {
            zstdOut.write(original);
        }
        return out.toByteArray();
    }

    private static byte[] nativeDecompress(byte[] compressed)
            throws IOException
    {
        return new ZstdNativeInputStream(new ByteArrayInputStream(compressed)).readAllBytes();
    }
}
