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
import org.junit.jupiter.api.condition.EnabledIf;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the native Zstd stream implementation (ZstdNativeInputStream/ZstdNativeOutputStream).
 */
@EnabledIf("isNativeEnabled")
class TestZstdNativeStream
        extends AbstractTestZstdStream
{
    static boolean isNativeEnabled()
    {
        return ZstdNative.isEnabled();
    }

    @Override
    protected OutputStream createCompressingOutputStream(OutputStream out)
            throws IOException
    {
        return new ZstdNativeOutputStream(out);
    }

    @Override
    protected InputStream createDecompressingInputStream(InputStream in)
            throws IOException
    {
        return new ZstdNativeInputStream(in);
    }

    @Test
    void testFinishWithoutClosingSource()
            throws Exception
    {
        byte[] original = new byte[10000];
        new Random(222).nextBytes(original);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ZstdNativeOutputStream zstdOut = new ZstdNativeOutputStream(out);
        zstdOut.write(original);
        zstdOut.finishWithoutClosingSource();
        // Stream should be finished but underlying stream still open
        byte[] compressed = out.toByteArray();

        byte[] uncompressed = decompress(compressed);
        assertThat(uncompressed).isEqualTo(original);

        // Should be able to write more to underlying stream
        out.write(new byte[] {1, 2, 3});
    }

    @Test
    void testCompressionLevel()
            throws Exception
    {
        byte[] original = new byte[50000];
        Arrays.fill(original, (byte) 'x');

        // Test different compression levels
        byte[] compressedDefault = compress(original);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (OutputStream zstdOut = new ZstdNativeOutputStream(out, 19)) {  // Max level
            zstdOut.write(original);
        }
        byte[] compressedHigh = out.toByteArray();

        // Higher compression level should produce smaller output (or same)
        assertThat(compressedHigh.length).isLessThanOrEqualTo(compressedDefault.length);

        // Both should decompress correctly
        assertThat(decompress(compressedDefault)).isEqualTo(original);
        assertThat(decompress(compressedHigh)).isEqualTo(original);
    }
}
