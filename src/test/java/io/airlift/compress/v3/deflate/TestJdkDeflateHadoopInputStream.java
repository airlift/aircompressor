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
package io.airlift.compress.v3.deflate;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.DeflaterOutputStream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestJdkDeflateHadoopInputStream
{
    @Test
    void testZeroLengthReadDoesNotReadInput()
            throws IOException
    {
        try (InputStream input = new JdkDeflateHadoopInputStream(InputStream.nullInputStream(), 1024)) {
            assertThat(input.read(new byte[0])).isZero();
            assertThat(input.read(new byte[1], 1, 0)).isZero();
        }
    }

    @Test
    void testZeroLengthReadAtEndOfStream()
            throws IOException
    {
        byte[] data = "hello world".getBytes(UTF_8);
        ByteArrayOutputStream compressed = new ByteArrayOutputStream();
        try (DeflaterOutputStream output = new DeflaterOutputStream(compressed)) {
            output.write(data);
        }
        try (InputStream input = new JdkDeflateHadoopInputStream(new ByteArrayInputStream(compressed.toByteArray()), 1024)) {
            assertThat(input.readAllBytes()).isEqualTo(data);
            assertThat(input.read(new byte[0])).isZero();
            assertThat(input.read()).isEqualTo(-1);
        }
    }

    @Test
    void testZeroLengthReadChecksBounds()
            throws IOException
    {
        try (InputStream input = new JdkDeflateHadoopInputStream(InputStream.nullInputStream(), 1024)) {
            assertThatThrownBy(() -> input.read(new byte[1], -1, 0)).isInstanceOf(IndexOutOfBoundsException.class);
            assertThatThrownBy(() -> input.read(new byte[1], 2, 0)).isInstanceOf(IndexOutOfBoundsException.class);
            assertThatThrownBy(() -> input.read(null, 0, 0)).isInstanceOf(NullPointerException.class);
        }
    }
}
