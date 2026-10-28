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

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.zip.Deflater;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestJdkDeflateHadoopOutputStream
{
    @Test
    void testCloseReleasesDeflater()
            throws ReflectiveOperationException, IOException
    {
        JdkDeflateHadoopOutputStream output = new JdkDeflateHadoopOutputStream(OutputStream.nullOutputStream(), 8192);
        Deflater deflater = getDeflater(output);
        output.write(new byte[8192]);
        output.close();
        output.close();
        assertThatThrownBy(deflater::reset).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testFailedFinishReleasesDeflater()
            throws ReflectiveOperationException
    {
        IOException failure = new IOException("write failed");
        JdkDeflateHadoopOutputStream output = new JdkDeflateHadoopOutputStream(new OutputStream()
        {
            @Override
            public void write(int value)
                    throws IOException
            {
                throw failure;
            }
        }, 8192);
        Deflater deflater = getDeflater(output);
        assertThatThrownBy(output::close).isSameAs(failure);
        assertThatThrownBy(deflater::reset).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testFailedDestinationCloseReleasesDeflater()
            throws ReflectiveOperationException
    {
        IOException failure = new IOException("close failed");
        JdkDeflateHadoopOutputStream output = new JdkDeflateHadoopOutputStream(new OutputStream()
        {
            @Override
            public void write(int value) {}

            @Override
            public void close()
                    throws IOException
            {
                throw failure;
            }
        }, 8192);
        Deflater deflater = getDeflater(output);
        assertThatThrownBy(output::close).isSameAs(failure);
        assertThatThrownBy(deflater::reset).isInstanceOf(IllegalStateException.class);
    }

    private static Deflater getDeflater(JdkDeflateHadoopOutputStream output)
            throws ReflectiveOperationException
    {
        Field field = JdkDeflateHadoopOutputStream.class.getDeclaredField("deflater");
        field.setAccessible(true);
        return (Deflater) field.get(output);
    }
}
