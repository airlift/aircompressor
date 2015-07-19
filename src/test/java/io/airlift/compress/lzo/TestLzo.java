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
package io.airlift.compress.lzo;

import io.airlift.compress.AbstractTestCompression;
import io.airlift.compress.Decompressor;
import io.airlift.compress.HadoopNative;
import io.airlift.compress.benchmark.DataSet;
import io.airlift.compress.thirdparty.HadoopLzoCompressor;
import io.airlift.compress.thirdparty.HadoopLzoDecompressor;

public class TestLzo
    extends AbstractTestCompression
{
    static {
        HadoopNative.requireHadoopNative();
    }

    @Override
    public void testCompress(DataSet testCase)
            throws Exception
    {
        // not yet supported
    }

    @Override
    public void testCompressByteBufferHeapToHeap(DataSet dataSet)
            throws Exception
    {
        // not yet supported
    }

    @Override
    public void testCompressByteBufferHeapToDirect(DataSet dataSet)
            throws Exception
    {
        // not yet supported
    }

    @Override
    public void testCompressByteBufferDirectToHeap(DataSet dataSet)
            throws Exception
    {
        // not yet supported
    }

    @Override
    public void testCompressByteBufferDirectToDirect(DataSet dataSet)
            throws Exception
    {
        // not yet supported
    }

    @Override
    protected io.airlift.compress.Compressor getCompressor()
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    protected Decompressor getDecompressor()
    {
        return new LzoDecompressor();
    }

    @Override
    protected io.airlift.compress.Compressor getVerifyCompressor()
    {
        return new HadoopLzoCompressor();
    }

    @Override
    protected Decompressor getVerifyDecompressor()
    {
        return new HadoopLzoDecompressor();
    }
}
