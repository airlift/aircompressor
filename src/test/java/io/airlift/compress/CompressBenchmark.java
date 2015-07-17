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
package io.airlift.compress;

import com.google.common.io.Files;
import io.airlift.compress.lz4.Lz4Compressor;
import io.airlift.compress.lz4.Lz4Decompressor;
import io.airlift.compress.snappy.ByteArrayOutputStream;
import io.airlift.compress.snappy.SnappyCodec;
import io.airlift.compress.snappy.SnappyCompressor;
import io.airlift.compress.snappy.SnappyDecompressor;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.util.Statistics;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 10)
@Warmup(iterations = 5)
@Fork(3)
public class CompressBenchmark
{
    static {
        HadoopNative.initialize();
    }

    private static final Configuration HADOOP_CONF = new Configuration();

    private byte[] data;

    private byte[] uncompressedBytes;

    private final SnappyCompressor snappyCompressor = new SnappyCompressor();
    private byte[] blockCompressedSnappy;
    private byte[] streamCompressSnappy;
    private SnappyCodec airliftSnappyCodec = new SnappyCodec();
    private org.apache.hadoop.io.compress.SnappyCodec hadoopSnappyCodec;

    private final LZ4Compressor jpountzLz4Compressor = LZ4Factory.fastestInstance().fastCompressor();
    private final Lz4Compressor airliftLz4Compressor = new Lz4Compressor();
    private byte[] blockCompressedLz4;

    @Setup
    public void prepare()
            throws IOException
    {
        data = getUncompressedData();
        uncompressedBytes = new byte[data.length];

        blockCompressedSnappy = new byte[snappyCompressor.maxCompressedLength(data.length)];
        // assume stream code will not add more that 10% overhead
        streamCompressSnappy = new byte[(int) (blockCompressedSnappy.length * 1.1) + 8];
        hadoopSnappyCodec = new org.apache.hadoop.io.compress.SnappyCodec();
        hadoopSnappyCodec.setConf(HADOOP_CONF);

        blockCompressedLz4 = new byte[airliftLz4Compressor.maxCompressedLength(data.length)];
    }

    /**
     * DO NOT call this from prepare!
     * Verify the implementations are working.  This executes all implementation code,
     * which can cause some implementations (e.g. Hadoop) to become highly virtualized
     * and thus slow.
     */
    public void verify()
            throws IOException
    {
        int written;

        Arrays.fill(uncompressedBytes, (byte) 0);
        written = blockAirliftSnappy(new BytesCounter());
        new SnappyDecompressor().decompress(blockCompressedSnappy, 0, written, uncompressedBytes, 0, uncompressedBytes.length);
        if (!Arrays.equals(data, uncompressedBytes)) {
            throw new IllegalStateException("broken decompressor: block airlift snappy");
        }

        Arrays.fill(uncompressedBytes, (byte) 0);
        written = blockXerialSnappy(new BytesCounter());
        new SnappyDecompressor().decompress(blockCompressedSnappy, 0, written, uncompressedBytes, 0, uncompressedBytes.length);
        if (!Arrays.equals(data, uncompressedBytes)) {
            throw new IllegalStateException("broken decompressor: block serial snappy");
        }

        Arrays.fill(uncompressedBytes, (byte) 0);
        written = streamAirliftSnappy(new BytesCounter());
        hadoopDecompress(airliftSnappyCodec, streamCompressSnappy, 0, written);
        if (!Arrays.equals(data, uncompressedBytes)) {
            throw new IllegalStateException("broken decompressor: stream airlift snappy");
        }

        Arrays.fill(uncompressedBytes, (byte) 0);
        written = streamHadoopSnappy(new BytesCounter());
        hadoopDecompress(hadoopSnappyCodec, streamCompressSnappy, 0, written);
        if (!Arrays.equals(data, uncompressedBytes)) {
            throw new IllegalStateException("broken decompressor: stream hadoop snappy");
        }

        Arrays.fill(uncompressedBytes, (byte) 0);
        written = blockAirliftLz4(new BytesCounter());
        new Lz4Decompressor().decompress(blockCompressedLz4, 0, written, uncompressedBytes, 0, uncompressedBytes.length);
        if (!Arrays.equals(data, uncompressedBytes)) {
            throw new IllegalStateException("broken decompressor: block serial snappy");
        }

        Arrays.fill(uncompressedBytes, (byte) 0);
        written = blockJpountzLz4Jni(new BytesCounter());
        new Lz4Decompressor().decompress(blockCompressedLz4, 0, written, uncompressedBytes, 0, uncompressedBytes.length);
        if (!Arrays.equals(data, uncompressedBytes)) {
            throw new IllegalStateException("broken decompressor: block serial snappy");
        }
    }

    private static byte[] getUncompressedData()
            throws IOException
    {
        return Files.toByteArray(new File("testdata/html"));
    }

    @Benchmark
    public int blockAirliftLz4(BytesCounter counter)
    {
        int written = airliftLz4Compressor.compress(data, 0, data.length, blockCompressedLz4, 0, blockCompressedLz4.length);
        counter.add(uncompressedBytes.length);
        return written;
    }

    @Benchmark
    public int blockJpountzLz4Jni(BytesCounter counter)
    {
        int written = jpountzLz4Compressor.compress(data, 0, data.length, blockCompressedLz4, 0, blockCompressedLz4.length);
        counter.add(uncompressedBytes.length);
        return written;
    }

    @Benchmark
    public int blockAirliftSnappy(BytesCounter counter)
    {
        int written = snappyCompressor.compress(data,
                0,
                data.length,
                blockCompressedSnappy,
                0,
                blockCompressedSnappy.length);

        counter.add(uncompressedBytes.length);
        return written;
    }

    @Benchmark
    public int blockXerialSnappy(BytesCounter counter)
            throws IOException
    {
        int written = org.xerial.snappy.Snappy.compress(data, 0, data.length, blockCompressedSnappy, 0);
        counter.add(uncompressedBytes.length);
        return written;
    }

    @Benchmark
    public int streamAirliftSnappy(BytesCounter counter)
            throws IOException
    {
        return streamHadoop(counter, airliftSnappyCodec, streamCompressSnappy);
    }

    @Benchmark
    public int streamHadoopSnappy(BytesCounter counter)
            throws IOException
    {
        return streamHadoop(counter, hadoopSnappyCodec, streamCompressSnappy);
    }

    private int streamHadoop(BytesCounter counter, CompressionCodec codec, byte[] compressed)
            throws IOException
    {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(compressed);

        OutputStream out = codec.createOutputStream(byteArrayOutputStream);
        out.write(data, 0, data.length);
        out.close();

        counter.add(uncompressedBytes.length);
        return byteArrayOutputStream.size();
    }

    @AuxCounters
    @State(Scope.Thread)
    public static class BytesCounter
    {
        private long bytes;

        @Setup(Level.Iteration)
        public void reset()
        {
            bytes = 0;
        }

        public void add(long bytes)
        {
            this.bytes += bytes;
        }

        public long getBytes()
        {
            return bytes;
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        CompressBenchmark verifyDecompressor = new CompressBenchmark();
        verifyDecompressor.prepare();
        verifyDecompressor.verify();

        Options opt = new OptionsBuilder()
//                .outputFormat(OutputFormatType.Silent)
                .include(".*" + CompressBenchmark.class.getSimpleName() + ".*")
//                .forks(1)
//                .warmupIterations(5)
//                .measurementIterations(10)
                .build();

        Collection<RunResult> results = new Runner(opt).run();

        for (RunResult result : results) {
            Statistics stats = result.getSecondaryResults().get("getBytes").getStatistics();
            System.out.printf("  %-19s %10s ± %10s (%5.2f%%) (N = %d, \u03B1 = 99.9%%)\n",
                    result.getPrimaryResult().getLabel(),
                    Util.toHumanReadableSpeed((long) stats.getMean()),
                    Util.toHumanReadableSpeed((long) stats.getMeanErrorAt(0.999)),
                    stats.getMeanErrorAt(0.999) * 100 / stats.getMean(),
                    stats.getN());
        }
        System.out.println();
    }

    private int hadoopDecompress(CompressionCodec codec, byte[] compressed, int compressedOffset, int compressedLength)
            throws IOException
    {
        InputStream in = codec.createInputStream(new ByteArrayInputStream(compressed, compressedOffset, compressedLength));

        int offset = 0;
        while (offset < uncompressedBytes.length) {
            offset += in.read(uncompressedBytes, offset, uncompressedBytes.length - offset);
        }

        in.close();
        return offset;
    }
}
