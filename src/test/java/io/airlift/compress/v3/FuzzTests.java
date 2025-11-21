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
package io.airlift.compress.v3;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.code_intelligence.jazzer.junit.FuzzTest;
import io.airlift.compress.v3.bzip2.BZip2Codec;
import io.airlift.compress.v3.deflate.DeflateCompressor;
import io.airlift.compress.v3.deflate.DeflateDecompressor;
import io.airlift.compress.v3.deflate.JdkDeflateCodec;
import io.airlift.compress.v3.gzip.JdkGzipCodec;
import io.airlift.compress.v3.lz4.Lz4Codec;
import io.airlift.compress.v3.lz4.Lz4JavaCompressor;
import io.airlift.compress.v3.lz4.Lz4JavaDecompressor;
import io.airlift.compress.v3.lzo.LzoCodec;
import io.airlift.compress.v3.lzo.LzoCompressor;
import io.airlift.compress.v3.lzo.LzoDecompressor;
import io.airlift.compress.v3.lzo.LzopCodec;
import io.airlift.compress.v3.snappy.SnappyCodec;
import io.airlift.compress.v3.snappy.SnappyJavaCompressor;
import io.airlift.compress.v3.snappy.SnappyJavaDecompressor;
import io.airlift.compress.v3.zstd.ZstdCodec;
import io.airlift.compress.v3.zstd.ZstdJavaCompressor;
import io.airlift.compress.v3.zstd.ZstdJavaDecompressor;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.lz4.BlockLZ4CompressorInputStream;
import org.apache.commons.compress.compressors.lz4.BlockLZ4CompressorOutputStream;
import org.apache.commons.compress.compressors.snappy.SnappyCompressorInputStream;
import org.apache.commons.compress.compressors.snappy.SnappyCompressorOutputStream;
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorInputStream;
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorOutputStream;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

public class FuzzTests
{
    private static final int MAX_EXPANSION_RATIO = 80;

    // ============================================================================================
    // Block Compression
    // ============================================================================================
    private record BlockAlgorithm(
            String name,
            Supplier<Compressor> compressor,
            Supplier<Decompressor> decompressor
    ) {}

    private static final List<BlockAlgorithm> BLOCK_ALGORITHMS = List.of(
            new BlockAlgorithm("lz4-java", Lz4JavaCompressor::new, Lz4JavaDecompressor::new),
            new BlockAlgorithm("lz4-raw", Lz4JavaCompressor::new, Lz4JavaDecompressor::new),
            new BlockAlgorithm("snappy", SnappyJavaCompressor::new, SnappyJavaDecompressor::new),
            new BlockAlgorithm("zstd", ZstdJavaCompressor::new, ZstdJavaDecompressor::new),
            new BlockAlgorithm("lzo", LzoCompressor::new, LzoDecompressor::new),
            new BlockAlgorithm("deflate", DeflateCompressor::new, DeflateDecompressor::new));

    /**
     * Verifies that data compressed with Block algorithms (lz4, snappy, zstd, lzo, deflate)
     * returns to its exact original state after decompression.
     */
    @FuzzTest
    public void fuzzBlockRoundTrip(FuzzedDataProvider fdp)
    {
        BlockAlgorithm algorithm = fdp.pickValue(BLOCK_ALGORITHMS);
        byte[] originalData = fdp.consumeRemainingAsBytes();

        Compressor compressor = algorithm.compressor().get();
        Decompressor decompressor = algorithm.decompressor().get();

        // Compress
        byte[] compressed = new byte[compressor.maxCompressedLength(originalData.length)];
        int compressedSize = compressor.compress(originalData, 0, originalData.length, compressed, 0, compressed.length);

        // Decompress
        byte[] uncompressed = new byte[originalData.length];
        decompressor.decompress(compressed, 0, compressedSize, uncompressed, 0, uncompressed.length);

        // Validate round trip
        if (!Arrays.equals(originalData, uncompressed)) {
            throw new RuntimeException("Block round trip mismatch for " + algorithm);
        }
    }

    /**
     * Feeds random data to Block decompressors.
     */
    @FuzzTest
    public void fuzzBlockDecompress(FuzzedDataProvider fdp)
    {
        BlockAlgorithm algorithm = fdp.pickValue(BLOCK_ALGORITHMS);
        if (algorithm.name.equals("deflate")) {
            return;
        }

        byte[] inputData = fdp.consumeRemainingAsBytes();
        Decompressor decompressor = algorithm.decompressor().get();

        try {
            byte[] uncompressed = new byte[inputData.length];
            decompressor.decompress(inputData, 0, inputData.length, uncompressed, 0, uncompressed.length);
        }
        catch (IllegalArgumentException | IllegalStateException | MalformedInputException
                 | IndexOutOfBoundsException | ArithmeticException ignored) {
        }
    }

    // ============================================================================================
    // Aircompressor Streams
    // ============================================================================================
    private record AircompressorAlgorithm(
            String name,
            Supplier<CompressionCodec> codecSupplier
    ) {}

    private static final List<AircompressorAlgorithm> AIRCOMPRESSOR_ALGORITHMS = List.of(
            new AircompressorAlgorithm("lzo", LzoCodec::new),
            new AircompressorAlgorithm("lzop", LzopCodec::new),
            new AircompressorAlgorithm("lz4", Lz4Codec::new),
            new AircompressorAlgorithm("snappy", SnappyCodec::new),
            new AircompressorAlgorithm("zstd", ZstdCodec::new));

    /**
     * Verifies that data compressed with Streaming API algorithms (lz4, snappy, zstd, lzo) returns
     * to its exact original state after decompression.
     */
    @FuzzTest
    public void fuzzAircompressorStreamRoundTrip(FuzzedDataProvider fdp)
            throws IOException
    {
        AircompressorAlgorithm algorithm = fdp.pickValue(AIRCOMPRESSOR_ALGORITHMS);
        CompressionCodec codec = algorithm.codecSupplier().get();

        byte[] originalData = fdp.consumeRemainingAsBytes();
        if (originalData.length == 0) {
            return;
        }

        // Write via Output Stream
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (CompressionOutputStream cos = codec.createOutputStream(out)) {
            cos.write(originalData);
        }
        byte[] compressedData = out.toByteArray();

        // Read via Input Stream
        ByteArrayInputStream in = new ByteArrayInputStream(compressedData);
        CompressionInputStream cis = codec.createInputStream(in);
        byte[] uncompressedData = cis.readAllBytes();
        cis.close();

        // Validate round trip
        if (!Arrays.equals(originalData, uncompressedData)) {
            throw new RuntimeException("Stream round trip mismatch for " + algorithm);
        }
    }

    /**
     * Feeds random data to Streaming API decompressors.
     */
    @FuzzTest
    public void fuzzAircompressorDecompress(FuzzedDataProvider fdp)
    {
        AircompressorAlgorithm algorithm = fdp.pickValue(AIRCOMPRESSOR_ALGORITHMS);
        byte[] inputData = fdp.consumeRemainingAsBytes();

        CompressionCodec codec = algorithm.codecSupplier().get();

        try (ByteArrayInputStream in = new ByteArrayInputStream(inputData);
                CompressionInputStream cis = codec.createInputStream(in)) {
            cis.readAllBytes();
        }
        catch (IOException | IllegalArgumentException | MalformedInputException | NegativeArraySizeException
                 | IndexOutOfBoundsException | IllegalStateException | ArithmeticException ignored) {
        }
    }

    /**
     * Checks for "Compression Bombs." It fails if small input expands
     * to disproportionately large output (defined by MAX_EXPANSION_RATIO).
     * This fuzz test has produced a finding.
     */
    @FuzzTest
    public void fuzzDecompressionBomb(FuzzedDataProvider fdp)
    {
        AircompressorAlgorithm algorithm = fdp.pickValue(AIRCOMPRESSOR_ALGORITHMS);
        byte[] inputData = fdp.consumeRemainingAsBytes();
        long maxAllowedSize = (long) inputData.length * MAX_EXPANSION_RATIO + (1024 * 1024);

        CompressionCodec codec = algorithm.codecSupplier().get();

        try (ByteArrayInputStream in = new ByteArrayInputStream(inputData);
                CompressionInputStream cis = codec.createInputStream(in)) {
            byte[] uncompressedData = cis.readAllBytes();
            cis.close();

            int uncompressedSize = uncompressedData.length;
            if (uncompressedSize > maxAllowedSize) {
                throw new RuntimeException("Possible Zip Bomb detected for " + algorithm.name() +
                        "! Input size: " + inputData.length +
                        ", Produced over: " + uncompressedSize + " bytes");
            }
        }
        catch (IOException | IllegalArgumentException | MalformedInputException | NegativeArraySizeException
                 | IndexOutOfBoundsException | IllegalStateException | ArithmeticException ignored) {
        }
    }

    // ============================================================================================
    // Consistency Checks (Block vs Stream Output Comparison)
    // ============================================================================================
    private record ConsistencyAlgorithm(
            String name,
            Supplier<Compressor> compressor,
            Supplier<CompressionCodec> codec
    ) {}

    private static final List<ConsistencyAlgorithm> CONSISTENCY_ALGORITHMS = List.of(
            new ConsistencyAlgorithm("lz4", Lz4JavaCompressor::new, Lz4Codec::new),
            new ConsistencyAlgorithm("snappy", SnappyJavaCompressor::new, SnappyCodec::new),
            new ConsistencyAlgorithm("zstd", ZstdJavaCompressor::new, ZstdCodec::new),
            new ConsistencyAlgorithm("lzo", LzoCompressor::new, LzoCodec::new));

    /**
     * Ensures that the Block API and Stream API produce compatible output data.
     * This fuzz test currently fails for some inputs.
     */
    @FuzzTest
    public void fuzzBlockVsStreamConsistency(FuzzedDataProvider fdp)
            throws IOException
    {
        ConsistencyAlgorithm algorithm = fdp.pickValue(CONSISTENCY_ALGORITHMS);
        byte[] originalData = fdp.consumeRemainingAsBytes();
        if (originalData.length == 0) {
            return;
        }

        // Generate Block Output
        Compressor compressor = algorithm.compressor().get();
        byte[] blockBuffer = new byte[compressor.maxCompressedLength(originalData.length)];
        int blockSize = compressor.compress(originalData, 0, originalData.length, blockBuffer, 0, blockBuffer.length);
        byte[] blockOutput = Arrays.copyOf(blockBuffer, blockSize);

        // Generate Stream Output
        CompressionCodec codec = algorithm.codec().get();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (CompressionOutputStream cos = codec.createOutputStream(baos)) {
            cos.write(originalData);
        }
        byte[] streamOutput = baos.toByteArray();

        // Compare results
        if (indexOf(streamOutput, blockOutput) == -1) {
            throw new RuntimeException("Consistency mismatch for " + algorithm.name() +
                    "\nBlock size: " + blockOutput.length +
                    "\nStream size: " + streamOutput.length);
        }
    }

    private int indexOf(byte[] outer, byte[] inner)
    {
        if (inner.length == 0) {
            return 0;
        }
        if (outer.length < inner.length) {
            return -1;
        }

        for (int i = 0; i <= outer.length - inner.length; i++) {
            boolean found = true;
            for (int j = 0; j < inner.length; j++) {
                if (outer[i + j] != inner[j]) {
                    found = false;
                    break;
                }
            }
            if (found) {
                return i;
            }
        }
        return -1;
    }

    // ============================================================================================
    // Hadoop Codecs
    // ============================================================================================
    interface InputStreamWrapper
    {
        InputStream wrap(InputStream in)
                throws IOException;
    }

    interface OutputStreamWrapper
    {
        OutputStream wrap(OutputStream out)
                throws IOException;
    }

    private record HadoopAlgorithm(
            String name,
            InputStreamWrapper decompressor,
            OutputStreamWrapper compressor
    ) {}

    private static final List<HadoopAlgorithm> HADOOP_ALGORITHMS = List.of(
            new HadoopAlgorithm("bzip2",
                    in -> createHadoopStream(BZip2Codec::new, codec -> codec.createInputStream(in)),
                    out -> createHadoopStream(BZip2Codec::new, codec -> codec.createOutputStream(out))),
            new HadoopAlgorithm("gzip",
                    in -> createHadoopStream(JdkGzipCodec::new, codec -> codec.createInputStream(in)),
                    out -> createHadoopStream(JdkGzipCodec::new, codec -> codec.createOutputStream(out))),
            new HadoopAlgorithm("deflate",
                    in -> createHadoopStream(JdkDeflateCodec::new, codec -> codec.createInputStream(in)),
                    out -> createHadoopStream(JdkDeflateCodec::new, codec -> codec.createOutputStream(out))));

    private static <T extends Configurable, R> R createHadoopStream(Supplier<T> codecSupplier, StreamFunction<T, R> streamCreator)
            throws IOException
    {
        T codec = codecSupplier.get();
        codec.setConf(new Configuration());
        return streamCreator.apply(codec);
    }

    interface StreamFunction<T, R>
    {
        R apply(T t)
                throws IOException;
    }

    /**
     * Verifies that data compressed with Hadoop Codecs (bzip2, gzip, deflate) returns
     * to its exact original state after decompression.
     */
    @FuzzTest
    public void fuzzHadoopRoundTrip(FuzzedDataProvider fdp)
    {
        HadoopAlgorithm algorithm = fdp.pickValue(HADOOP_ALGORITHMS);

        int bufferSize = 1 << fdp.consumeInt(0, 16);
        int readStrategy = fdp.consumeInt(0, 2);

        byte[] originalData;
        if (algorithm.name.equals("bzip2")) {
            originalData = fdp.consumeBytes(fdp.consumeInt(0, 128 * 1024));
        }
        else {
            originalData = fdp.consumeRemainingAsBytes();
        }

        // Compress
        ByteArrayOutputStream compressedBaos = new ByteArrayOutputStream();
        try (OutputStream compressorStream = algorithm.compressor.wrap(compressedBaos)) {
            compressorStream.write(originalData);
        }
        catch (IOException ignored) {
            return;
        }

        byte[] compressed = compressedBaos.toByteArray();
        if (compressed.length == 0) {
            return;
        }

        // Decompress
        ByteArrayOutputStream decompressedBaos = new ByteArrayOutputStream();
        try (InputStream decompressorStream = algorithm.decompressor.wrap(new ByteArrayInputStream(compressed))) {
            byte[] buf = new byte[bufferSize];

            if (readStrategy == 0) {
                // Read byte by byte
                int b;
                while ((b = decompressorStream.read()) != -1) {
                    decompressedBaos.write(b);
                }
            }
            else if (readStrategy == 1) {
                // Read full buffers
                int len;
                while ((len = decompressorStream.read(buf)) != -1) {
                    decompressedBaos.write(buf, 0, len);
                }
            }
            else {
                // Read partial buffers
                int pos = 0;
                while (true) {
                    int maxLen = bufferSize - pos;
                    if (maxLen <= 0) {
                        decompressedBaos.write(buf, 0, bufferSize);
                        pos = 0;
                        maxLen = bufferSize;
                    }
                    int read = decompressorStream.read(buf, pos, maxLen);
                    if (read == -1) {
                        if (pos > 0) {
                            decompressedBaos.write(buf, 0, pos);
                        }
                        break;
                    }
                    pos += read;
                }
            }
        }
        catch (IOException e) {
            throw new RuntimeException("Decompression failed on data we just compressed (" + algorithm.name + ")", e);
        }
        byte[] decompressed = decompressedBaos.toByteArray();

        // Validate round trip
        if (!Arrays.equals(originalData, decompressed)) {
            throw new RuntimeException("Hadoop round trip mismatch for " + algorithm.name + " (input size = " + originalData.length + ")");
        }
    }

    /**
     * Feeds random data to Streaming API decompressors.
     * This fuzz test has produced a finding.
     */
    @FuzzTest
    public void fuzzHadoopDecompress(FuzzedDataProvider fdp)
    {
        HadoopAlgorithm algorithm = fdp.pickValue(HADOOP_ALGORITHMS);

        byte[] fuzzData = fdp.consumeRemainingAsBytes();

        ByteArrayOutputStream decompressedBaos = new ByteArrayOutputStream();
        try (InputStream in = algorithm.decompressor.wrap(new ByteArrayInputStream(fuzzData))) {
            byte[] buffer = new byte[8192];
            int len;
            while ((len = in.read(buffer)) != -1) {
                decompressedBaos.write(buffer, 0, len);
            }
        }
        catch (IOException ignored) {
        }
    }

    // ============================================================================================
    // Compare Aircompressor with Apache Commons
    // ============================================================================================
    /**
     * Verifies that aircompressor is interoperable with the
     * standard org.apache.commons.compress implementations.
     */
    @FuzzTest
    public void fuzzAircompressorVsApacheCommons(FuzzedDataProvider fdp)
    {
        AircompressorAlgorithm algo = fdp.pickValue(AIRCOMPRESSOR_ALGORITHMS);
        CompressionCodec air = algo.codecSupplier().get();

        byte[] input = fdp.consumeRemainingAsBytes();
        if (input.length == 0) {
            return;
        }

        String name = algo.name();
        boolean supported = switch (name) {
            case "lz4", "snappy", "zstd" -> true;
            default -> false;
        };
        if (!supported) {
            return;
        }

        // Compression via Aircompressor ----------------------------------------
        byte[] airCompressed = new byte[0];
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                CompressionOutputStream out = air.createOutputStream(baos)) {
            out.write(input);
            airCompressed = baos.toByteArray();
        }
        catch (IOException ignored) {
        }

        // Compression via Apache Compress --------------------------------------
        byte[] apacheCompressed = new byte[0];
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                CompressorOutputStream out = switch (name) {
                case "lz4" -> new BlockLZ4CompressorOutputStream(baos);
                case "snappy" -> new SnappyCompressorOutputStream(baos, input.length);
                case "zstd" -> new ZstdCompressorOutputStream(baos);
                default -> throw new AssertionError();
            }) {
            out.write(input);
            out.close();
            apacheCompressed = baos.toByteArray();
        }
        catch (IOException ignored) {
        }

        // Cross-decompress: Apache decompresses Aircompressor
        byte[] crossApache = new byte[0];
        try (CompressorInputStream in = switch (name) {
                case "lz4" -> new BlockLZ4CompressorInputStream(new ByteArrayInputStream(airCompressed));
                case "snappy" -> new SnappyCompressorInputStream(new ByteArrayInputStream(airCompressed));
                case "zstd" -> new ZstdCompressorInputStream(new ByteArrayInputStream(airCompressed));
                default -> throw new AssertionError();
            }) {
            crossApache = in.readAllBytes();
        }
        catch (IOException ignored) {
        }

        // Cross-decompress: Aircompressor decompresses Apache
        byte[] crossAir = new byte[0];
        try (CompressionInputStream in = air.createInputStream(new ByteArrayInputStream(apacheCompressed))) {
            crossAir = in.readAllBytes();
        }
        catch (IOException | MalformedInputException | IllegalArgumentException | NegativeArraySizeException ignored) {
        }

        // Compare decompressed results
        if (!Arrays.equals(crossApache, crossAir)) {
            throw new AssertionError("Cross-decompression mismatch for " + name);
        }
    }
}
