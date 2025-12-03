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
import com.code_intelligence.jazzer.mutation.annotation.InRange;
import com.code_intelligence.jazzer.mutation.annotation.NotNull;
import com.code_intelligence.jazzer.mutation.annotation.WithLength;
import io.airlift.compress.v3.bzip2.BZip2Codec;
import io.airlift.compress.v3.deflate.DeflateCompressor;
import io.airlift.compress.v3.deflate.DeflateDecompressor;
import io.airlift.compress.v3.deflate.JdkDeflateCodec;
import io.airlift.compress.v3.gzip.JdkGzipCodec;
import io.airlift.compress.v3.lz4.*;
import io.airlift.compress.v3.lzo.LzoCodec;
import io.airlift.compress.v3.lzo.LzoCompressor;
import io.airlift.compress.v3.lzo.LzoDecompressor;
import io.airlift.compress.v3.lzo.LzopCodec;
import io.airlift.compress.v3.snappy.*;
import io.airlift.compress.v3.zstd.*;
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
import org.junit.jupiter.api.Assertions;

import java.io.*;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

public class FuzzTests {
  private static final int MAX_EXPANSION_RATIO = 80;

  // ============================================================================================
  // Block Compression
  // ============================================================================================
  private record BlockAlgorithm(
    String name,
    Supplier<Compressor> compressor,
    Supplier<Decompressor> decompressor,
    BooleanSupplier available
  ) {
    private BlockAlgorithm(String name, Supplier<Compressor> compressor, Supplier<Decompressor> decompressor) {
      this(name, compressor, decompressor, () -> true);
    }
  }

  private static final int BLOCK_ALGORITHMS_NUMBER = 8;
  private static final BlockAlgorithm[] BLOCK_ALGORITHMS = {
    new BlockAlgorithm("lz4-java", Lz4JavaCompressor::new, Lz4JavaDecompressor::new),
    new BlockAlgorithm("lz4-native", Lz4NativeCompressor::new, Lz4NativeDecompressor::new, Lz4NativeCompressor::isEnabled),
    new BlockAlgorithm("snappy-java", SnappyJavaCompressor::new, SnappyJavaDecompressor::new),
    new BlockAlgorithm("snappy-native", SnappyNativeCompressor::new, SnappyNativeDecompressor::new, SnappyNativeCompressor::isEnabled),
    new BlockAlgorithm("zstd-java", ZstdJavaCompressor::new, ZstdJavaDecompressor::new),
    new BlockAlgorithm("zstd-native", ZstdNativeCompressor::new, ZstdNativeDecompressor::new, ZstdNativeCompressor::isEnabled),
    new BlockAlgorithm("lzo", LzoCompressor::new, LzoDecompressor::new),
    new BlockAlgorithm("deflate", DeflateCompressor::new, DeflateDecompressor::new)};

  static {
    assert BLOCK_ALGORITHMS.length == BLOCK_ALGORITHMS_NUMBER;
  }

  /**
   * Verifies that data compressed with Block algorithms (lz4-java/native, snappy-java/native, zstd-java/native, lzo, deflate)
   * returns to its exact original state after decompression.
   */
  @FuzzTest
  public void fuzzBlockRoundTrip(@InRange(min = 0, max = BLOCK_ALGORITHMS_NUMBER - 1) int algId, byte @NotNull [] input) {

    BlockAlgorithm algorithm = BLOCK_ALGORITHMS[algId];
    if (!algorithm.available().getAsBoolean()) {
      return;
    }
    Compressor compressor = algorithm.compressor().get();
    Decompressor decompressor = algorithm.decompressor().get();

    // Compress
    byte[] compressed = new byte[compressor.maxCompressedLength(input.length)];
    int compressedSize = compressor.compress(input, 0, input.length, compressed, 0, compressed.length);

    // Decompress
    byte[] uncompressed = new byte[input.length];
    decompressor.decompress(compressed, 0, compressedSize, uncompressed, 0, uncompressed.length);

    // Validate round trip
    if (!Arrays.equals(input, uncompressed)) {
      throw new RuntimeException("Block round trip mismatch for " + algorithm);
    }
  }

  /**
   * Feeds random data to Block decompressors.
   */
  @FuzzTest
  public void fuzzBlockDecompressMismatch(@InRange(min = 0, max = BLOCK_ALGORITHMS_NUMBER - 1) int algId, byte @NotNull @WithLength(min = 1, max = 10) [] input) {
    BlockAlgorithm algorithm = BLOCK_ALGORITHMS[algId];
    if (!algorithm.available().getAsBoolean() && !algorithm.name.equals("lz4-java")) {
      return;
    }
    Decompressor decompressor = algorithm.decompressor().get();

    try {
      byte[] outputBuffer1 = new byte[input.length + 1024 * 80];
      Arrays.fill(outputBuffer1, (byte) 1);
      int numDecompressed1 = decompressor.decompress(input, 0, input.length, outputBuffer1, 0, outputBuffer1.length);
      byte[] decompressed1 = Arrays.copyOfRange(outputBuffer1, 0, numDecompressed1);


      byte[] outputBuffer2 = new byte[input.length + 1024 * 80];
      Arrays.fill(outputBuffer2, (byte) 2);
      int numDecompressed2 = decompressor.decompress(input, 0, input.length, outputBuffer2, 0, outputBuffer2.length);
      byte[] decompressed2 = Arrays.copyOfRange(outputBuffer2, 0, numDecompressed2);

      Assertions.assertEquals(numDecompressed1, numDecompressed2);

      Assertions.assertArrayEquals(decompressed1, decompressed2,
        "[" + algorithm.name + "]: " + "decompressed output in range [0..numDecompressedBytes) depends on output buffer content: \n    " +
          Arrays.toString(decompressed1) + "\n     !=\n    " + Arrays.toString(decompressed2) + "\n");
    } catch (IllegalArgumentException | IllegalStateException | MalformedInputException
             | IndexOutOfBoundsException
      ignored) {
    } catch (RuntimeException e) {
      if (e.getMessage() == null || !e.getMessage().equals("Invalid compressed stream")) {
        throw e;
      }
    }
  }

  // ============================================================================================
  // Aircompressor Streams
  // ============================================================================================
  private record AircompressorAlgorithm(
    String name,
    Supplier<CompressionCodec> codecSupplier
  ) {
  }

  private static final AircompressorAlgorithm[] AIRCOMPRESSOR_ALGORITHMS = {
    new AircompressorAlgorithm("bzip2", BZip2Codec::new),
    new AircompressorAlgorithm("deflate", JdkDeflateCodec::new),
    new AircompressorAlgorithm("gzip", JdkGzipCodec::new),
    new AircompressorAlgorithm("lz4", Lz4Codec::new),
    new AircompressorAlgorithm("lzo", LzoCodec::new),
    new AircompressorAlgorithm("lzop", LzopCodec::new),
    new AircompressorAlgorithm("snappy", SnappyCodec::new),
    new AircompressorAlgorithm("zstd", ZstdCodec::new)
  };

  private static final int AIRCOMPRESSOR_ALGORITHMS_NUMBER = 8;

  static {
    assert AIRCOMPRESSOR_ALGORITHMS.length == AIRCOMPRESSOR_ALGORITHMS_NUMBER;
  }

  /**
   * Verifies that data compressed with Streaming API algorithms (bzip2, deflate, gzip, lz4, lzo, lzop, snappy, zstd) returns
   * to its exact original state after decompression.
   */
  @FuzzTest
  public void fuzzAircompressorStreamRoundTrip(@InRange(min = 0, max = AIRCOMPRESSOR_ALGORITHMS_NUMBER - 1) int algId, byte @NotNull [] input)
    throws IOException {
    AircompressorAlgorithm algorithm = AIRCOMPRESSOR_ALGORITHMS[algId];
    CompressionCodec codec = algorithm.codecSupplier().get();

    // Write via Output Stream
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (CompressionOutputStream cos = codec.createOutputStream(out)) {
      cos.write(input);
    }
    byte[] compressedData = out.toByteArray();

    // Read via Input Stream
    ByteArrayInputStream in = new ByteArrayInputStream(compressedData);
    CompressionInputStream cis = codec.createInputStream(in);
    byte[] uncompressedData = cis.readAllBytes();
    cis.close();

    // Validate round trip
    if (!Arrays.equals(input, uncompressedData)) {
      throw new RuntimeException("Stream round trip mismatch for " + algorithm);
    }
  }

  /**
   * Feeds random data to Streaming API decompressors.
   */
  @FuzzTest
  public void fuzzAircompressorDecompress(@InRange(min = 0, max = AIRCOMPRESSOR_ALGORITHMS_NUMBER - 1) int algId, byte @NotNull [] input) {
    AircompressorAlgorithm algorithm = AIRCOMPRESSOR_ALGORITHMS[algId];

    CompressionCodec codec = algorithm.codecSupplier().get();

    try (ByteArrayInputStream in = new ByteArrayInputStream(input);
         CompressionInputStream cis = codec.createInputStream(in)) {
      cis.readAllBytes();
    } catch (IOException | IllegalArgumentException | MalformedInputException
//               | NegativeArraySizeException
//               | IndexOutOfBoundsException
             | IllegalStateException
//               | ArithmeticException
      ignored) {
    }
  }

  /**
   * Checks for "Compression Bombs." It fails if small input expands
   * to disproportionately large output (defined by MAX_EXPANSION_RATIO).
   * This fuzz test has produced a finding.
   */
  @FuzzTest
  public void fuzzDecompressionBomb(FuzzedDataProvider fdp) {
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
    } catch (IOException | IllegalArgumentException | MalformedInputException | NegativeArraySizeException
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
  ) {
  }

  private static final ConsistencyAlgorithm[] CONSISTENCY_ALGORITHMS = {
    new ConsistencyAlgorithm("lz4", Lz4JavaCompressor::new, Lz4Codec::new),
    new ConsistencyAlgorithm("snappy", SnappyJavaCompressor::new, SnappyCodec::new),
    new ConsistencyAlgorithm("zstd", ZstdJavaCompressor::new, ZstdCodec::new),
    new ConsistencyAlgorithm("lzo", LzoCompressor::new, LzoCodec::new)
  };

  private static final int CONSISTENCY_ALGORITHMS_NUMBER = 4;

  // ============================================================================================
  // Hadoop Codecs
  // ============================================================================================
  interface InputStreamWrapper {
    InputStream wrap(InputStream in)
      throws IOException;
  }

  interface OutputStreamWrapper {
    OutputStream wrap(OutputStream out)
      throws IOException;
  }

  private record HadoopAlgorithm(
    String name,
    InputStreamWrapper decompressor,
    OutputStreamWrapper compressor
  ) {
  }

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
    throws IOException {
    T codec = codecSupplier.get();
    codec.setConf(new Configuration());
    return streamCreator.apply(codec);
  }

  interface StreamFunction<T, R> {
    R apply(T t)
      throws IOException;
  }

  /**
   * Verifies that data compressed with Hadoop Codecs (bzip2, gzip, deflate) returns
   * to its exact original state after decompression.
   */
  @FuzzTest
  public void fuzzHadoopRoundTrip(FuzzedDataProvider fdp) {
    HadoopAlgorithm algorithm = fdp.pickValue(HADOOP_ALGORITHMS);

    int bufferSize = 1 << fdp.consumeInt(0, 16);
    int readStrategy = fdp.consumeInt(0, 2);

    byte[] originalData;
    if (algorithm.name.equals("bzip2")) {
      originalData = fdp.consumeBytes(fdp.consumeInt(0, 128 * 1024));
    } else {
      originalData = fdp.consumeRemainingAsBytes();
    }

    // Compress
    ByteArrayOutputStream compressedBaos = new ByteArrayOutputStream();
    try (OutputStream compressorStream = algorithm.compressor.wrap(compressedBaos)) {
      compressorStream.write(originalData);
    } catch (IOException ignored) {
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
      } else if (readStrategy == 1) {
        // Read full buffers
        int len;
        while ((len = decompressorStream.read(buf)) != -1) {
          decompressedBaos.write(buf, 0, len);
        }
      } else {
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
    } catch (IOException e) {
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
  public void fuzzHadoopDecompress(FuzzedDataProvider fdp) {
    HadoopAlgorithm algorithm = fdp.pickValue(HADOOP_ALGORITHMS);

    byte[] fuzzData = fdp.consumeRemainingAsBytes();

    ByteArrayOutputStream decompressedBaos = new ByteArrayOutputStream();
    try (InputStream in = algorithm.decompressor.wrap(new ByteArrayInputStream(fuzzData))) {
      byte[] buffer = new byte[8192];
      int len;
      while ((len = in.read(buffer)) != -1) {
        decompressedBaos.write(buffer, 0, len);
      }
    } catch (IOException ignored) {
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
  public void fuzzAircompressorVsApacheCommons(FuzzedDataProvider fdp) {
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
    } catch (IOException ignored) {
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
    } catch (IOException ignored) {
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
    } catch (IOException ignored) {
    }

    // Cross-decompress: Aircompressor decompresses Apache
    byte[] crossAir = new byte[0];
    try (CompressionInputStream in = air.createInputStream(new ByteArrayInputStream(apacheCompressed))) {
      crossAir = in.readAllBytes();
    } catch (IOException | MalformedInputException | IllegalArgumentException | NegativeArraySizeException ignored) {
    }

    // Compare decompressed results
    if (!Arrays.equals(crossApache, crossAir)) {
      throw new AssertionError("Cross-decompression mismatch for " + name);
    }
  }
}
