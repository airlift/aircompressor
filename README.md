# Compression for Java
[![Maven Central](https://img.shields.io/maven-central/v/io.airlift/aircompressor.svg?label=Maven%20Central)](https://search.maven.org/#search%7Cga%7C1%7Cg%3A%22io.airlift%22%20AND%20a%3A%22aircompressor%22)

This library provides a set of compression algorithms implemented in pure Java and 
where possible native implementations. The Java implementations use `sun.misc.Unsafe`
to provide fast access to memory. The native implementations use `java.lang.foreign`
to interact directly with native libraries without the need for JNI.

# Usage

Each algorithm provides a simple block compression API using the `io.airlift.compress.Compressor` 
and `io.airlift.compress.Decompressor` classes. Block compression is the simplest form of
which simply compresses a small block of data provided as a `byte[]`, or more generally a
`java.lang.foreign.MemorySegment`. Each algorithm may have one or more streaming format
which typically produces a sequence of block compressed chunks.

## byte array API
```java
byte[] data = ...

Compressor compressor = new Lz4JavaCompressor();
byte[] compressed = new byte[compressor.maxCompressedLength(data.length)];
int compressedSize = compressor.compress(data, 0, data.length, compressed, 0, compressed.length);

Decompressor decompressor = new Lz4JavaDecompressor();
byte[] uncompressed = new byte[data.length];
int uncompressedSize = decompressor.decompress(compressed, 0, compressedSize, uncompressed, 0, uncompressed.length);
```

## MemorySegment API
```java
Arena arena = ...
MemorySegment data = ...

Compressor compressor = new Lz4JavaCompressor();
MemorySegment compressed = arena.allocate(compressor.maxCompressedLength(toIntExact(data.byteSize())));
int compressedSize = compressor.compress(data, compressed);
compressed = compressed.asSlice(0, compressedSize);

Decompressor decompressor = new Lz4JavaDecompressor();
MemorySegment uncompressed = arena.allocate(data.byteSize());
int uncompressedSize = decompressor.decompress(compressed, uncompressed);
uncompressed = uncompressed.asSlice(0, uncompressedSize);
```

# Algorithms

## [Zstandard (Zstd)](https://www.zstd.net/) **(Recommended)**
Zstandard is the recommended algorithm for most compression. It provides
superior compression and performance at all levels compared to zlib. Zstandard is
an excellent choice for most use cases, especially storage and bandwidth constrained
network transfer.

The native implementation of Zstandard is provided by the `ZstdNativeCompressor` and
`ZstdNativeDecompressor` classes. The Java implementation is provided by the
`ZstdJavaCompressor` and `ZstdJavaDecompressor` classes.

The Zstandard streaming format is supported by `ZstdInputStream` and `ZstdOutputStream`.

## [LZ4](https://www.lz4.org/)
LZ4 is an extremely fast compression algorithm that provides compression ratios comparable
to Snappy and LZO. LZ4 is an excellent choice for applications that require high-performance
compression and decompression.

The native implementation of LZ4 is provided by `Lz4NativeCompressor` and `Lz4NativeDecompressor`.
The Java implementation is provided by `Lz4JavaCompressor` and `Lz4JavaDecompressor`.

## [Snappy](https://google.github.io/snappy/)
Snappy is not as fast as LZ4, but provides a guarantee on memory usage that makes it a good
choice for extremely resource-limited environments (e.g. embedded systems like a network 
switch). If your application is not highly resource constrained, LZ4 is a better choice.

The native implementation of Snappy is provided by `SnappyNativeCompressor` and `SnappyNativeDecompressor`.
The Java implementation is provided by `SnappyJavaCompressor` and `SnappyJavaDecompressor`.

The Snappy framed format is supported by `SnappyFramedInputStream` and `SnappyFramedOutputStream`.

## [LZO](https://www.oberhumer.com/opensource/lzo/) 
LZO is only provided for compatibility with existing systems that use LZO. We recommend 
rewriting LZO data using Zstandard or LZ4. 

The Java implementation of LZO is provided by `LzoJavaCompressor` and `LzoJavaDecompressor`.
Due to licensing issues, the LZO only has a Java implementation which is based on LZ4.

## Deflate
Deflate is the block compression algorithm used by the `gzip` and `zlib` libraries. Deflate is
provided for compatibility with existing systems that use Deflate. We recommend rewriting
Deflate data using Zstandard which provides superior compression and performance.

The implementation of Deflate is provided by `DeflateCompressor` and `DeflateDecompressor`.
This is implemented in the built-in Java libraries which internally use the native code.

# Hadoop Compression

In addition to the raw block encoders, there are implementations of the
Hadoop streams for the above algorithms. In addition, implementations of
gzip and bzip2 are provided so that all standard Hadoop algorithms are available.

The `HadoopStreams` class provides a factory for creating `InputStream` and `OutputStream`
implementations without the need for any Hadoop dependencies.  For environments 
that have Hadoop dependencies, each algorithm also provides a `CompressionCodec` class.

# Requirements

This library requires a Java 22+ virtual machine containing the `sun.misc.Unsafe` interface running on a little endian platform.

# Users

This library is used in projects such as Trino (https://trino.io), a distributed SQL engine.
