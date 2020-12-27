# Compression in pure Java
[![Maven Central](https://img.shields.io/maven-central/v/io.airlift/aircompressor.svg?label=Maven%20Central)](https://search.maven.org/#search%7Cga%7C1%7Cg%3A%22io.airlift%22%20AND%20a%3A%22aircompressor%22)
[![Build Status](https://travis-ci.org/airlift/aircompressor.svg?branch=master)](https://travis-ci.org/airlift/aircompressor)

This library contains implementations of
[LZ4](https://www.lz4.org/),
[Zstandard](https://www.zstd.net/) (Zstd),
[Snappy](https://google.github.io/snappy/), and
[LZO](https://www.oberhumer.com/opensource/lzo/) written in pure Java. They are 
typically 10-40% faster than the JNI wrapper for the native libraries.

# Hadoop CompressionCodec

In addition to the raw block encoders, there are implementations of the
Hadoop CompressionCodec for each algorithm. They are
typically 300% faster than the JNI wrappers.

# Requirements

This library requires a Java 1.8+ virtual machine containing the `sun.misc.Unsafe` interface running on a little endian platform.

# Users

This library is used in projects such as Trino (https://trino.io), a distributed SQL engine.
