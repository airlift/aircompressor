# Compression in pure Java

This library contains implementations of [LZ4](https://github.com/Cyan4973/lz4),
[Snappy](http://code.google.com/p/snappy/), and
[LZO](http://www.oberhumer.com/opensource/lzo/) written in pure Java. They are 
typically 10-40% faster than the JNI wrapper for the native libraries.

# Hadoop CompressionCodec

In addition to the raw block encoders, there are implementations of the
Hadoop CompressionCodec for each algorithm. They are
typically 300% faster than the JNI wrappers.
