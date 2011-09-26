# Snappy in Java

This is a rewrite (port) of [Snappy](http://code.google.com/p/snappy/) writen in
pure Java. This compression code produces a byte-for-byte exact copy of the output
created by the original C++ code, and extremely fast.

# Performance

This code has been tested using the Ning JVM compression benchmark against the
excellent Snappy JNI wrapper from [xerial](http://code.google.com/p/snappy-java/),
and the [Ning LZF](https://github.com/ning/compress) codec.  The
[**results** show](http://dain.github.com/snappy/) that the pure Java Snappy is 10-50%
faster than JNI Snappy for compression and the same speed for decompression. Both,
the pure Java Snappy and JNI Snappy implementations are faster that the Ning LZF
codec.

A port of the Snappy micro-benchmark.  As you can see below, the pure Java port
is 20-30% faster for compression, and is typically 10-20% slower for decompression.
These results were run with Java 7 on a Core i7, 64-bit Mac.

It is currently not know why the Ning JVM compression benchmark and the Snappy
micro-benchmark report such different results for decompression, and as with all
benchmarks your mileage will vary.

<pre><code>
Benchmark          Size   Compress        JNI       Java   Change
-----------------------------------------------------------------
Compress/0       102400     +76.4%  292.0MB/s  376.6MB/s   +22.5%  html
Compress/1       702087     +49.1%  181.5MB/s  224.3MB/s   +19.1%  urls
Compress/2       126958      +0.1%    2.7GB/s    2.9GB/s    +6.4%  jpg
Compress/3        94330     +17.9%  639.5MB/s  849.0MB/s   +24.7%  pdf
Compress/4       409600     +76.4%  288.0MB/s  373.5MB/s   +22.9%  html4
Compress/5        24603     +51.9%  164.6MB/s  219.0MB/s   +24.9%  cp
Compress/6        11150     +57.6%  177.5MB/s  259.6MB/s   +31.6%  c
Compress/7         3721     +51.6%  249.1MB/s  273.1MB/s    +8.8%  lsp
Compress/8      1029744     +58.7%  260.0MB/s  290.3MB/s   +10.4%  xls
Compress/9       152089     +40.2%  116.5MB/s  160.3MB/s   +27.3%  txt1
Compress/10      125179     +35.9%  113.0MB/s  151.3MB/s   +25.3%  txt2
Compress/11      426754     +42.9%  122.8MB/s  167.7MB/s   +26.8%  txt3
Compress/12      481861     +31.7%  107.8MB/s  143.8MB/s   +25.0%  txt4
Compress/13      513216     +81.8%  406.6MB/s  491.2MB/s   +17.2%  bin
Compress/14       38240     +48.1%  162.9MB/s  205.3MB/s   +20.6%  sum
Compress/15        4227     +40.6%  193.5MB/s  223.6MB/s   +13.5%  man
Compress/16      118588     +76.8%  362.8MB/s  436.0MB/s   +16.8%  pb
Compress/17      184320     +61.7%  166.1MB/s  250.6MB/s   +33.7%  gaviota

Benchmark          Size   Compress        JNI       Java   Change
-----------------------------------------------------------------
Compress/0       102400     +76.4%    1.5GB/s    1.3GB/s   -14.3%  html
Compress/1       702087     +49.1%  964.4MB/s  820.1MB/s   -17.6%  urls
Compress/2       126958      +0.1%   18.9GB/s   18.8GB/s    -0.0%  jpg
Compress/3        94330     +17.9%    4.1GB/s    3.7GB/s    -8.3%  pdf
Compress/4       409600     +76.4%    1.4GB/s    1.2GB/s   -20.2%  html4
Compress/5        24603     +51.9%  968.8MB/s  951.3MB/s    -1.8%  cp
Compress/6        11150     +57.6%  981.0MB/s  929.2MB/s    -5.6%  c
Compress/7         3721     +51.6%  986.0MB/s  966.9MB/s    -2.0%  lsp
Compress/8      1029744     +58.7%  797.7MB/s  743.8MB/s    -7.2%  xls
Compress/9       152089     +40.2%  641.5MB/s  577.0MB/s   -11.2%  txt1
Compress/10      125179     +35.9%  609.9MB/s  543.0MB/s   -12.3%  txt2
Compress/11      426754     +42.9%  677.5MB/s  607.3MB/s   -11.6%  txt3
Compress/12      481861     +31.7%  558.5MB/s  501.1MB/s   -11.5%  txt4
Compress/13      513216     +81.8%    1.5GB/s    1.2GB/s   -25.8%  bin
Compress/14       38240     +48.1%  838.0MB/s  764.6MB/s    -9.6%  sum
Compress/15        4227     +40.6%  854.9MB/s  846.6MB/s    -1.0%  man
Compress/16      118588     +76.8%    1.7GB/s    1.5GB/s   -15.9%  pb
Compress/17      184320     +61.7%  764.4MB/s  689.2MB/s   -10.9%  gaviota
</code></pre>
