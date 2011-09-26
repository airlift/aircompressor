# Snappy in Java

This is a rewrite (port) of [Snappy](http://code.google.com/p/snappy/) writen in
pure Java. This compression code produces a byte-for-byte exact copy of the output
created by the original C++ code, and extremely fast.

# Performance

A port of the Snappy micro-benchmark is included, and can be used to measure
the performance of this code against the excellent Snappy JNI wrapper from
[xerial](http://code.google.com/p/snappy-java/).  As you can see below, the
pure Java port is 20-30% faster for compression, and is typically 0-10% slower
for decompression. These results were run with Java 7 on a Core i7, 64-bit Mac.

As a second more independent test, the performance has been measured using the
Ning JVM compression benchmark against Snappy JNI, and the pure Java
[Ning LZF](https://github.com/ning/compress) codec. The
[results](http://dain.github.com/snappy/) show that the pure Java Snappy is
20-30% faster than JNI Snappy for compression, and is typically 10-20% slower
for decompression. Both, the pure Java Snappy and JNI Snappy implementations
are faster that the Ning LZF codec.  These results were run with Java 6 on a
Core i7, 64-bit Mac.

The difference in performance between these two tests is due to the difference
in JVM version;  Java 7 is consistently 5-10% faster than Java 6 in the
compression code. As with all benchmarks your mileage will vary, so test with
your actual use case.

<pre><code>
Benchmark          Size   Compress        JNI       Java   Change
-----------------------------------------------------------------
Compress/0       102400     +76.4%  288.3MB/s  374.9MB/s   +30.1%  html
Compress/1       702087     +49.1%  177.3MB/s  224.6MB/s   +26.7%  urls
Compress/2       126958      +0.1%    2.7GB/s    2.9GB/s    +6.6%  jpg (not compressible)
Compress/3        94330     +17.9%  636.1MB/s  854.2MB/s   +34.3%  pdf
Compress/4       409600     +76.4%  286.6MB/s  370.6MB/s   +29.3%  html4
Compress/5        24603     +51.9%  162.0MB/s  218.2MB/s   +34.7%  cp
Compress/6        11150     +57.6%  176.6MB/s  258.8MB/s   +46.5%  c
Compress/7         3721     +51.6%  239.5MB/s  270.5MB/s   +12.9%  lsp
Compress/8      1029744     +58.7%  260.4MB/s  289.9MB/s   +11.3%  xls
Compress/9       152089     +40.2%  116.3MB/s  161.9MB/s   +39.2%  txt1
Compress/10      125179     +35.9%  111.2MB/s  151.5MB/s   +36.2%  txt2
Compress/11      426754     +42.9%  119.7MB/s  167.8MB/s   +40.1%  txt3
Compress/12      481861     +31.7%  106.7MB/s  143.9MB/s   +34.8%  txt4
Compress/13      513216     +81.8%  403.0MB/s  489.6MB/s   +21.5%  bin
Compress/14       38240     +48.1%  161.3MB/s  205.3MB/s   +27.3%  sum
Compress/15        4227     +40.6%  194.1MB/s  223.8MB/s   +15.3%  man
Compress/16      118588     +76.8%  360.6MB/s  441.3MB/s   +22.4%  pb
Compress/17      184320     +61.7%  165.7MB/s  255.2MB/s   +54.0%  gaviota

Benchmark          Size   Compress        JNI       Java   Change
-----------------------------------------------------------------
Compress/0       102400     +76.4%    1.5GB/s    1.3GB/s   -11.6%  html
Compress/1       702087     +49.1%  956.7MB/s  812.2MB/s   -15.1%  urls
Compress/2       126958      +0.1%   18.8GB/s   19.4GB/s    +3.4%  jpg (not compressible)
Compress/3        94330     +17.9%    4.2GB/s    3.7GB/s   -11.8%  pdf
Compress/4       409600     +76.4%    1.4GB/s    1.2GB/s   -15.9%  html4
Compress/5        24603     +51.9%  968.9MB/s  920.8MB/s    -5.0%  cp
Compress/6        11150     +57.6%  981.7MB/s  916.0MB/s    -6.7%  c
Compress/7         3721     +51.6%  983.8MB/s  953.6MB/s    -3.1%  lsp
Compress/8      1029744     +58.7%  797.2MB/s  746.0MB/s    -6.4%  xls
Compress/9       152089     +40.2%  636.7MB/s  579.6MB/s    -9.0%  txt1
Compress/10      125179     +35.9%  602.1MB/s  544.8MB/s    -9.5%  txt2
Compress/11      426754     +42.9%  673.9MB/s  612.9MB/s    -9.1%  txt3
Compress/12      481861     +31.7%  555.5MB/s  501.7MB/s    -9.7%  txt4
Compress/13      513216     +81.8%    1.5GB/s    1.2GB/s   -18.5%  bin
Compress/14       38240     +48.1%  840.5MB/s  784.3MB/s    -6.7%  sum
Compress/15        4227     +40.6%  860.7MB/s  856.0MB/s    -0.6%  man
Compress/16      118588     +76.8%    1.7GB/s    1.5GB/s   -13.3%  pb
Compress/17      184320     +61.7%  766.3MB/s  683.0MB/s   -10.9%  gaviota
</code></pre>
