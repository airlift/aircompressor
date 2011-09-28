# Snappy in Java

This is a rewrite (port) of [Snappy](http://code.google.com/p/snappy/) writen in
pure Java. This compression code produces a byte-for-byte exact copy of the output
created by the original C++ code, and extremely fast.

# Performance

The Snappy micro-benchmark has been ported, and can be used to measure
the performance of this code against the excellent Snappy JNI wrapper from
[xerial](http://code.google.com/p/snappy-java/).  As you can see in the results
below, the pure Java port is 20-30% faster for block compress, 0-10% slower
for block uncompress, and 0-5% slower for round-trip block compression.  The
streaming mode is significantly faster that the Snappy JNI library due to
the completely unoptimized stream implementation in Snappy JNI.  These results
were run with Java 7 on a Core i7, 64-bit Mac.

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


### Block Compress
<pre><code>
                        JNI      Java         JNI        Java
Input        Size  Compress  Compress  Throughput  Throughput  Change
---------------------------------------------------------------------
html       102400     76.4%     76.4%   377.9MB/s   291.4MB/s  +29.7%  html
urls       702087     49.1%     49.1%   221.9MB/s   176.4MB/s  +25.8%  urls
jpg        126958      0.1%      0.1%     2.8GB/s     2.6GB/s   +6.1%  jpg (not compressible)
pdf         94330     17.9%     17.9%   847.6MB/s   633.5MB/s  +33.8%  pdf
html4      409600     76.4%     76.4%   372.6MB/s   286.6MB/s  +30.0%  html4
cp          24603     51.9%     51.9%   218.6MB/s   162.1MB/s  +34.8%  cp
c           11150     57.6%     57.6%   258.5MB/s   175.0MB/s  +47.7%  c
lsp          3721     51.6%     51.6%   275.1MB/s   241.9MB/s  +13.7%  lsp
xls       1029744     58.7%     58.7%   286.4MB/s   258.2MB/s  +10.9%  xls
txt1       152089     40.2%     40.2%   161.3MB/s   116.0MB/s  +39.1%  txt1
txt2       125179     35.9%     35.9%   152.9MB/s   112.5MB/s  +35.9%  txt2
txt3       426754     42.9%     42.9%   169.1MB/s   122.2MB/s  +38.4%  txt3
txt4       481861     31.7%     31.7%   144.8MB/s   106.6MB/s  +35.8%  txt4
bin        513216     81.8%     81.8%   492.8MB/s   403.8MB/s  +22.0%  bin
sum         38240     48.1%     48.1%   205.9MB/s   160.2MB/s  +28.5%  sum
man          4227     40.6%     40.6%   224.7MB/s   192.2MB/s  +16.9%  man
pb         118588     76.8%     76.8%   437.8MB/s   359.0MB/s  +22.0%  pb
gaviota    184320     61.7%     61.7%   253.0MB/s   165.5MB/s  +52.9%  gaviota
</code></pre>


### Block Uncompress
<pre><code>
                        JNI      Java         JNI        Java
Input        Size  Compress  Compress  Throughput  Throughput  Change
---------------------------------------------------------------------
html       102400     76.4%     76.4%     1.5GB/s     1.3GB/s  -12.6%  html
urls       702087     49.1%     49.1%   950.8MB/s   820.4MB/s  -13.7%  urls
jpg        126958      0.1%      0.1%    18.7GB/s    18.5GB/s   -0.9%  jpg (not compressible)
pdf         94330     17.9%     17.9%     4.1GB/s     3.7GB/s   -7.9%  pdf
html4      409600     76.4%     76.4%     1.4GB/s     1.2GB/s  -16.9%  html4
cp          24603     51.9%     51.9%   970.1MB/s   949.1MB/s   -2.2%  cp
c           11150     57.6%     57.6%   978.5MB/s   917.1MB/s   -6.3%  c
lsp          3721     51.6%     51.6%   986.0MB/s   961.5MB/s   -2.5%  lsp
xls       1029744     58.7%     58.7%   792.1MB/s   740.8MB/s   -6.5%  xls
txt1       152089     40.2%     40.2%   640.3MB/s   580.9MB/s   -9.3%  txt1
txt2       125179     35.9%     35.9%   600.9MB/s   545.5MB/s   -9.2%  txt2
txt3       426754     42.9%     42.9%   673.7MB/s   606.7MB/s  -10.0%  txt3
txt4       481861     31.7%     31.7%   555.2MB/s   503.0MB/s   -9.4%  txt4
bin        513216     81.8%     81.8%     1.5GB/s     1.2GB/s  -20.2%  bin
sum         38240     48.1%     48.1%   836.3MB/s   768.8MB/s   -8.1%  sum
man          4227     40.6%     40.6%   853.0MB/s   839.6MB/s   -1.6%  man
pb         118588     76.8%     76.8%     1.7GB/s     1.5GB/s  -14.2%  pb
gaviota    184320     61.7%     61.7%   762.9MB/s   687.9MB/s   -9.8%  gaviota
</code></pre>


### Block Round Trip
<pre><code>
                        JNI      Java         JNI        Java
Input        Size  Compress  Compress  Throughput  Throughput  Change
---------------------------------------------------------------------
html       102400     76.4%     76.4%   295.2MB/s   282.4MB/s   -4.3%  html
urls       702087     49.1%     49.1%   180.7MB/s   175.9MB/s   -2.7%  urls
jpg        126958      0.1%      0.1%     2.3GB/s     2.3GB/s   +0.3%  jpg (not compressible)
pdf         94330     17.9%     17.9%   668.5MB/s   653.1MB/s   -2.3%  pdf
html4      409600     76.4%     76.4%   294.8MB/s   280.4MB/s   -4.9%  html4
cp          24603     51.9%     51.9%   168.8MB/s   165.3MB/s   -2.1%  cp
c           11150     57.6%     57.6%   198.2MB/s   195.2MB/s   -1.5%  c
lsp          3721     51.6%     51.6%   211.9MB/s   213.0MB/s   +0.5%  lsp
xls       1029744     58.7%     58.7%   209.9MB/s   206.2MB/s   -1.8%  xls
txt1       152089     40.2%     40.2%   129.4MB/s   125.9MB/s   -2.7%  txt1
txt2       125179     35.9%     35.9%   121.4MB/s   118.5MB/s   -2.3%  txt2
txt3       426754     42.9%     42.9%   134.8MB/s   132.0MB/s   -2.1%  txt3
txt4       481861     31.7%     31.7%   114.9MB/s   112.5MB/s   -2.1%  txt4
bin        513216     81.8%     81.8%   367.1MB/s   345.3MB/s   -5.9%  bin
sum         38240     48.1%     48.1%   160.0MB/s   156.9MB/s   -1.9%  sum
man          4227     40.6%     40.6%   176.0MB/s   175.5MB/s   -0.3%  man
pb         118588     76.8%     76.8%   340.7MB/s   325.3MB/s   -4.5%  pb
gaviota    184320     61.7%     61.7%   187.9MB/s   182.6MB/s   -2.8%  gaviota
</code></pre>


### Stream Compress
<pre><code>
                        JNI      Java         JNI        Java
Input        Size  Compress  Compress  Throughput  Throughput  Change
---------------------------------------------------------------------
html       102400     76.4%     76.4%   350.3MB/s   271.1MB/s  +29.2%  html
urls       702087     49.1%     49.1%   217.6MB/s   173.5MB/s  +25.4%  urls
jpg        126958      0.1%     -0.0%     1.5GB/s     1.7GB/s  -11.4%  jpg (not compressible)
pdf         94330     17.8%     16.0%   710.8MB/s   551.5MB/s  +28.9%  pdf
html4      409600     76.4%     76.4%   356.7MB/s   278.1MB/s  +28.3%  html4
cp          24603     51.8%     51.9%   200.5MB/s   149.0MB/s  +34.6%  cp
c           11150     57.4%     57.6%   214.4MB/s   147.3MB/s  +45.5%  c
lsp          3721     51.1%     51.5%   186.1MB/s   139.7MB/s  +33.2%  lsp
xls       1029744     58.6%     58.6%   277.6MB/s   251.2MB/s  +10.5%  xls
txt1       152089     40.2%     40.2%   156.9MB/s   113.5MB/s  +38.3%  txt1
txt2       125179     35.9%     35.9%   148.3MB/s   109.1MB/s  +35.9%  txt2
txt3       426754     42.9%     42.9%   165.0MB/s   119.7MB/s  +37.8%  txt3
txt4       481861     31.6%     31.6%   142.0MB/s   105.2MB/s  +35.0%  txt4
bin        513216     81.8%     81.8%   466.2MB/s   386.7MB/s  +20.6%  bin
sum         38240     48.1%     48.1%   197.2MB/s   150.9MB/s  +30.7%  sum
man          4227     40.2%     40.6%   164.6MB/s   125.4MB/s  +31.3%  man
pb         118588     76.8%     76.8%   408.6MB/s   334.1MB/s  +22.3%  pb
gaviota    184320     61.7%     61.7%   242.1MB/s   159.5MB/s  +51.8%  gaviota
</code></pre>


### Stream Uncompress
<pre><code>
                        JNI      Java         JNI        Java
Input        Size  Compress  Compress  Throughput  Throughput  Change
---------------------------------------------------------------------
html       102400     76.4%     76.4%     1.2GB/s     1.2GB/s   -0.4%  html
urls       702087     49.1%     49.1%   844.7MB/s   770.3MB/s   -8.8%  urls
jpg        126958      0.1%     -0.0%     3.0GB/s    10.8GB/s +257.6%  jpg (not compressible)
pdf         94330     17.8%     16.0%     2.0GB/s     3.4GB/s  +69.2%  pdf
html4      409600     76.4%     76.4%     1.2GB/s     1.1GB/s   -8.8%  html4
cp          24603     51.8%     51.9%   772.7MB/s   890.2MB/s  +15.2%  cp
c           11150     57.4%     57.6%   776.1MB/s   874.5MB/s  +12.7%  c
lsp          3721     51.1%     51.5%   729.3MB/s   890.6MB/s  +22.1%  lsp
xls       1029744     58.6%     58.6%   719.5MB/s   704.4MB/s   -2.1%  xls
txt1       152089     40.2%     40.2%   577.2MB/s   553.5MB/s   -4.1%  txt1
txt2       125179     35.9%     35.9%   537.7MB/s   521.1MB/s   -3.1%  txt2
txt3       426754     42.9%     42.9%   614.0MB/s   578.4MB/s   -5.8%  txt3
txt4       481861     31.6%     31.6%   515.4MB/s   479.3MB/s   -7.0%  txt4
bin        513216     81.8%     81.8%     1.2GB/s     1.1GB/s  -12.5%  bin
sum         38240     48.1%     48.1%   689.1MB/s   721.1MB/s   +4.6%  sum
man          4227     40.2%     40.6%   633.6MB/s   791.6MB/s  +24.9%  man
pb         118588     76.8%     76.8%     1.4GB/s     1.3GB/s   -1.1%  pb
gaviota    184320     61.7%     61.7%   685.6MB/s   650.5MB/s   -5.1%  gaviota
</code></pre>


### Stream RoundTrip
<pre><code>
                        JNI      Java         JNI        Java
Input        Size  Compress  Compress  Throughput  Throughput  Change
---------------------------------------------------------------------
html       102400     76.4%     76.4%   222.4MB/s   267.6MB/s  +20.3%  html
urls       702087     49.1%     49.1%   144.1MB/s   169.3MB/s  +17.5%  urls
jpg        126958      0.1%     -0.0%     1.1GB/s     1.3GB/s  +20.2%  jpg (not compressible)
pdf         94330     17.8%     16.0%   418.0MB/s   556.7MB/s  +33.2%  pdf
html4      409600     76.4%     76.4%   225.6MB/s   265.6MB/s  +17.8%  html4
cp          24603     51.8%     51.9%   123.8MB/s   153.1MB/s  +23.7%  cp
c           11150     57.4%     57.6%   123.5MB/s   166.7MB/s  +35.0%  c
lsp          3721     51.1%     51.5%   129.2MB/s   152.4MB/s  +18.0%  lsp
xls       1029744     58.6%     58.6%   186.3MB/s   198.4MB/s   +6.5%  xls
txt1       152089     40.2%     40.2%    94.3MB/s   121.7MB/s  +29.0%  txt1
txt2       125179     35.9%     35.9%    90.5MB/s   114.9MB/s  +27.0%  txt2
txt3       426754     42.9%     42.9%    99.8MB/s   128.2MB/s  +28.4%  txt3
txt4       481861     31.6%     31.6%    86.8MB/s   109.1MB/s  +25.7%  txt4
bin        513216     81.8%     81.8%   292.3MB/s   324.9MB/s  +11.2%  bin
sum         38240     48.1%     48.1%   122.3MB/s   149.1MB/s  +22.0%  sum
man          4227     40.2%     40.6%   111.8MB/s   135.2MB/s  +20.9%  man
pb         118588     76.8%     76.8%   267.4MB/s   301.8MB/s  +12.9%  pb
gaviota    184320     61.7%     61.7%   129.8MB/s   175.9MB/s  +35.5%  gaviota
</code></pre>
