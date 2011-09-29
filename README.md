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
html       102400     76.4%     76.4%   297.1MB/s   396.9MB/s  +33.6%  html
urls       702087     49.1%     49.1%   181.8MB/s   230.2MB/s  +26.6%  urls
jpg        126958      0.1%      0.1%     2.7GB/s     3.2GB/s  +17.7%  jpg (not compressible)
pdf         94330     17.9%     17.9%   647.8MB/s   922.7MB/s  +42.4%  pdf
html4      409600     76.4%     76.4%   294.0MB/s   383.4MB/s  +30.4%  html4
cp          24603     51.9%     51.9%   165.4MB/s   236.5MB/s  +43.0%  cp
c           11150     57.6%     57.6%   181.8MB/s   297.3MB/s  +63.5%  c
lsp          3721     51.6%     51.6%   248.1MB/s   282.3MB/s  +13.8%  lsp
xls       1029744     58.7%     58.7%   263.5MB/s   296.0MB/s  +12.3%  xls
txt1       152089     40.2%     40.2%   118.1MB/s   163.9MB/s  +38.8%  txt1
txt2       125179     35.9%     35.9%   114.0MB/s   155.4MB/s  +36.3%  txt2
txt3       426754     42.9%     42.9%   124.7MB/s   171.0MB/s  +37.1%  txt3
txt4       481861     31.7%     31.7%   109.2MB/s   147.1MB/s  +34.8%  txt4
bin        513216     81.8%     81.8%   411.1MB/s   501.9MB/s  +22.1%  bin
sum         38240     48.1%     48.1%   164.0MB/s   217.3MB/s  +32.5%  sum
man          4227     40.6%     40.6%   195.4MB/s   243.0MB/s  +24.4%  man
pb         118588     76.8%     76.8%   366.9MB/s   455.1MB/s  +24.0%  pb
gaviota    184320     61.7%     61.7%   168.4MB/s   256.8MB/s  +52.5%  gaviota
</code></pre>


### Block Uncompress
<pre><code>
                        JNI      Java         JNI        Java
Input        Size  Compress  Compress  Throughput  Throughput  Change
---------------------------------------------------------------------
html       102400     76.4%     76.4%     1.5GB/s     1.3GB/s  -13.3%  html
urls       702087     49.1%     49.1%   971.8MB/s   836.5MB/s  -13.9%  urls
jpg        126958      0.1%      0.1%    19.0GB/s    19.1GB/s   +0.6%  jpg (not compressible)
pdf         94330     17.9%     17.9%     4.2GB/s     3.8GB/s  -10.4%  pdf
html4      409600     76.4%     76.4%     1.5GB/s     1.2GB/s  -16.7%  html4
cp          24603     51.9%     51.9%   987.9MB/s   931.1MB/s   -5.7%  cp
c           11150     57.6%     57.6%  1000.4MB/s   932.4MB/s   -6.8%  c
lsp          3721     51.6%     51.6%  1018.3MB/s   992.9MB/s   -2.5%  lsp
xls       1029744     58.7%     58.7%   808.9MB/s   756.8MB/s   -6.4%  xls
txt1       152089     40.2%     40.2%   653.6MB/s   590.1MB/s   -9.7%  txt1
txt2       125179     35.9%     35.9%   614.5MB/s   555.2MB/s   -9.7%  txt2
txt3       426754     42.9%     42.9%   688.4MB/s   621.4MB/s   -9.7%  txt3
txt4       481861     31.7%     31.7%   567.1MB/s   510.2MB/s  -10.0%  txt4
bin        513216     81.8%     81.8%     1.5GB/s     1.2GB/s  -19.6%  bin
sum         38240     48.1%     48.1%   854.2MB/s   794.3MB/s   -7.0%  sum
man          4227     40.6%     40.6%   869.6MB/s   859.0MB/s   -1.2%  man
pb         118588     76.8%     76.8%     1.7GB/s     1.5GB/s  -13.9%  pb
gaviota    184320     61.7%     61.7%   776.7MB/s   695.8MB/s  -10.4%  gaviota
</code></pre>


### Block Round Trip
<pre><code>
                        JNI      Java         JNI        Java
Input        Size  Compress  Compress  Throughput  Throughput  Change
---------------------------------------------------------------------
html       102400     76.4%     76.4%   305.3MB/s   292.8MB/s   -4.1%  html
urls       702087     49.1%     49.1%   185.2MB/s   180.5MB/s   -2.5%  urls
jpg        126958      0.1%      0.1%     2.6GB/s     2.6GB/s   +1.5%  jpg (not compressible)
pdf         94330     17.9%     17.9%   710.3MB/s   693.6MB/s   -2.4%  pdf
html4      409600     76.4%     76.4%   300.5MB/s   286.2MB/s   -4.7%  html4
cp          24603     51.9%     51.9%   179.9MB/s   177.6MB/s   -1.2%  cp
c           11150     57.6%     57.6%   225.8MB/s   215.3MB/s   -4.6%  c
lsp          3721     51.6%     51.6%   217.9MB/s   217.8MB/s   -0.0%  lsp
xls       1029744     58.7%     58.7%   215.9MB/s   212.3MB/s   -1.7%  xls
txt1       152089     40.2%     40.2%   129.9MB/s   128.2MB/s   -1.3%  txt1
txt2       125179     35.9%     35.9%   123.1MB/s   120.2MB/s   -2.3%  txt2
txt3       426754     42.9%     42.9%   137.2MB/s   134.0MB/s   -2.3%  txt3
txt4       481861     31.7%     31.7%   116.6MB/s   114.1MB/s   -2.1%  txt4
bin        513216     81.8%     81.8%   374.7MB/s   353.5MB/s   -5.7%  bin
sum         38240     48.1%     48.1%   166.6MB/s   164.1MB/s   -1.5%  sum
man          4227     40.6%     40.6%   189.3MB/s   188.4MB/s   -0.5%  man
pb         118588     76.8%     76.8%   354.2MB/s   338.2MB/s   -4.5%  pb
gaviota    184320     61.7%     61.7%   190.0MB/s   185.4MB/s   -2.4%  gaviota
</code></pre>


### Stream Compress
<pre><code>
                        JNI      Java         JNI        Java
Input        Size  Compress  Compress  Throughput  Throughput  Change
---------------------------------------------------------------------
html       102400     76.4%     76.4%   276.9MB/s   282.3MB/s   +2.0%  html
urls       702087     49.1%     49.1%   177.5MB/s   190.6MB/s   +7.4%  urls
jpg        126958      0.1%     -0.0%     1.7GB/s   752.3MB/s  -56.9%  jpg (not compressible)
pdf         94330     17.8%     16.0%   561.6MB/s   480.8MB/s  -14.4%  pdf
html4      409600     76.4%     76.4%   284.6MB/s   285.9MB/s   +0.5%  html4
cp          24603     51.8%     51.9%   152.6MB/s   183.1MB/s  +20.0%  cp
c           11150     57.4%     57.5%   149.8MB/s   199.1MB/s  +33.0%  c
lsp          3721     51.1%     51.4%   139.3MB/s   154.1MB/s  +10.6%  lsp
xls       1029744     58.6%     58.6%   256.0MB/s   235.7MB/s   -7.9%  xls
txt1       152089     40.2%     40.2%   115.9MB/s   142.4MB/s  +22.9%  txt1
txt2       125179     35.9%     35.9%   111.2MB/s   135.9MB/s  +22.2%  txt2
txt3       426754     42.9%     42.9%   122.3MB/s   150.9MB/s  +23.5%  txt3
txt4       481861     31.6%     31.6%   107.3MB/s   130.0MB/s  +21.2%  txt4
bin        513216     81.8%     81.8%   394.2MB/s   351.3MB/s  -10.9%  bin
sum         38240     48.1%     48.1%   154.1MB/s   176.1MB/s  +14.2%  sum
man          4227     40.2%     40.5%   126.2MB/s   148.7MB/s  +17.8%  man
pb         118588     76.8%     76.8%   342.6MB/s   320.9MB/s   -6.3%  pb
gaviota    184320     61.7%     61.7%   165.1MB/s   208.4MB/s  +26.2%  gaviota
</code></pre>


### Stream Uncompress
<pre><code>
                        JNI      Java         JNI        Java
Input        Size  Compress  Compress  Throughput  Throughput  Change
---------------------------------------------------------------------
html       102400     76.4%     76.4%     1.2GB/s     1.2GB/s   -0.3%  html
urls       702087     49.1%     49.1%   862.5MB/s   789.6MB/s   -8.5%  urls
jpg        126958      0.1%     -0.0%     3.0GB/s    10.8GB/s +254.2%  jpg (not compressible)
pdf         94330     17.8%     16.0%     2.0GB/s     3.4GB/s  +71.1%  pdf
html4      409600     76.4%     76.4%     1.2GB/s     1.1GB/s   -8.4%  html4
cp          24603     51.8%     51.9%   783.1MB/s   865.8MB/s  +10.6%  cp
c           11150     57.4%     57.5%   784.4MB/s   882.4MB/s  +12.5%  c
lsp          3721     51.1%     51.4%   743.0MB/s   901.4MB/s  +21.3%  lsp
xls       1029744     58.6%     58.6%   738.4MB/s   722.4MB/s   -2.2%  xls
txt1       152089     40.2%     40.2%   589.2MB/s   564.4MB/s   -4.2%  txt1
txt2       125179     35.9%     35.9%   548.0MB/s   539.1MB/s   -1.6%  txt2
txt3       426754     42.9%     42.9%   628.7MB/s   594.2MB/s   -5.5%  txt3
txt4       481861     31.6%     31.6%   526.5MB/s   497.0MB/s   -5.6%  txt4
bin        513216     81.8%     81.8%     1.3GB/s     1.1GB/s  -11.7%  bin
sum         38240     48.1%     48.1%   701.1MB/s   756.4MB/s   +7.9%  sum
man          4227     40.2%     40.5%   649.6MB/s   800.6MB/s  +23.2%  man
pb         118588     76.8%     76.8%     1.4GB/s     1.4GB/s   -1.1%  pb
gaviota    184320     61.7%     61.7%   697.3MB/s   678.1MB/s   -2.7%  gaviota
</code></pre>


### Stream RoundTrip
<pre><code>
                        JNI      Java         JNI        Java
Input        Size  Compress  Compress  Throughput  Throughput  Change
---------------------------------------------------------------------
html       102400     76.4%     76.4%   227.6MB/s   225.4MB/s   -1.0%  html
urls       702087     49.1%     49.1%   146.6MB/s   153.8MB/s   +4.9%  urls
jpg        126958      0.1%     -0.0%     1.1GB/s   696.4MB/s  -37.2%  jpg (not compressible)
pdf         94330     17.8%     16.0%   424.2MB/s   407.2MB/s   -4.0%  pdf
html4      409600     76.4%     76.4%   230.5MB/s   226.6MB/s   -1.7%  html4
cp          24603     51.8%     51.9%   126.7MB/s   143.7MB/s  +13.4%  cp
c           11150     57.4%     57.5%   126.0MB/s   156.6MB/s  +24.3%  c
lsp          3721     51.1%     51.4%   129.7MB/s   130.9MB/s   +0.9%  lsp
xls       1029744     58.6%     58.6%   189.7MB/s   179.3MB/s   -5.5%  xls
txt1       152089     40.2%     40.2%    97.0MB/s   113.0MB/s  +16.5%  txt1
txt2       125179     35.9%     35.9%    92.2MB/s   107.0MB/s  +16.1%  txt2
txt3       426754     42.9%     42.9%   102.1MB/s   118.7MB/s  +16.2%  txt3
txt4       481861     31.6%     31.6%    88.8MB/s   102.2MB/s  +15.1%  txt4
bin        513216     81.8%     81.8%   299.8MB/s   266.6MB/s  -11.1%  bin
sum         38240     48.1%     48.1%   123.7MB/s   137.6MB/s  +11.3%  sum
man          4227     40.2%     40.5%   113.6MB/s   125.2MB/s  +10.2%  man
pb         118588     76.8%     76.8%   271.8MB/s   254.3MB/s   -6.5%  pb
gaviota    184320     61.7%     61.7%   132.9MB/s   158.3MB/s  +19.1%  gaviota
</code></pre>
