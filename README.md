# Snappy in Java

This is a rewrite (port) of [Snappy](http://code.google.com/p/snappy/) writen in
pure Java. This compression code produces a byte-for-byte exact copy of the output
created by the original C++ code, and extremely fast.

# Performance

The Snappy micro-benchmark has been ported, and can be used to measure
the performance of this code against the excellent Snappy JNI wrapper from
[xerial](http://code.google.com/p/snappy-java/).  As you can see in the results
below, the pure Java port is 20-30% faster for block compress, 0-10% slower
for block uncompress, and 0-5% slower for round-trip block compression.  These
results were run with Java 7 on a Core i7, 64-bit Mac.

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
html       102400     76.4%     76.4%   371.5MB/s   390.4MB/s   +5.1%  html
urls       702087     49.1%     49.1%   219.8MB/s   225.3MB/s   +2.5%  urls
jpg        126958      0.1%      0.1%     3.5GB/s     3.1GB/s  -11.1%  jpg (not compressible)
pdf         94330     17.9%     17.9%   824.7MB/s   902.0MB/s   +9.4%  pdf
html4      409600     76.4%     76.4%   365.4MB/s   386.2MB/s   +5.7%  html4
cp          24603     51.9%     51.9%   206.0MB/s   230.7MB/s  +12.0%  cp
c           11150     57.6%     57.6%   220.8MB/s   298.3MB/s  +35.1%  c
lsp          3721     51.6%     51.6%   291.3MB/s   281.7MB/s   -3.3%  lsp
xls       1029744     58.7%     58.7%   309.5MB/s   299.0MB/s   -3.4%  xls
txt1       152089     40.2%     40.2%   140.6MB/s   166.8MB/s  +18.6%  txt1
txt2       125179     35.9%     35.9%   136.7MB/s   155.3MB/s  +13.6%  txt2
txt3       426754     42.9%     42.9%   146.9MB/s   173.9MB/s  +18.4%  txt3
txt4       481861     31.7%     31.7%   130.0MB/s   148.5MB/s  +14.2%  txt4
bin        513216     81.8%     81.8%   514.2MB/s   509.6MB/s   -0.9%  bin
sum         38240     48.1%     48.1%   201.4MB/s   213.5MB/s   +6.0%  sum
man          4227     40.6%     40.6%   240.2MB/s   234.4MB/s   -2.4%  man
pb         118588     76.8%     76.8%   459.7MB/s   453.0MB/s   -1.5%  pb
gaviota    184320     61.7%     61.7%   201.6MB/s   266.1MB/s  +32.0%  gaviota
</code></pre>


### Block Uncompress
<pre><code>
                        JNI      Java         JNI        Java
Input        Size  Compress  Compress  Throughput  Throughput  Change
---------------------------------------------------------------------
html       102400     76.4%     76.4%     1.2GB/s     1.4GB/s  +16.9%  html
urls       702087     49.1%     49.1%   814.3MB/s   874.3MB/s   +7.4%  urls
jpg        126958      0.1%      0.1%    20.1GB/s    19.7GB/s   -2.0%  jpg (not compressible)
pdf         94330     17.9%     17.9%     3.2GB/s     3.9GB/s  +24.4%  pdf
html4      409600     76.4%     76.4%     1.1GB/s     1.3GB/s  +13.2%  html4
cp          24603     51.9%     51.9%   777.4MB/s  1016.2MB/s  +30.7%  cp
c           11150     57.6%     57.6%   789.0MB/s   972.3MB/s  +23.2%  c
lsp          3721     51.6%     51.6%   931.7MB/s  1014.3MB/s   +8.9%  lsp
xls       1029744     58.7%     58.7%   724.8MB/s   790.1MB/s   +9.0%  xls
txt1       152089     40.2%     40.2%   484.5MB/s   607.0MB/s  +25.3%  txt1
txt2       125179     35.9%     35.9%   458.2MB/s   571.2MB/s  +24.7%  txt2
txt3       426754     42.9%     42.9%   508.2MB/s   631.1MB/s  +24.2%  txt3
txt4       481861     31.7%     31.7%   419.7MB/s   522.8MB/s  +24.6%  txt4
bin        513216     81.8%     81.8%     1.2GB/s     1.2GB/s   +3.1%  bin
sum         38240     48.1%     48.1%   676.2MB/s   811.2MB/s  +20.0%  sum
man          4227     40.6%     40.6%   777.2MB/s   867.5MB/s  +11.6%  man
pb         118588     76.8%     76.8%     1.4GB/s     1.5GB/s  +13.1%  pb
gaviota    184320     61.7%     61.7%   574.8MB/s   698.9MB/s  +21.6%  gaviota
</code></pre>


### Block Round Trip
<pre><code>
                        JNI      Java         JNI        Java
Input        Size  Compress  Compress  Throughput  Throughput  Change
---------------------------------------------------------------------
html       102400     76.4%     76.4%   281.8MB/s   293.3MB/s   +4.1%  html
urls       702087     49.1%     49.1%   172.3MB/s   175.5MB/s   +1.9%  urls
jpg        126958      0.1%      0.1%     2.5GB/s     2.5GB/s   -0.1%  jpg (not compressible)
pdf         94330     17.9%     17.9%   639.9MB/s   671.9MB/s   +5.0%  pdf
html4      409600     76.4%     76.4%   279.5MB/s   286.4MB/s   +2.5%  html4
cp          24603     51.9%     51.9%   166.1MB/s   170.1MB/s   +2.4%  cp
c           11150     57.6%     57.6%   196.6MB/s   212.7MB/s   +8.2%  c
lsp          3721     51.6%     51.6%   207.0MB/s   215.1MB/s   +3.9%  lsp
xls       1029744     58.7%     58.7%   209.1MB/s   214.2MB/s   +2.4%  xls
txt1       152089     40.2%     40.2%   120.6MB/s   127.2MB/s   +5.5%  txt1
txt2       125179     35.9%     35.9%   113.6MB/s   119.4MB/s   +5.1%  txt2
txt3       426754     42.9%     42.9%   127.9MB/s   134.9MB/s   +5.4%  txt3
txt4       481861     31.7%     31.7%   110.3MB/s   115.0MB/s   +4.2%  txt4
bin        513216     81.8%     81.8%   357.4MB/s   359.2MB/s   +0.5%  bin
sum         38240     48.1%     48.1%   157.9MB/s   162.6MB/s   +3.0%  sum
man          4227     40.6%     40.6%   178.3MB/s   184.3MB/s   +3.4%  man
pb         118588     76.8%     76.8%   332.2MB/s   338.8MB/s   +2.0%  pb
gaviota    184320     61.7%     61.7%   180.6MB/s   190.3MB/s   +5.4%  gaviota
</code></pre>

# Stream Format

There is no defined stream format for Snappy, but there is an effort to create
a common format with the Google Snappy project.

The stream format used in this library has a couple of unique features not
found in the other Snappy stream formats.  Like the other formats, the user
input is broken into blocks and each block is compressed.  If the compressed
block is smaller that the user input, the compressed block is written,
otherwise the uncompressed original is written.  This dramatically improves the
speed of uncompressible input such as JPG images.  Additionally, a checksum of
the user input data for each block is written to the stream.  This safety check
assures that the stream has not been corrupted in transit or by a bad Snappy
implementation.  Finally, like gzip, compressed Snappy files can be
concatenated together without issue, since the input stream will ignore a
Snappy stream header in the middle of a stream.  This makes combining files in
Hadoop and S3 trivial.

The the SnappyOutputStream javadocs contain formal definition of the stream
format.

## Stream Performance

The streaming mode performance can not be directly compared to other
compression algorithms since most formats do not contain a checksum.  The basic
streaming code is significantly faster that the Snappy JNI library due to
the completely unoptimized stream implementation in Snappy JNI, but once the
check sum is enabled the performance drops off by about 20%.

### Stream Compress (no checksums)
<pre><code>
                        JNI      Java         JNI        Java
Input        Size  Compress  Compress  Throughput  Throughput  Change
---------------------------------------------------------------------
html       102400     76.4%     76.4%   340.3MB/s   390.6MB/s  +14.8%  html
urls       702087     49.1%     49.1%   213.1MB/s   222.9MB/s   +4.6%  urls
jpg        126958      0.1%     -0.0%     2.1GB/s     2.5GB/s  +18.5%  jpg (not compressible)
pdf         94330     17.8%     16.0%   713.9MB/s   849.3MB/s  +19.0%  pdf
html4      409600     76.4%     76.4%   350.7MB/s   387.1MB/s  +10.4%  html4
cp          24603     51.8%     51.8%   184.5MB/s   228.9MB/s  +24.1%  cp
c           11150     57.4%     57.5%   193.8MB/s   292.0MB/s  +50.6%  c
lsp          3721     51.1%     51.2%   204.6MB/s   280.4MB/s  +37.1%  lsp
xls       1029744     58.6%     58.6%   299.9MB/s   301.2MB/s   +0.4%  xls
txt1       152089     40.2%     40.2%   137.2MB/s   165.6MB/s  +20.7%  txt1
txt2       125179     35.9%     35.9%   133.3MB/s   154.9MB/s  +16.2%  txt2
txt3       426754     42.9%     42.9%   144.8MB/s   171.1MB/s  +18.2%  txt3
txt4       481861     31.6%     31.6%   127.5MB/s   147.9MB/s  +16.0%  txt4
bin        513216     81.8%     81.8%   487.2MB/s   507.0MB/s   +4.1%  bin
sum         38240     48.1%     48.1%   185.9MB/s   212.0MB/s  +14.0%  sum
man          4227     40.2%     40.3%   179.7MB/s   236.0MB/s  +31.3%  man
pb         118588     76.8%     76.8%   436.2MB/s   446.9MB/s   +2.5%  pb
gaviota    184320     61.7%     61.7%   195.2MB/s   264.6MB/s  +35.6%  gaviota
</code></pre>


### Stream Uncompress (no checksum)
<pre><code>
                        JNI      Java         JNI        Java
Input        Size  Compress  Compress  Throughput  Throughput  Change
---------------------------------------------------------------------
html       102400     76.4%     76.4%     1.0GB/s     1.3GB/s  +27.1%  html
urls       702087     49.1%     49.1%   739.6MB/s   827.1MB/s  +11.8%  urls
jpg        126958      0.1%     -0.0%     3.8GB/s    12.8GB/s +238.5%  jpg (not compressible)
pdf         94330     17.8%     16.0%     1.9GB/s     3.7GB/s  +95.7%  pdf
html4      409600     76.4%     76.4%  1023.5MB/s     1.2GB/s  +20.2%  html4
cp          24603     51.8%     51.8%   658.2MB/s   960.6MB/s  +45.9%  cp
c           11150     57.4%     57.5%   664.3MB/s   940.4MB/s  +41.6%  c
lsp          3721     51.1%     51.2%   706.6MB/s   943.7MB/s  +33.6%  lsp
xls       1029744     58.6%     58.6%   671.2MB/s   752.6MB/s  +12.1%  xls
txt1       152089     40.2%     40.2%   455.7MB/s   587.9MB/s  +29.0%  txt1
txt2       125179     35.9%     35.9%   423.2MB/s   558.7MB/s  +32.0%  txt2
txt3       426754     42.9%     42.9%   484.3MB/s   618.2MB/s  +27.6%  txt3
txt4       481861     31.6%     31.6%   403.7MB/s   514.4MB/s  +27.4%  txt4
bin        513216     81.8%     81.8%     1.0GB/s     1.2GB/s  +11.9%  bin
sum         38240     48.1%     48.1%   595.4MB/s   782.1MB/s  +31.4%  sum
man          4227     40.2%     40.3%   609.6MB/s   816.8MB/s  +34.0%  man
pb         118588     76.8%     76.8%     1.2GB/s     1.4GB/s  +24.3%  pb
gaviota    184320     61.7%     61.7%   517.2MB/s   679.8MB/s  +31.4%  gaviota
</code></pre>


### Stream RoundTrip (no checksum)
<pre><code>
                        JNI      Java         JNI        Java
Input        Size  Compress  Compress  Throughput  Throughput  Change
---------------------------------------------------------------------
html       102400     76.4%     76.4%   253.0MB/s   289.2MB/s  +14.3%  html
urls       702087     49.1%     49.1%   164.4MB/s   175.8MB/s   +6.9%  urls
jpg        126958      0.1%     -0.0%     1.4GB/s     2.0GB/s  +48.0%  jpg (not compressible)
pdf         94330     17.8%     16.0%   480.2MB/s   636.8MB/s  +32.6%  pdf
html4      409600     76.4%     76.4%   256.8MB/s   285.5MB/s  +11.2%  html4
cp          24603     51.8%     51.8%   141.0MB/s   169.8MB/s  +20.5%  cp
c           11150     57.4%     57.5%   141.9MB/s   211.6MB/s  +49.1%  c
lsp          3721     51.1%     51.2%   150.2MB/s   212.8MB/s  +41.6%  lsp
xls       1029744     58.6%     58.6%   206.2MB/s   213.5MB/s   +3.5%  xls
txt1       152089     40.2%     40.2%   105.2MB/s   127.9MB/s  +21.6%  txt1
txt2       125179     35.9%     35.9%   100.4MB/s   119.7MB/s  +19.2%  txt2
txt3       426754     42.9%     42.9%   110.3MB/s   133.8MB/s  +21.3%  txt3
txt4       481861     31.6%     31.6%    95.1MB/s   113.0MB/s  +18.8%  txt4
bin        513216     81.8%     81.8%   327.2MB/s   343.7MB/s   +5.0%  bin
sum         38240     48.1%     48.1%   138.5MB/s   158.9MB/s  +14.7%  sum
man          4227     40.2%     40.3%   128.7MB/s   178.2MB/s  +38.4%  man
pb         118588     76.8%     76.8%   308.6MB/s   331.0MB/s   +7.2%  pb
gaviota    184320     61.7%     61.7%   138.0MB/s   189.4MB/s  +37.2%  gaviota
</code></pre>
