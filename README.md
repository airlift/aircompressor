# Snappy in Java

This is a rewrite (port) of [Snappy](http://code.google.com/p/snappy/) writen in
pure Java. This compression code produces a byte-for-byte exact copy of the output
created by the original C++ code, and extremely fast.

# Performance

This code base include port of the Snappy micro-benchmark.  As you can see below,
when the Java port is compared to the excellent JNI wrapper from
[xerial](http://code.google.com/p/snappy-java/) the pure Java port is 20-50% faster
is most cases for compression, and is typically 10% slower for decompression.

These results were run on a Core i7, 64-bit Mac, and as with all
benchmarks your mileage will vary.

<pre><code>
Benchmark                Size   Time(ns) Iterations Throughput
--------------------------------------------------------------
Compress/0/java        102400     258260       1440  378.1MB/s  html (23.57 %)
Compress/0/jni         102400     334565        509  291.9MB/s  html (23.57 %)
Compress/1/java        702087    2985135        133  224.3MB/s  urls (50.89 %)
Compress/1/jni         702087    3723509        106  179.8MB/s  urls (50.89 %)
Compress/2/java        126958      41800       8591    2.8GB/s  jpg (99.88 %)
Compress/2/jni         126958      43351       9212    2.7GB/s  jpg (99.88 %)
Compress/3/java         94330     105446       3857  853.1MB/s  pdf (82.13 %)
Compress/3/jni          94330     141217       2780  637.0MB/s  pdf (82.13 %)
Compress/4/java        409600    1045507        384  373.6MB/s  html4 (23.55 %)
Compress/4/jni         409600    1350479        296  289.2MB/s  html4 (23.55 %)
Compress/5/java         24603     107016       3778  219.2MB/s  cp (48.12 %)
Compress/5/jni          24603     142849       2724  164.3MB/s  cp (48.12 %)
Compress/6/java         11150      40881       9891  260.1MB/s  c (42.40 %)
Compress/6/jni          11150      60052       6141  177.1MB/s  c (42.40 %)
Compress/7/java          3721      12946      30007  274.1MB/s  lsp (48.37 %)
Compress/7/jni           3721      14565      24242  243.6MB/s  lsp (48.37 %)
Compress/8/java       1029744    3393641        117  289.4MB/s  xls (41.34 %)
Compress/8/jni        1029744    3785447        105  259.4MB/s  xls (41.34 %)
Compress/9/java        152089     902063        438  160.8MB/s  txt1 (59.81 %)
Compress/9/jni         152089    1244254        322  116.6MB/s  txt1 (59.81 %)
Compress/10/java       125179     791633        505  150.8MB/s  txt2 (64.07 %)
Compress/10/jni        125179    1067128        373  111.9MB/s  txt2 (64.07 %)
Compress/11/java       426754    2426834        163  167.7MB/s  txt3 (57.11 %)
Compress/11/jni        426754    3319441        120  122.6MB/s  txt3 (57.11 %)
Compress/12/java       481861    3203878        123  143.4MB/s  txt4 (68.35 %)
Compress/12/jni        481861    4312890        100  106.6MB/s  txt4 (68.35 %)
Compress/13/java       513216     998922        399  490.0MB/s  bin (18.21 %)
Compress/13/jni        513216    1205818        331  405.9MB/s  bin (18.21 %)
Compress/14/java        38240     177569       2294  205.4MB/s  sum (51.88 %)
Compress/14/jni         38240     224951       1762  162.1MB/s  sum (51.88 %)
Compress/15/java         4227      18010      22547  223.8MB/s  man (59.36 %)
Compress/15/jni          4227      20746      18075  194.3MB/s  man (59.36 %)
Compress/16/java       118588     259417       1540  436.0MB/s  pb (23.15 %)
Compress/16/jni        118588     312005       1266  362.5MB/s  pb (23.15 %)
Compress/17/java       184320     705996        568  249.0MB/s  gaviota (38.27 %)
Compress/17/jni        184320    1057406        376  166.2MB/s  gaviota (38.27 %)

Benchmark                Size   Time(ns) Iterations Throughput
--------------------------------------------------------------
Uncompress/0/java      102400      74327       5136    1.3GB/s  html
Uncompress/0/jni       102400      65142        779    1.5GB/s  html
Uncompress/1/java      702087     806947        499  829.7MB/s  urls
Uncompress/1/jni       702087     702786        563  952.7MB/s  urls
Uncompress/2/java      126958       6312      62893   18.7GB/s  jpg
Uncompress/2/jni       126958       6433      61255   18.4GB/s  jpg
Uncompress/3/java       94330      23482      15917    3.7GB/s  pdf
Uncompress/3/jni        94330      21334      17497    4.1GB/s  pdf
Uncompress/4/java      409600     314347       1282    1.2GB/s  html4
Uncompress/4/jni       409600     265336       1508    1.4GB/s  html4
Uncompress/5/java       24603      25590      14864  916.9MB/s  cp
Uncompress/5/jni        24603      24322      15564  964.7MB/s  cp
Uncompress/6/java       11150      11494      33955  925.1MB/s  c
Uncompress/6/jni        11150      10794      34843  985.1MB/s  c
Uncompress/7/java        3721       3674     104712  965.6MB/s  lsp
Uncompress/7/jni         3721       3586     114613  989.5MB/s  lsp
Uncompress/8/java     1029744    1303435        310  753.4MB/s  xls
Uncompress/8/jni      1029744    1234733        327  795.3MB/s  xls
Uncompress/9/java      152089     248061       1633  584.7MB/s  txt1
Uncompress/9/jni       152089     226779       1781  639.6MB/s  txt1
Uncompress/10/java     125179     217037       1848  550.0MB/s  txt2
Uncompress/10/jni      125179     196529       2044  607.4MB/s  txt2
Uncompress/11/java     426754     661597        599  615.2MB/s  txt3
Uncompress/11/jni      426754     601945        660  676.1MB/s  txt3
Uncompress/12/java     481861     900297        440  510.4MB/s  txt4
Uncompress/12/jni      481861     818547        495  561.4MB/s  txt4
Uncompress/13/java     513216     406732        966    1.2GB/s  bin
Uncompress/13/jni      513216     326275       1210    1.5GB/s  bin
Uncompress/14/java      38240      46639       8164  781.9MB/s  sum
Uncompress/14/jni       38240      43448       8940  839.3MB/s  sum
Uncompress/15/java       4227       4726      82987  852.9MB/s  man
Uncompress/15/jni        4227       4702      85653  857.2MB/s  man
Uncompress/16/java     118588      74348       5215    1.5GB/s  pb
Uncompress/16/jni      118588      65282       6049    1.7GB/s  pb
Uncompress/17/java     184320     254445       1568  690.8MB/s  gaviota
Uncompress/17/jni      184320     231212       1702  760.3MB/s  gaviota
</code></pre>
