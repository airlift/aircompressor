# Snappy in Java

This is a rewrite (port) of [Snappy](http://code.google.com/p/snappy/) writen in
pure Java. This compression code produces a byte-for-byte exact copy of the output
created by the original C++ code, and extremely fast.

# Performance

This code base include port of the Snappy micro-benchmark.  As you can see below,
when the Java port is compared to the excellent JNI wrapper from
[xerial](http://code.google.com/p/snappy-java/) the pure Java port is 20-50% faster
is most cases, and is typically 10% slower for  decompression.

These results were run on a Core i7, 64-bit Mac, and as with all
benchmarks your mileage will vary.

<pre><code>
Benchmark                Size   Time(ns) Iterations Throughput
--------------------------------------------------------------
Compress/0/java        102400     258626        755  377.6MB/s  html (23.57 %)
Compress/0/jni         102400     332768        255  293.5MB/s  html (23.57 %)
Compress/1/java        702087    2985940        100  224.2MB/s  urls (50.89 %)
Compress/1/jni         702087    3748990        100  178.6MB/s  urls (50.89 %)
Compress/2/java        126958      42221       4746    2.8GB/s  jpg (99.88 %)
Compress/2/jni         126958      44040       4337    2.7GB/s  jpg (99.88 %)
Compress/3/java         94330     106325       1836  846.1MB/s  pdf (82.13 %)
Compress/3/jni          94330     143032       1365  628.9MB/s  pdf (82.13 %)
Compress/4/java        409600    1054510        190  370.4MB/s  html4 (23.55 %)
Compress/4/jni         409600    1375793        145  283.9MB/s  html4 (23.55 %)
Compress/5/java         24603     108565       1721  216.1MB/s  cp (48.12 %)
Compress/5/jni          24603     142857       1395  164.2MB/s  cp (48.12 %)
Compress/6/java         11150      41314       4703  257.4MB/s  c (42.40 %)
Compress/6/jni          11150      60901       3186  174.6MB/s  c (42.40 %)
Compress/7/java          3721      13067      14925  271.6MB/s  lsp (48.37 %)
Compress/7/jni           3721      14645      12345  242.3MB/s  lsp (48.37 %)
Compress/8/java       1029744    3468170        100  283.2MB/s  xls (41.34 %)
Compress/8/jni        1029744    3819150        100  257.1MB/s  xls (41.34 %)
Compress/9/java        152089     917045        218  158.2MB/s  txt1 (59.81 %)
Compress/9/jni         152089    1261300        160  115.0MB/s  txt1 (59.81 %)
Compress/10/java       125179     806052        247  148.1MB/s  txt2 (64.07 %)
Compress/10/jni        125179    1075108        185  111.0MB/s  txt2 (64.07 %)
Compress/11/java       426754    2439270        100  166.8MB/s  txt3 (57.11 %)
Compress/11/jni        426754    3338930        100  121.9MB/s  txt3 (57.11 %)
Compress/12/java       481861    3226080        100  142.4MB/s  txt4 (68.35 %)
Compress/12/jni        481861    4290260        100  107.1MB/s  txt4 (68.35 %)
Compress/13/java       513216    1001385        197  488.8MB/s  bin (18.21 %)
Compress/13/jni        513216    1220524        164  401.0MB/s  bin (18.21 %)
Compress/14/java        38240     185018       1106  197.1MB/s  sum (51.88 %)
Compress/14/jni         38240     228966        870  159.3MB/s  sum (51.88 %)
Compress/15/java         4227      18143      10643  222.2MB/s  man (59.36 %)
Compress/15/jni          4227      21394       8525  188.4MB/s  man (59.36 %)
Compress/16/java       118588     260960        761  433.4MB/s  pb (23.15 %)
Compress/16/jni        118588     315788        634  358.1MB/s  pb (23.15 %)
Compress/17/java       184320     709414        282  247.8MB/s  gaviota (38.27 %)
Compress/17/jni        184320    1073562        185  163.7MB/s  gaviota (38.27 %)

Benchmark                Size   Time(ns) Iterations Throughput
--------------------------------------------------------------
Uncompress/0/java      102400      79059       2453    1.2GB/s  html
Uncompress/0/jni       102400      65656       2920    1.5GB/s  html
Uncompress/1/java      702087     829338        242  807.3MB/s  urls
Uncompress/1/jni       702087     708386        282  945.2MB/s  urls
Uncompress/2/java      126958      21802       9433    5.4GB/s  jpg
Uncompress/2/jni       126958       6355      29197   18.6GB/s  jpg
Uncompress/3/java       94330      34374       5709    2.6GB/s  pdf
Uncompress/3/jni        94330      22352       8613    3.9GB/s  pdf
Uncompress/4/java      409600     326363        617    1.2GB/s  html4
Uncompress/4/jni       409600     265742        754    1.4GB/s  html4
Uncompress/5/java       24603      26381       7280  889.4MB/s  cp
Uncompress/5/jni        24603      24342       8045  963.9MB/s  cp
Uncompress/6/java       11150      12174      16764  873.4MB/s  c
Uncompress/6/jni        11150      10908      18903  974.8MB/s  c
Uncompress/7/java        3721       3806      55096  932.1MB/s  lsp
Uncompress/7/jni         3721       3611      53475  982.6MB/s  lsp
Uncompress/8/java     1029744    1366877        147  718.5MB/s  xls
Uncompress/8/jni      1029744    1243833        162  789.5MB/s  xls
Uncompress/9/java      152089     267391        745  542.4MB/s  txt1
Uncompress/9/jni       152089     229473        877  632.1MB/s  txt1
Uncompress/10/java     125179     234033        856  510.1MB/s  txt2
Uncompress/10/jni      125179     199316        994  598.9MB/s  txt2
Uncompress/11/java     426754     712130        283  571.5MB/s  txt3
Uncompress/11/jni      426754     604990        332  672.7MB/s  txt3
Uncompress/12/java     481861     962692        208  477.3MB/s  txt4
Uncompress/12/jni      481861     826646        243  555.9MB/s  txt4
Uncompress/13/java     513216     405202        494    1.2GB/s  bin
Uncompress/13/jni      513216     330843        606    1.4GB/s  bin
Uncompress/14/java      38240      49699       3930  733.8MB/s  sum
Uncompress/14/jni       38240      43878       4530  831.1MB/s  sum
Uncompress/15/java       4227       4971      39525  810.8MB/s  man
Uncompress/15/jni        4227       4757      41067  847.4MB/s  man
Uncompress/16/java     118588      77977       2455    1.4GB/s  pb
Uncompress/16/jni      118588      66731       3012    1.7GB/s  pb
Uncompress/17/java     184320     274083        729  641.3MB/s  gaviota
Uncompress/17/jni      184320     232298        850  756.7MB/s  gaviota
</code></pre>
