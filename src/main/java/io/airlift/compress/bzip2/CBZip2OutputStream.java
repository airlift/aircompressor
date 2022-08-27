/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package io.airlift.compress.bzip2;

import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.OutputStream;

import static io.airlift.compress.bzip2.BZip2Constants.G_SIZE;
import static io.airlift.compress.bzip2.BZip2Constants.MAX_ALPHA_SIZE;
import static io.airlift.compress.bzip2.BZip2Constants.MAX_SELECTORS;
import static io.airlift.compress.bzip2.BZip2Constants.N_GROUPS;
import static io.airlift.compress.bzip2.BZip2Constants.RUN_A;
import static io.airlift.compress.bzip2.BZip2Constants.RUN_B;

/**
 * An output stream that compresses into the BZip2 format (without the file
 * header chars) into another stream.
 *
 * <p>
 * The compression requires large amounts of memory. Thus you should call the
 * {@link #close() close()} method as soon as possible, to force
 * <tt>CBZip2OutputStream</tt> to release the allocated memory.
 * </p>
 *
 * <p>
 * You can shrink the amount of allocated memory and maybe raise the compression
 * speed by choosing a lower blocksize, which in turn may cause a lower
 * compression ratio. You can avoid unnecessary memory allocation by avoiding
 * using a blocksize which is bigger than the size of the input.
 * </p>
 *
 * <p>
 * You can compute the memory usage for compressing by the following formula:
 * </p>
 *
 * <pre>
 * &lt;code&gt;400k + (9 * blocksize)&lt;/code&gt;.
 * </pre>
 *
 * <p>
 * To get the memory required for decompression by {@link CBZip2InputStream
 * CBZip2InputStream} use
 * </p>
 *
 * <pre>
 * &lt;code&gt;65k + (5 * blocksize)&lt;/code&gt;.
 * </pre>
 *
 * <table width="100%" border="1">
 * <colgroup> <col width="33%" /> <col width="33%" /> <col width="33%" />
 * </colgroup>
 * <tr>
 * <th colspan="3">Memory usage by blocksize</th>
 * </tr>
 * <tr>
 * <th align="right">Blocksize</th> <th align="right">Compression<br>
 * memory usage</th> <th align="right">Decompression<br>
 * memory usage</th>
 * </tr>
 * <tr>
 * <td align="right">100k</td>
 * <td align="right">1300k</td>
 * <td align="right">565k</td>
 * </tr>
 * <tr>
 * <td align="right">200k</td>
 * <td align="right">2200k</td>
 * <td align="right">1065k</td>
 * </tr>
 * <tr>
 * <td align="right">300k</td>
 * <td align="right">3100k</td>
 * <td align="right">1565k</td>
 * </tr>
 * <tr>
 * <td align="right">400k</td>
 * <td align="right">4000k</td>
 * <td align="right">2065k</td>
 * </tr>
 * <tr>
 * <td align="right">500k</td>
 * <td align="right">4900k</td>
 * <td align="right">2565k</td>
 * </tr>
 * <tr>
 * <td align="right">600k</td>
 * <td align="right">5800k</td>
 * <td align="right">3065k</td>
 * </tr>
 * <tr>
 * <td align="right">700k</td>
 * <td align="right">6700k</td>
 * <td align="right">3565k</td>
 * </tr>
 * <tr>
 * <td align="right">800k</td>
 * <td align="right">7600k</td>
 * <td align="right">4065k</td>
 * </tr>
 * <tr>
 * <td align="right">900k</td>
 * <td align="right">8500k</td>
 * <td align="right">4565k</td>
 * </tr>
 * </table>
 *
 * <p>
 * For decompression <tt>CBZip2InputStream</tt> allocates less memory if the
 * bzipped input is smaller than one block.
 * </p>
 *
 * <p>
 * Instances of this class are not thread safe.
 * </p>
 *
 * <p>
 * TODO: Update to BZip2 1.0.1
 * </p>
 */
// forked from Apache Hadoop
class CBZip2OutputStream
        extends OutputStream
{
    /**
     * The maximum supported block size <tt> == 9</tt>.
     */
    private static final int MAX_BLOCK_SIZE = 9;
    private static final int[] R_NUMS = {619, 720, 127, 481, 931, 816, 813, 233, 566, 247,
            985, 724, 205, 454, 863, 491, 741, 242, 949, 214, 733, 859, 335,
            708, 621, 574, 73, 654, 730, 472, 419, 436, 278, 496, 867, 210,
            399, 680, 480, 51, 878, 465, 811, 169, 869, 675, 611, 697, 867,
            561, 862, 687, 507, 283, 482, 129, 807, 591, 733, 623, 150, 238,
            59, 379, 684, 877, 625, 169, 643, 105, 170, 607, 520, 932, 727,
            476, 693, 425, 174, 647, 73, 122, 335, 530, 442, 853, 695, 249,
            445, 515, 909, 545, 703, 919, 874, 474, 882, 500, 594, 612, 641,
            801, 220, 162, 819, 984, 589, 513, 495, 799, 161, 604, 958, 533,
            221, 400, 386, 867, 600, 782, 382, 596, 414, 171, 516, 375, 682,
            485, 911, 276, 98, 553, 163, 354, 666, 933, 424, 341, 533, 870,
            227, 730, 475, 186, 263, 647, 537, 686, 600, 224, 469, 68, 770,
            919, 190, 373, 294, 822, 808, 206, 184, 943, 795, 384, 383, 461,
            404, 758, 839, 887, 715, 67, 618, 276, 204, 918, 873, 777, 604,
            560, 951, 160, 578, 722, 79, 804, 96, 409, 713, 940, 652, 934, 970,
            447, 318, 353, 859, 672, 112, 785, 645, 863, 803, 350, 139, 93,
            354, 99, 820, 908, 609, 772, 154, 274, 580, 184, 79, 626, 630, 742,
            653, 282, 762, 623, 680, 81, 927, 626, 789, 125, 411, 521, 938,
            300, 821, 78, 343, 175, 128, 250, 170, 774, 972, 275, 999, 639,
            495, 78, 352, 126, 857, 956, 358, 619, 580, 124, 737, 594, 701,
            612, 669, 112, 134, 694, 363, 992, 809, 743, 168, 974, 944, 375,
            748, 52, 600, 747, 642, 182, 862, 81, 344, 805, 988, 739, 511, 655,
            814, 334, 249, 515, 897, 955, 664, 981, 649, 113, 974, 459, 893,
            228, 433, 837, 553, 268, 926, 240, 102, 654, 459, 51, 686, 754,
            806, 760, 493, 403, 415, 394, 687, 700, 946, 670, 656, 610, 738,
            392, 760, 799, 887, 653, 978, 321, 576, 617, 626, 502, 894, 679,
            243, 440, 680, 879, 194, 572, 640, 724, 926, 56, 204, 700, 707,
            151, 457, 449, 797, 195, 791, 558, 945, 679, 297, 59, 87, 824, 713,
            663, 412, 693, 342, 606, 134, 108, 571, 364, 631, 212, 174, 643,
            304, 329, 343, 97, 430, 751, 497, 314, 983, 374, 822, 928, 140,
            206, 73, 263, 980, 736, 876, 478, 430, 305, 170, 514, 364, 692,
            829, 82, 855, 953, 676, 246, 369, 970, 294, 750, 807, 827, 150,
            790, 288, 923, 804, 378, 215, 828, 592, 281, 565, 555, 710, 82,
            896, 831, 547, 261, 524, 462, 293, 465, 502, 56, 661, 821, 976,
            991, 658, 869, 905, 758, 745, 193, 768, 550, 608, 933, 378, 286,
            215, 979, 792, 961, 61, 688, 793, 644, 986, 403, 106, 366, 905,
            644, 372, 567, 466, 434, 645, 210, 389, 550, 919, 135, 780, 773,
            635, 389, 707, 100, 626, 958, 165, 504, 920, 176, 193, 713, 857,
            265, 203, 50, 668, 108, 645, 990, 626, 197, 510, 357, 358, 850,
            858, 364, 936, 638};
    private static final int N_ITERS = 4;
    private static final int NUM_OVERSHOOT_BYTES = 20;

    /**
     * This constant is accessible by subclasses for historical purposes. If you
     * don't know what it means then you don't need it.
     */
    private static final int SET_MASK = (1 << 21);

    /**
     * This constant is accessible by subclasses for historical purposes. If you
     * don't know what it means then you don't need it.
     */
    private static final int CLEAR_MASK = (~SET_MASK);

    /**
     * This constant is accessible by subclasses for historical purposes. If you
     * don't know what it means then you don't need it.
     */
    private static final int GREATER_ICOST = 15;

    /**
     * This constant is accessible by subclasses for historical purposes. If you
     * don't know what it means then you don't need it.
     */
    private static final int LESSER_ICOST = 0;

    /**
     * This constant is accessible by subclasses for historical purposes. If you
     * don't know what it means then you don't need it.
     */
    private static final int SMALL_THRESH = 20;

    /**
     * This constant is accessible by subclasses for historical purposes. If you
     * don't know what it means then you don't need it.
     */
    private static final int DEPTH_THRESH = 10;

    /**
     * This constant is accessible by subclasses for historical purposes. If you
     * don't know what it means then you don't need it.
     */
    private static final int WORK_FACTOR = 30;

    /**
     * This constant is accessible by subclasses for historical purposes. If you
     * don't know what it means then you don't need it.
     * <p>
     * If you are ever unlucky/improbable enough to get a stack overflow whilst
     * sorting, increase the following constant and try again. In practice I
     * have never seen the stack go above 27 elems, so the following limit seems
     * very generous.
     * </p>
     */
    private static final int QSORT_STACK_SIZE = 1000;

    /**
     * Knuth's increments seem to work better than Incerpi-Sedgewick here.
     * Possibly because the number of elems to sort is usually small, typically
     * &lt;= 20.
     */
    private static final int[] INCS = {1, 4, 13, 40, 121, 364, 1093, 3280, 9841, 29524, 88573, 265720, 797161, 2391484};

    private static void hbMakeCodeLengths(final byte[] len, final int[] freq,
            final Data dat, final int alphaSize, final int maxLen)
    {
        /*
         * Nodes and heap entries run from 1. Entry 0 for both the heap and
         * nodes is a sentinel.
         */
        final int[] heap = dat.heap;
        final int[] weight = dat.weight;
        final int[] parent = dat.parent;

        for (int i = alphaSize; --i >= 0; ) {
            weight[i + 1] = (freq[i] == 0 ? 1 : freq[i]) << 8;
        }

        for (boolean tooLong = true; tooLong; ) {
            tooLong = false;

            int nNodes = alphaSize;
            int nHeap = 0;
            heap[0] = 0;
            weight[0] = 0;
            parent[0] = -2;

            for (int i = 1; i <= alphaSize; i++) {
                parent[i] = -1;
                nHeap++;
                heap[nHeap] = i;

                int zz = nHeap;
                int tmp = heap[zz];
                while (weight[tmp] < weight[heap[zz >> 1]]) {
                    heap[zz] = heap[zz >> 1];
                    zz >>= 1;
                }
                heap[zz] = tmp;
            }

            while (nHeap > 1) {
                int n1 = heap[1];
                heap[1] = heap[nHeap];
                nHeap--;

                int yy;
                int zz = 1;
                int tmp = heap[1];

                while (true) {
                    yy = zz << 1;

                    if (yy > nHeap) {
                        break;
                    }

                    if ((yy < nHeap)
                            && (weight[heap[yy + 1]] < weight[heap[yy]])) {
                        yy++;
                    }

                    if (weight[tmp] < weight[heap[yy]]) {
                        break;
                    }

                    heap[zz] = heap[yy];
                    zz = yy;
                }

                heap[zz] = tmp;

                int n2 = heap[1];
                heap[1] = heap[nHeap];
                nHeap--;

                zz = 1;
                tmp = heap[1];

                while (true) {
                    yy = zz << 1;

                    if (yy > nHeap) {
                        break;
                    }

                    if ((yy < nHeap)
                            && (weight[heap[yy + 1]] < weight[heap[yy]])) {
                        yy++;
                    }

                    if (weight[tmp] < weight[heap[yy]]) {
                        break;
                    }

                    heap[zz] = heap[yy];
                    zz = yy;
                }

                heap[zz] = tmp;
                nNodes++;
                parent[n1] = nNodes;
                parent[n2] = nNodes;

                final int weightN1 = weight[n1];
                final int weightN2 = weight[n2];
                weight[nNodes] = ((weightN1 & 0xffffff00) + (weightN2 & 0xffffff00))
                        | (1 + (Math.max((weightN1 & 0x000000ff), (weightN2 & 0x000000ff))));

                parent[nNodes] = -1;
                nHeap++;
                heap[nHeap] = nNodes;

                zz = nHeap;
                tmp = heap[zz];
                final int weightTmp = weight[tmp];
                while (weightTmp < weight[heap[zz >> 1]]) {
                    heap[zz] = heap[zz >> 1];
                    zz >>= 1;
                }
                heap[zz] = tmp;
            }

            for (int i = 1; i <= alphaSize; i++) {
                int j = 0;
                int k = i;

                for (int parentK; (parentK = parent[k]) >= 0; ) {
                    k = parentK;
                    j++;
                }

                len[i - 1] = (byte) j;
                if (j > maxLen) {
                    tooLong = true;
                }
            }

            if (tooLong) {
                for (int i = 1; i < alphaSize; i++) {
                    int j = weight[i] >> 8;
                    j = 1 + (j >> 1);
                    weight[i] = j << 8;
                }
            }
        }
    }

    /**
     * Index of the last char in the block, so the block size == last + 1.
     */
    private int last;

    /**
     * Index in fmap[] of original string after sorting.
     */
    private int origPtr;

    /**
     * Always: in the range 0 .. 9. The current block size is 100000 * this
     * number.
     */
    private final int blockSize100k;

    private boolean blockRandomised;

    private int bsBuff;
    private int bsLive;
    private final Crc32 crc32 = new Crc32();

    private int nInUse;

    private int nMTF;

    /*
     * Used when sorting. If too many long comparisons happen, we stop sorting,
     * randomise the block slightly, and try again.
     */
    private int workDone;
    private int workLimit;
    private boolean firstAttempt;

    private int currentChar = -1;
    private int runLength;

    private int combinedCRC;
    private int allowableBlockSize;

    /**
     * All memory intensive stuff.
     */
    private Data data;

    private OutputStream out;

    /**
     * Constructs a new <tt>CBZip2OutputStream</tt> with a block size of 900k.
     *
     * <p>
     * <b>Attention: </b>The caller is responsible to write the two BZip2 magic
     * bytes <tt>"BZ"</tt> to the specified stream prior to calling this
     * constructor.
     * </p>
     *
     * @param out *
     * the destination stream.
     * @throws IOException if an I/O error occurs in the specified stream.
     * @throws NullPointerException if <code>out == null</code>.
     */
    public CBZip2OutputStream(final OutputStream out)
            throws IOException
    {
        this(out, MAX_BLOCK_SIZE);
    }

    /**
     * Constructs a new <tt>CBZip2OutputStream</tt> with specified block size.
     *
     * <p>
     * <b>Attention: </b>The caller is responsible to write the two BZip2 magic
     * bytes <tt>"BZ"</tt> to the specified stream prior to calling this
     * constructor.
     * </p>
     *
     * @param out the destination stream.
     * @param blockSize the blockSize as 100k units.
     * @throws IOException if an I/O error occurs in the specified stream.
     * @throws IllegalArgumentException if <code>(blockSize < 1) || (blockSize > 9)</code>.
     * @throws NullPointerException if <code>out == null</code>.
     * @see #MAX_BLOCK_SIZE
     */
    private CBZip2OutputStream(final OutputStream out, final int blockSize)
            throws IOException
    {
        if (blockSize < 1) {
            throw new IllegalArgumentException("blockSize(" + blockSize
                    + ") < 1");
        }
        if (blockSize > 9) {
            throw new IllegalArgumentException("blockSize(" + blockSize
                    + ") > 9");
        }

        this.blockSize100k = blockSize;
        this.out = out;
        init();
    }

    @Override
    public void write(final int b)
            throws IOException
    {
        if (this.out != null) {
            write0(b);
        }
        else {
            throw new IOException("closed");
        }
    }

    private void writeRun()
            throws IOException
    {
        final int lastShadow = this.last;

        if (lastShadow < this.allowableBlockSize) {
            final int currentCharShadow = this.currentChar;
            final Data dataShadow = this.data;
            dataShadow.inUse[currentCharShadow] = true;
            final byte ch = (byte) currentCharShadow;

            int runLengthShadow = this.runLength;
            this.crc32.updateCRC(currentCharShadow, runLengthShadow);

            switch (runLengthShadow) {
                case 1:
                    dataShadow.block[lastShadow + 2] = ch;
                    this.last = lastShadow + 1;
                    break;

                case 2:
                    dataShadow.block[lastShadow + 2] = ch;
                    dataShadow.block[lastShadow + 3] = ch;
                    this.last = lastShadow + 2;
                    break;

                case 3: {
                    final byte[] block = dataShadow.block;
                    block[lastShadow + 2] = ch;
                    block[lastShadow + 3] = ch;
                    block[lastShadow + 4] = ch;
                    this.last = lastShadow + 3;
                }
                break;

                default: {
                    runLengthShadow -= 4;
                    dataShadow.inUse[runLengthShadow] = true;
                    final byte[] block = dataShadow.block;
                    block[lastShadow + 2] = ch;
                    block[lastShadow + 3] = ch;
                    block[lastShadow + 4] = ch;
                    block[lastShadow + 5] = ch;
                    block[lastShadow + 6] = (byte) runLengthShadow;
                    this.last = lastShadow + 5;
                }
                break;
            }
        }
        else {
            endBlock();
            initBlock();
            writeRun();
        }
    }

    /**
     * Overridden to close the stream.
     */
    @Override
    protected void finalize()
            throws Throwable
    {
        finish();
        super.finalize();
    }

    public void finish()
            throws IOException
    {
        if (out != null) {
            try {
                if (this.runLength > 0) {
                    writeRun();
                }
                this.currentChar = -1;
                endBlock();
                endCompression();
            }
            finally {
                this.out = null;
                this.data = null;
            }
        }
    }

    @Override
    public void close()
            throws IOException
    {
        if (out != null) {
            OutputStream outShadow = this.out;
            try {
                finish();
                outShadow.close();
                outShadow = null;
            }
            finally {
                IOUtils.closeStream(outShadow);
            }
        }
    }

    @Override
    public void flush()
            throws IOException
    {
        OutputStream outShadow = this.out;
        if (outShadow != null) {
            outShadow.flush();
        }
    }

    private void init()
            throws IOException
    {
        // write magic: done by caller who created this stream
        // this.out.write('B');
        // this.out.write('Z');

        this.data = new Data(this.blockSize100k);

        /*
         * Write `magic' bytes h indicating file-format == huffmanised, followed
         * by a digit indicating blockSize100k.
         */
        bsPutUByte('h');
        bsPutUByte((int) '0' + this.blockSize100k);

        this.combinedCRC = 0;
        initBlock();
    }

    private void initBlock()
    {
        // blockNo++;
        this.crc32.initialiseCRC();
        this.last = -1;
        // ch = 0;

        boolean[] inUse = this.data.inUse;
        for (int i = 256; --i >= 0; ) {
            inUse[i] = false;
        }

        /* 20 is just a paranoia constant */
        this.allowableBlockSize = (this.blockSize100k * BZip2Constants.BASE_BLOCK_SIZE) - 20;
    }

    private void endBlock()
            throws IOException
    {
        int blockCRC = this.crc32.getFinalCRC();
        this.combinedCRC = (this.combinedCRC << 1) | (this.combinedCRC >>> 31);
        this.combinedCRC ^= blockCRC;

        // empty block at end of file
        if (this.last == -1) {
            return;
        }

        /* sort the block and establish posn of original string */
        blockSort();

        /*
         * A 6-byte block header, the value chosen arbitrarily as 0x314159265359
         * :-). A 32 bit value does not really give a strong enough guarantee
         * that the value will not appear by chance in the compressed
         * data stream. Worst-case probability of this event, for a 900k block,
         * is about 2.0e-3 for 32 bits, 1.0e-5 for 40 bits and 4.0e-8 for 48
         * bits. For a compressed file of size 100Gb -- about 100000 blocks --
         * only a 48-bit marker will do. NB: normal compression/ decompression
         * do not rely on these statistical properties. They are only important
         * when trying to recover blocks from damaged files.
         */
        bsPutUByte(0x31);
        bsPutUByte(0x41);
        bsPutUByte(0x59);
        bsPutUByte(0x26);
        bsPutUByte(0x53);
        bsPutUByte(0x59);

        /* Now the block's CRC, so it is in a known place. */
        bsPutInt(blockCRC);

        /* Now a single bit indicating randomisation. */
        if (this.blockRandomised) {
            bsW(1, 1);
        }
        else {
            bsW(1, 0);
        }

        /* Finally, block's contents proper. */
        moveToFrontCodeAndSend();
    }

    private void endCompression()
            throws IOException
    {
        /*
         * Now another magic 48-bit number, 0x177245385090, to indicate the end
         * of the last block. (sqrt(pi), if you want to know. I did want to use
         * e, but it contains too much repetition -- 27 18 28 18 28 46 -- for me
         * to feel statistically comfortable. Call me paranoid.)
         */
        bsPutUByte(0x17);
        bsPutUByte(0x72);
        bsPutUByte(0x45);
        bsPutUByte(0x38);
        bsPutUByte(0x50);
        bsPutUByte(0x90);

        bsPutInt(this.combinedCRC);
        bsFinishedWithStream();
    }

    @Override
    public void write(final byte[] buf, int offs, final int len)
            throws IOException
    {
        if (offs < 0) {
            throw new IndexOutOfBoundsException("offs(" + offs + ") < 0.");
        }
        if (len < 0) {
            throw new IndexOutOfBoundsException("len(" + len + ") < 0.");
        }
        if (offs + len > buf.length) {
            throw new IndexOutOfBoundsException("offs(" + offs + ") + len("
                    + len + ") > buf.length(" + buf.length + ").");
        }
        if (this.out == null) {
            throw new IOException("stream closed");
        }

        for (int hi = offs + len; offs < hi; ) {
            write0(buf[offs++]);
        }
    }

    private void write0(int b)
            throws IOException
    {
        if (this.currentChar != -1) {
            b &= 0xff;
            if (this.currentChar == b) {
                if (++this.runLength > 254) {
                    writeRun();
                    this.currentChar = -1;
                    this.runLength = 0;
                }
                // else nothing to do
            }
            else {
                writeRun();
                this.runLength = 1;
                this.currentChar = b;
            }
        }
        else {
            this.currentChar = b & 0xff;
            this.runLength++;
        }
    }

    private static void hbAssignCodes(final int[] code, final byte[] length,
            final int minLen, final int maxLen, final int alphaSize)
    {
        int vec = 0;
        for (int n = minLen; n <= maxLen; n++) {
            for (int i = 0; i < alphaSize; i++) {
                if ((length[i] & 0xff) == n) {
                    code[i] = vec;
                    vec++;
                }
            }
            vec <<= 1;
        }
    }

    private void bsFinishedWithStream()
            throws IOException
    {
        while (this.bsLive > 0) {
            int ch = this.bsBuff >> 24;
            this.out.write(ch); // write 8-bit
            this.bsBuff <<= 8;
            this.bsLive -= 8;
        }
    }

    private void bsW(final int n, final int v)
            throws IOException
    {
        final OutputStream outShadow = this.out;
        int bsLiveShadow = this.bsLive;
        int bsBuffShadow = this.bsBuff;

        while (bsLiveShadow >= 8) {
            outShadow.write(bsBuffShadow >> 24); // write 8-bit
            bsBuffShadow <<= 8;
            bsLiveShadow -= 8;
        }

        this.bsBuff = bsBuffShadow | (v << (32 - bsLiveShadow - n));
        this.bsLive = bsLiveShadow + n;
    }

    private void bsPutUByte(final int c)
            throws IOException
    {
        bsW(8, c);
    }

    private void bsPutInt(final int u)
            throws IOException
    {
        bsW(8, (u >> 24) & 0xff);
        bsW(8, (u >> 16) & 0xff);
        bsW(8, (u >> 8) & 0xff);
        bsW(8, u & 0xff);
    }

    private void sendMTFValues()
            throws IOException
    {
        final byte[][] len = this.data.sendMTFValuesLen;
        final int alphaSize = this.nInUse + 2;

        for (int t = N_GROUPS; --t >= 0; ) {
            byte[] lenT = len[t];
            for (int v = alphaSize; --v >= 0; ) {
                lenT[v] = GREATER_ICOST;
            }
        }

        /* Decide how many coding tables to use */
        // assert (this.nMTF > 0) : this.nMTF;
        final int nGroups = (this.nMTF < 200) ? 2 : (this.nMTF < 600) ? 3
                : (this.nMTF < 1200) ? 4 : (this.nMTF < 2400) ? 5 : 6;

        /* Generate an initial set of coding tables */
        sendMTFValues0(nGroups, alphaSize);

        /*
         * Iterate up to N_ITERS times to improve the tables.
         */
        final int nSelectors = sendMTFValues1(nGroups, alphaSize);

        /* Compute MTF values for the selectors. */
        sendMTFValues2(nGroups, nSelectors);

        /* Assign actual codes for the tables. */
        sendMTFValues3(nGroups, alphaSize);

        /* Transmit the mapping table. */
        sendMTFValues4();

        /* Now the selectors. */
        sendMTFValues5(nGroups, nSelectors);

        /* Now the coding tables. */
        sendMTFValues6(nGroups, alphaSize);

        /* And finally, the block data proper */
        sendMTFValues7();
    }

    private void sendMTFValues0(final int nGroups, final int alphaSize)
    {
        final byte[][] len = this.data.sendMTFValuesLen;
        final int[] mtfFreq = this.data.mtfFreq;

        int remF = this.nMTF;
        int gs = 0;

        for (int nPart = nGroups; nPart > 0; nPart--) {
            final int tFreq = remF / nPart;
            int ge = gs - 1;
            int aFreq = 0;

            for (final int a = alphaSize - 1; (aFreq < tFreq) && (ge < a); ) {
                aFreq += mtfFreq[++ge];
            }

            if ((ge > gs) && (nPart != nGroups) && (nPart != 1)
                    && (((nGroups - nPart) & 1) != 0)) {
                aFreq -= mtfFreq[ge--];
            }

            final byte[] lenNp = len[nPart - 1];
            for (int v = alphaSize; --v >= 0; ) {
                if ((v >= gs) && (v <= ge)) {
                    lenNp[v] = LESSER_ICOST;
                }
                else {
                    lenNp[v] = GREATER_ICOST;
                }
            }

            gs = ge + 1;
            remF -= aFreq;
        }
    }

    private int sendMTFValues1(final int nGroups, final int alphaSize)
    {
        final Data dataShadow = this.data;
        final int[][] rfreq = dataShadow.sendMTFValuesRfreq;
        final int[] fave = dataShadow.sendMTFValuesFave;
        final short[] cost = dataShadow.sendMTFValuesCost;
        final char[] sfmap = dataShadow.sfmap;
        final byte[] selector = dataShadow.selector;
        final byte[][] len = dataShadow.sendMTFValuesLen;
        final byte[] len0 = len[0];
        final byte[] len1 = len[1];
        final byte[] len2 = len[2];
        final byte[] len3 = len[3];
        final byte[] len4 = len[4];
        final byte[] len5 = len[5];
        final int nMTFShadow = this.nMTF;

        int nSelectors = 0;

        for (int iter = 0; iter < N_ITERS; iter++) {
            for (int t = nGroups; --t >= 0; ) {
                fave[t] = 0;
                int[] rfreqt = rfreq[t];
                for (int i = alphaSize; --i >= 0; ) {
                    rfreqt[i] = 0;
                }
            }

            nSelectors = 0;

            for (int gs = 0; gs < this.nMTF; ) {
                /* Set group start & end marks. */

                /*
                 * Calculate the cost of this group as coded by each of the
                 * coding tables.
                 */

                final int ge = Math.min(gs + G_SIZE - 1, nMTFShadow - 1);

                if (nGroups == N_GROUPS) {
                    // unrolled version of the else-block

                    short cost0 = 0;
                    short cost1 = 0;
                    short cost2 = 0;
                    short cost3 = 0;
                    short cost4 = 0;
                    short cost5 = 0;

                    for (int i = gs; i <= ge; i++) {
                        final int icv = sfmap[i];
                        cost0 += len0[icv] & 0xff;
                        cost1 += len1[icv] & 0xff;
                        cost2 += len2[icv] & 0xff;
                        cost3 += len3[icv] & 0xff;
                        cost4 += len4[icv] & 0xff;
                        cost5 += len5[icv] & 0xff;
                    }

                    cost[0] = cost0;
                    cost[1] = cost1;
                    cost[2] = cost2;
                    cost[3] = cost3;
                    cost[4] = cost4;
                    cost[5] = cost5;
                }
                else {
                    for (int t = nGroups; --t >= 0; ) {
                        cost[t] = 0;
                    }

                    for (int i = gs; i <= ge; i++) {
                        final int icv = sfmap[i];
                        for (int t = nGroups; --t >= 0; ) {
                            cost[t] += len[t][icv] & 0xff;
                        }
                    }
                }

                /*
                 * Find the coding table which is best for this group, and
                 * record its identity in the selector table.
                 */
                int bt = -1;
                for (int t = nGroups, bc = 999999999; --t >= 0; ) {
                    final int costT = cost[t];
                    if (costT < bc) {
                        bc = costT;
                        bt = t;
                    }
                }

                fave[bt]++;
                selector[nSelectors] = (byte) bt;
                nSelectors++;

                /*
                 * Increment the symbol frequencies for the selected table.
                 */
                final int[] rfreqBt = rfreq[bt];
                for (int i = gs; i <= ge; i++) {
                    rfreqBt[sfmap[i]]++;
                }

                gs = ge + 1;
            }

            /*
             * Recompute the tables based on the accumulated frequencies.
             */
            for (int t = 0; t < nGroups; t++) {
                hbMakeCodeLengths(len[t], rfreq[t], this.data, alphaSize, 20);
            }
        }

        return nSelectors;
    }

    private void sendMTFValues2(final int nGroups, final int nSelectors)
    {
        // assert (nGroups < 8) : nGroups;

        final Data dataShadow = this.data;
        byte[] pos = dataShadow.sendMTFValues2Pos;

        for (int i = nGroups; --i >= 0; ) {
            pos[i] = (byte) i;
        }

        for (int i = 0; i < nSelectors; i++) {
            final byte llI = dataShadow.selector[i];
            byte tmp = pos[0];
            int j = 0;

            while (llI != tmp) {
                j++;
                byte tmp2 = tmp;
                tmp = pos[j];
                pos[j] = tmp2;
            }

            pos[0] = tmp;
            dataShadow.selectorMtf[i] = (byte) j;
        }
    }

    private void sendMTFValues3(final int nGroups, final int alphaSize)
    {
        int[][] code = this.data.sendMTFValuesCode;
        byte[][] len = this.data.sendMTFValuesLen;

        for (int t = 0; t < nGroups; t++) {
            int minLen = 32;
            int maxLen = 0;
            final byte[] lenT = len[t];
            for (int i = alphaSize; --i >= 0; ) {
                final int l = lenT[i] & 0xff;
                if (l > maxLen) {
                    maxLen = l;
                }
                if (l < minLen) {
                    minLen = l;
                }
            }

            // assert (maxLen <= 20) : maxLen;
            // assert (minLen >= 1) : minLen;

            hbAssignCodes(code[t], len[t], minLen, maxLen, alphaSize);
        }
    }

    private void sendMTFValues4()
            throws IOException
    {
        final boolean[] inUse = this.data.inUse;
        final boolean[] inUse16 = this.data.sentMTFValues4InUse16;

        for (int i = 16; --i >= 0; ) {
            inUse16[i] = false;
            final int i16 = i * 16;
            for (int j = 16; --j >= 0; ) {
                if (inUse[i16 + j]) {
                    inUse16[i] = true;
                    break;
                }
            }
        }

        for (int i = 0; i < 16; i++) {
            bsW(1, inUse16[i] ? 1 : 0);
        }

        final OutputStream outShadow = this.out;
        int bsLiveShadow = this.bsLive;
        int bsBuffShadow = this.bsBuff;

        for (int i = 0; i < 16; i++) {
            if (inUse16[i]) {
                final int i16 = i * 16;
                for (int j = 0; j < 16; j++) {
                    // inlined: bsW(1, inUse[i16 + j] ? 1 : 0);
                    while (bsLiveShadow >= 8) {
                        outShadow.write(bsBuffShadow >> 24); // write 8-bit
                        bsBuffShadow <<= 8;
                        bsLiveShadow -= 8;
                    }
                    if (inUse[i16 + j]) {
                        bsBuffShadow |= 1 << (32 - bsLiveShadow - 1);
                    }
                    bsLiveShadow++;
                }
            }
        }

        this.bsBuff = bsBuffShadow;
        this.bsLive = bsLiveShadow;
    }

    private void sendMTFValues5(final int nGroups, final int nSelectors)
            throws IOException
    {
        bsW(3, nGroups);
        bsW(15, nSelectors);

        final OutputStream outShadow = this.out;
        final byte[] selectorMtf = this.data.selectorMtf;

        int bsLiveShadow = this.bsLive;
        int bsBuffShadow = this.bsBuff;

        for (int i = 0; i < nSelectors; i++) {
            for (int j = 0, hj = selectorMtf[i] & 0xff; j < hj; j++) {
                // inlined: bsW(1, 1);
                while (bsLiveShadow >= 8) {
                    outShadow.write(bsBuffShadow >> 24);
                    bsBuffShadow <<= 8;
                    bsLiveShadow -= 8;
                }
                bsBuffShadow |= 1 << (32 - bsLiveShadow - 1);
                bsLiveShadow++;
            }

            // inlined: bsW(1, 0);
            while (bsLiveShadow >= 8) {
                outShadow.write(bsBuffShadow >> 24);
                bsBuffShadow <<= 8;
                bsLiveShadow -= 8;
            }
            // bsBuffShadow |= 0 << (32 - bsLiveShadow - 1);
            bsLiveShadow++;
        }

        this.bsBuff = bsBuffShadow;
        this.bsLive = bsLiveShadow;
    }

    private void sendMTFValues6(final int nGroups, final int alphaSize)
            throws IOException
    {
        final byte[][] len = this.data.sendMTFValuesLen;
        final OutputStream outShadow = this.out;

        int bsLiveShadow = this.bsLive;
        int bsBuffShadow = this.bsBuff;

        for (int t = 0; t < nGroups; t++) {
            byte[] lenT = len[t];
            int curr = lenT[0] & 0xff;

            // inlined: bsW(5, curr);
            while (bsLiveShadow >= 8) {
                outShadow.write(bsBuffShadow >> 24); // write 8-bit
                bsBuffShadow <<= 8;
                bsLiveShadow -= 8;
            }
            bsBuffShadow |= curr << (32 - bsLiveShadow - 5);
            bsLiveShadow += 5;

            for (int i = 0; i < alphaSize; i++) {
                int lti = lenT[i] & 0xff;
                while (curr < lti) {
                    // inlined: bsW(2, 2);
                    while (bsLiveShadow >= 8) {
                        outShadow.write(bsBuffShadow >> 24); // write 8-bit
                        bsBuffShadow <<= 8;
                        bsLiveShadow -= 8;
                    }
                    bsBuffShadow |= 2 << (32 - bsLiveShadow - 2);
                    bsLiveShadow += 2;

                    curr++; /* 10 */
                }

                while (curr > lti) {
                    // inlined: bsW(2, 3);
                    while (bsLiveShadow >= 8) {
                        outShadow.write(bsBuffShadow >> 24); // write 8-bit
                        bsBuffShadow <<= 8;
                        bsLiveShadow -= 8;
                    }
                    bsBuffShadow |= 3 << (32 - bsLiveShadow - 2);
                    bsLiveShadow += 2;

                    curr--; /* 11 */
                }

                // inlined: bsW(1, 0);
                while (bsLiveShadow >= 8) {
                    outShadow.write(bsBuffShadow >> 24); // write 8-bit
                    bsBuffShadow <<= 8;
                    bsLiveShadow -= 8;
                }
                // bsBuffShadow |= 0 << (32 - bsLiveShadow - 1);
                bsLiveShadow++;
            }
        }

        this.bsBuff = bsBuffShadow;
        this.bsLive = bsLiveShadow;
    }

    private void sendMTFValues7()
            throws IOException
    {
        final Data dataShadow = this.data;
        final byte[][] len = dataShadow.sendMTFValuesLen;
        final int[][] code = dataShadow.sendMTFValuesCode;
        final OutputStream outShadow = this.out;
        final byte[] selector = dataShadow.selector;
        final char[] sfmap = dataShadow.sfmap;
        final int nMTFShadow = this.nMTF;

        int selCtr = 0;

        int bsLiveShadow = this.bsLive;
        int bsBuffShadow = this.bsBuff;

        for (int gs = 0; gs < nMTFShadow; ) {
            final int ge = Math.min(gs + G_SIZE - 1, nMTFShadow - 1);
            final int selectorSelCtr = selector[selCtr] & 0xff;
            final int[] codeSelCtr = code[selectorSelCtr];
            final byte[] lenSelCtr = len[selectorSelCtr];

            while (gs <= ge) {
                final int sfmapI = sfmap[gs];

                // inlined: bsW(lenSelCtr[sfmapI] & 0xff,
                // codeSelCtr[sfmapI]);
                while (bsLiveShadow >= 8) {
                    outShadow.write(bsBuffShadow >> 24);
                    bsBuffShadow <<= 8;
                    bsLiveShadow -= 8;
                }
                final int n = lenSelCtr[sfmapI] & 0xFF;
                bsBuffShadow |= codeSelCtr[sfmapI] << (32 - bsLiveShadow - n);
                bsLiveShadow += n;

                gs++;
            }

            gs = ge + 1;
            selCtr++;
        }

        this.bsBuff = bsBuffShadow;
        this.bsLive = bsLiveShadow;
    }

    private void moveToFrontCodeAndSend()
            throws IOException
    {
        bsW(24, this.origPtr);
        generateMTFValues();
        sendMTFValues();
    }

    /**
     * This is the most hammered method of this class.
     *
     * <p>
     * This is the version using unrolled loops. Normally I never use such ones
     * in Java code. The unrolling has shown a noticeable performance improvement
     * on JRE 1.4.2 (Linux i586 / HotSpot Client). Of course it depends on the
     * JIT compiler of the vm.
     * </p>
     */
    @SuppressWarnings("checkstyle:InnerAssignment")
    private boolean mainSimpleSort(final Data dataShadow, final int lo,
            final int hi, final int d)
    {
        final int bigN = hi - lo + 1;
        if (bigN < 2) {
            return this.firstAttempt && (this.workDone > this.workLimit);
        }

        int hp = 0;
        while (INCS[hp] < bigN) {
            hp++;
        }

        final int[] fmap = dataShadow.fmap;
        final char[] quadrant = dataShadow.quadrant;
        final byte[] block = dataShadow.block;
        final int lastShadow = this.last;
        final int lastPlus1 = lastShadow + 1;
        final boolean firstAttemptShadow = this.firstAttempt;
        final int workLimitShadow = this.workLimit;
        int workDoneShadow = this.workDone;

        // Following block contains unrolled code which could be shortened by
        // coding it in additional loops.

        HP:
        while (--hp >= 0) {
            final int h = INCS[hp];
            final int mj = lo + h - 1;

            for (int i = lo + h; i <= hi; ) {
                // copy
                for (int k = 3; (i <= hi) && (--k >= 0); i++) {
                    final int v = fmap[i];
                    final int vd = v + d;
                    int j = i;

                    // for (int a;
                    // (j > mj) && mainGtU((a = fmap[j - h]) + d, vd,
                    // block, quadrant, lastShadow);
                    // j -= h) {
                    // fmap[j] = a;
                    // }
                    //
                    // unrolled version:

                    // start inline mainGTU
                    boolean onceRunned = false;
                    int a = 0;

                    HAMMER:
                    while (true) {
                        if (onceRunned) {
                            fmap[j] = a;
                            if ((j -= h) <= mj) {
                                break;
                            }
                        }
                        else {
                            onceRunned = true;
                        }

                        a = fmap[j - h];
                        int i1 = a + d;
                        int i2 = vd;

                        // following could be done in a loop, but
                        // unrolled it for performance:
                        if (block[i1 + 1] == block[i2 + 1]) {
                            if (block[i1 + 2] == block[i2 + 2]) {
                                if (block[i1 + 3] == block[i2 + 3]) {
                                    if (block[i1 + 4] == block[i2 + 4]) {
                                        if (block[i1 + 5] == block[i2 + 5]) {
                                            if (block[(i1 += 6)] == block[(i2 += 6)]) {
                                                int x = lastShadow;
                                                while (x > 0) {
                                                    x -= 4;

                                                    if (block[i1 + 1] == block[i2 + 1]) {
                                                        if (quadrant[i1] == quadrant[i2]) {
                                                            if (block[i1 + 2] == block[i2 + 2]) {
                                                                if (quadrant[i1 + 1] == quadrant[i2 + 1]) {
                                                                    if (block[i1 + 3] == block[i2 + 3]) {
                                                                        if (quadrant[i1 + 2] == quadrant[i2 + 2]) {
                                                                            if (block[i1 + 4] == block[i2 + 4]) {
                                                                                if (quadrant[i1 + 3] == quadrant[i2 + 3]) {
                                                                                    if ((i1 += 4) >= lastPlus1) {
                                                                                        i1 -= lastPlus1;
                                                                                    }
                                                                                    if ((i2 += 4) >= lastPlus1) {
                                                                                        i2 -= lastPlus1;
                                                                                    }
                                                                                    workDoneShadow++;
                                                                                }
                                                                                else if ((quadrant[i1 + 3] > quadrant[i2 + 3])) {
                                                                                    continue HAMMER;
                                                                                }
                                                                                else {
                                                                                    break HAMMER;
                                                                                }
                                                                            }
                                                                            else if ((block[i1 + 4] & 0xff) > (block[i2 + 4] & 0xff)) {
                                                                                continue HAMMER;
                                                                            }
                                                                            else {
                                                                                break HAMMER;
                                                                            }
                                                                        }
                                                                        else if ((quadrant[i1 + 2] > quadrant[i2 + 2])) {
                                                                            continue HAMMER;
                                                                        }
                                                                        else {
                                                                            break HAMMER;
                                                                        }
                                                                    }
                                                                    else if ((block[i1 + 3] & 0xff) > (block[i2 + 3] & 0xff)) {
                                                                        continue HAMMER;
                                                                    }
                                                                    else {
                                                                        break HAMMER;
                                                                    }
                                                                }
                                                                else if ((quadrant[i1 + 1] > quadrant[i2 + 1])) {
                                                                    continue HAMMER;
                                                                }
                                                                else {
                                                                    break HAMMER;
                                                                }
                                                            }
                                                            else if ((block[i1 + 2] & 0xff) > (block[i2 + 2] & 0xff)) {
                                                                continue HAMMER;
                                                            }
                                                            else {
                                                                break HAMMER;
                                                            }
                                                        }
                                                        else if ((quadrant[i1] > quadrant[i2])) {
                                                            continue HAMMER;
                                                        }
                                                        else {
                                                            break HAMMER;
                                                        }
                                                    }
                                                    else if ((block[i1 + 1] & 0xff) > (block[i2 + 1] & 0xff)) {
                                                        continue HAMMER;
                                                    }
                                                    else {
                                                        break HAMMER;
                                                    }
                                                }
                                                break;
                                            } // while x > 0
                                            else {
                                                if ((block[i1] & 0xff) <= (block[i2] & 0xff)) {
                                                    break;
                                                }
                                            }
                                        }
                                        else if ((block[i1 + 5] & 0xff) > (block[i2 + 5] & 0xff)) {
                                            // ignored
                                        }
                                        else {
                                            break;
                                        }
                                    }
                                    else if ((block[i1 + 4] & 0xff) > (block[i2 + 4] & 0xff)) {
                                        // ignored
                                    }
                                    else {
                                        break;
                                    }
                                }
                                else if ((block[i1 + 3] & 0xff) > (block[i2 + 3] & 0xff)) {
                                    // ignored
                                }
                                else {
                                    break;
                                }
                            }
                            else if ((block[i1 + 2] & 0xff) > (block[i2 + 2] & 0xff)) {
                                // ignored
                            }
                            else {
                                break;
                            }
                        }
                        else if ((block[i1 + 1] & 0xff) > (block[i2 + 1] & 0xff)) {
                            // ignored
                        }
                        else {
                            break;
                        }
                    }

                    fmap[j] = v;
                }

                if (firstAttemptShadow && (i <= hi)
                        && (workDoneShadow > workLimitShadow)) {
                    break HP;
                }
            }
        }

        this.workDone = workDoneShadow;
        return firstAttemptShadow && (workDoneShadow > workLimitShadow);
    }

    private static void vswap(int[] fmap, int p1, int p2, int n)
    {
        n += p1;
        while (p1 < n) {
            int t = fmap[p1];
            fmap[p1++] = fmap[p2];
            fmap[p2++] = t;
        }
    }

    private static byte med3(byte a, byte b, byte c)
    {
        return (a < b) ? (b < c ? b : a < c ? c : a) : (b > c ? b : a > c ? c : a);
    }

    private void blockSort()
    {
        this.workLimit = WORK_FACTOR * this.last;
        this.workDone = 0;
        this.blockRandomised = false;
        this.firstAttempt = true;
        mainSort();

        if (this.firstAttempt && (this.workDone > this.workLimit)) {
            randomiseBlock();
            this.workLimit = 0;
            this.workDone = 0;
            this.firstAttempt = false;
            mainSort();
        }

        int[] fmap = this.data.fmap;
        this.origPtr = -1;
        for (int i = 0, lastShadow = this.last; i <= lastShadow; i++) {
            if (fmap[i] == 0) {
                this.origPtr = i;
                break;
            }
        }
    }

    /**
     * Method "mainQSort3", file "blocksort.c", BZip2 1.0.2
     */
    private void mainQSort3(final Data dataShadow, final int loSt, final int hiSt, final int dSt)
    {
        final int[] stackLl = dataShadow.stackLl;
        final int[] stackHh = dataShadow.stackHh;
        final int[] stackDd = dataShadow.stackDd;
        final int[] fmap = dataShadow.fmap;
        final byte[] block = dataShadow.block;

        stackLl[0] = loSt;
        stackHh[0] = hiSt;
        stackDd[0] = dSt;

        for (int sp = 1; --sp >= 0; ) {
            final int lo = stackLl[sp];
            final int hi = stackHh[sp];
            final int d = stackDd[sp];

            if ((hi - lo < SMALL_THRESH) || (d > DEPTH_THRESH)) {
                if (mainSimpleSort(dataShadow, lo, hi, d)) {
                    return;
                }
            }
            else {
                final int d1 = d + 1;
                final int med = med3(block[fmap[lo] + d1], block[fmap[hi] + d1], block[fmap[(lo + hi) >>> 1] + d1]) & 0xff;

                int unLo = lo;
                int unHi = hi;
                int ltLo = lo;
                int gtHi = hi;

                while (true) {
                    while (unLo <= unHi) {
                        final int n = ((int) block[fmap[unLo] + d1] & 0xff) - med;
                        if (n == 0) {
                            final int temp = fmap[unLo];
                            fmap[unLo++] = fmap[ltLo];
                            fmap[ltLo++] = temp;
                        }
                        else if (n < 0) {
                            unLo++;
                        }
                        else {
                            break;
                        }
                    }

                    while (unLo <= unHi) {
                        final int n = ((int) block[fmap[unHi] + d1] & 0xff) - med;
                        if (n == 0) {
                            final int temp = fmap[unHi];
                            fmap[unHi--] = fmap[gtHi];
                            fmap[gtHi--] = temp;
                        }
                        else if (n > 0) {
                            unHi--;
                        }
                        else {
                            break;
                        }
                    }

                    if (unLo <= unHi) {
                        final int temp = fmap[unLo];
                        fmap[unLo++] = fmap[unHi];
                        fmap[unHi--] = temp;
                    }
                    else {
                        break;
                    }
                }

                if (gtHi < ltLo) {
                    stackLl[sp] = lo;
                    stackHh[sp] = hi;
                    stackDd[sp] = d1;
                    sp++;
                }
                else {
                    int n = Math.min((ltLo - lo), (unLo - ltLo));
                    vswap(fmap, lo, unLo - n, n);
                    int m = Math.min((hi - gtHi), (gtHi - unHi));
                    vswap(fmap, unLo, hi - m + 1, m);

                    n = lo + unLo - ltLo - 1;
                    m = hi - (gtHi - unHi) + 1;

                    stackLl[sp] = lo;
                    stackHh[sp] = n;
                    stackDd[sp] = d;
                    sp++;

                    stackLl[sp] = n + 1;
                    stackHh[sp] = m - 1;
                    stackDd[sp] = d1;
                    sp++;

                    stackLl[sp] = m;
                    stackHh[sp] = hi;
                    stackDd[sp] = d;
                    sp++;
                }
            }
        }
    }

    private void mainSort()
    {
        final Data dataShadow = this.data;
        final int[] runningOrder = dataShadow.mainSortRunningOrder;
        final int[] copy = dataShadow.mainSortCopy;
        final boolean[] bigDone = dataShadow.mainSortBigDone;
        final int[] ftab = dataShadow.ftab;
        final byte[] block = dataShadow.block;
        final int[] fmap = dataShadow.fmap;
        final char[] quadrant = dataShadow.quadrant;
        final int lastShadow = this.last;
        final int workLimitShadow = this.workLimit;
        final boolean firstAttemptShadow = this.firstAttempt;

        // Set up the 2-byte frequency table
        for (int i = 65537; --i >= 0; ) {
            ftab[i] = 0;
        }

        /*
         * In the various block-sized structures, live data runs from 0 to
         * last+NUM_OVERSHOOT_BYTES inclusive. First, set up the overshoot area
         * for block.
         */
        for (int i = 0; i < NUM_OVERSHOOT_BYTES; i++) {
            block[lastShadow + i + 2] = block[(i % (lastShadow + 1)) + 1];
        }
        for (int i = lastShadow + NUM_OVERSHOOT_BYTES + 1; --i >= 0; ) {
            quadrant[i] = 0;
        }
        block[0] = block[lastShadow + 1];

        // Complete the initial radix sort:

        int c1 = block[0] & 0xff;
        for (int i = 0; i <= lastShadow; i++) {
            final int c2 = block[i + 1] & 0xff;
            ftab[(c1 << 8) + c2]++;
            c1 = c2;
        }

        for (int i = 1; i <= 65536; i++) {
            ftab[i] += ftab[i - 1];
        }

        c1 = block[1] & 0xff;
        for (int i = 0; i < lastShadow; i++) {
            final int c2 = block[i + 2] & 0xff;
            fmap[--ftab[(c1 << 8) + c2]] = i;
            c1 = c2;
        }

        fmap[--ftab[((block[lastShadow + 1] & 0xff) << 8) + (block[1] & 0xff)]] = lastShadow;

        /*
         * Now ftab contains the first loc of every small bucket. Calculate the
         * running order, from smallest to largest big bucket.
         */
        for (int i = 256; --i >= 0; ) {
            bigDone[i] = false;
            runningOrder[i] = i;
        }

        for (int h = 364; h != 1; ) {
            h /= 3;
            for (int i = h; i <= 255; i++) {
                final int vv = runningOrder[i];
                final int a = ftab[(vv + 1) << 8] - ftab[vv << 8];
                final int b = h - 1;
                int j = i;
                for (int ro = runningOrder[j - h]; (ftab[(ro + 1) << 8] - ftab[ro << 8]) > a; ro = runningOrder[j - h]) {
                    runningOrder[j] = ro;
                    j -= h;
                    if (j <= b) {
                        break;
                    }
                }
                runningOrder[j] = vv;
            }
        }

        /*
         * The main sorting loop.
         */
        for (int i = 0; i <= 255; i++) {
            /*
             * Process big buckets, starting with the least full.
             */
            final int ss = runningOrder[i];

            // Step 1:
            /*
             * Complete the big bucket [ss] by quick sorting any unsorted small
             * buckets [ss, j]. Hopefully previous pointer-scanning phases have
             * already completed many of the small buckets [ss, j], so we don't
             * have to sort them at all.
             */
            for (int j = 0; j <= 255; j++) {
                final int sb = (ss << 8) + j;
                final int ftabSb = ftab[sb];
                if ((ftabSb & SET_MASK) != SET_MASK) {
                    final int lo = ftabSb & CLEAR_MASK;
                    final int hi = (ftab[sb + 1] & CLEAR_MASK) - 1;
                    if (hi > lo) {
                        mainQSort3(dataShadow, lo, hi, 2);
                        if (firstAttemptShadow
                                && (this.workDone > workLimitShadow)) {
                            return;
                        }
                    }
                    ftab[sb] = ftabSb | SET_MASK;
                }
            }

            // Step 2:
            // Now scan this big bucket to synthesise the
            // sorted order for small buckets [t, ss] for all t != ss.

            for (int j = 0; j <= 255; j++) {
                copy[j] = ftab[(j << 8) + ss] & CLEAR_MASK;
            }

            for (int j = ftab[ss << 8] & CLEAR_MASK, hj = (ftab[(ss + 1) << 8] & CLEAR_MASK); j < hj; j++) {
                final int fmapJ = fmap[j];
                c1 = block[fmapJ] & 0xff;
                if (!bigDone[c1]) {
                    fmap[copy[c1]] = (fmapJ == 0) ? lastShadow : (fmapJ - 1);
                    copy[c1]++;
                }
            }

            for (int j = 256; --j >= 0; ) {
                ftab[(j << 8) + ss] |= SET_MASK;
            }

            // Step 3:
            /*
             * The ss big bucket is now done. Record this fact, and update the
             * quadrant descriptors. Remember to update quadrants in the
             * overshoot area too, if necessary. The "if (i < 255)" test merely
             * skips this updating for the last bucket processed, since updating
             * for the last bucket is pointless.
             */
            bigDone[ss] = true;

            if (i < 255) {
                final int bbStart = ftab[ss << 8] & CLEAR_MASK;
                final int bbSize = (ftab[(ss + 1) << 8] & CLEAR_MASK) - bbStart;
                int shifts = 0;

                while ((bbSize >> shifts) > 65534) {
                    shifts++;
                }

                for (int j = 0; j < bbSize; j++) {
                    final int a2update = fmap[bbStart + j];
                    final char qVal = (char) (j >> shifts);
                    quadrant[a2update] = qVal;
                    if (a2update < NUM_OVERSHOOT_BYTES) {
                        quadrant[a2update + lastShadow + 1] = qVal;
                    }
                }
            }
        }
    }

    private void randomiseBlock()
    {
        final boolean[] inUse = this.data.inUse;
        final byte[] block = this.data.block;
        final int lastShadow = this.last;

        for (int i = 256; --i >= 0; ) {
            inUse[i] = false;
        }

        int rNToGo = 0;
        int rTPos = 0;
        for (int i = 0, j = 1; i <= lastShadow; i = j, j++) {
            if (rNToGo == 0) {
                rNToGo = (char) R_NUMS[rTPos];
                if (++rTPos == 512) {
                    rTPos = 0;
                }
            }

            rNToGo--;
            block[j] ^= ((rNToGo == 1) ? 1 : 0);

            // handle 16 bit signed numbers
            inUse[block[j] & 0xff] = true;
        }

        this.blockRandomised = true;
    }

    private void generateMTFValues()
    {
        final int lastShadow = this.last;
        final Data dataShadow = this.data;
        final boolean[] inUse = dataShadow.inUse;
        final byte[] block = dataShadow.block;
        final int[] fmap = dataShadow.fmap;
        final char[] sfmap = dataShadow.sfmap;
        final int[] mtfFreq = dataShadow.mtfFreq;
        final byte[] unseqToSeq = dataShadow.unseqToSeq;
        final byte[] yy = dataShadow.generateMTFValuesYy;

        // make maps
        int nInUseShadow = 0;
        for (int i = 0; i < 256; i++) {
            if (inUse[i]) {
                unseqToSeq[i] = (byte) nInUseShadow;
                nInUseShadow++;
            }
        }
        this.nInUse = nInUseShadow;

        final int eob = nInUseShadow + 1;

        for (int i = eob; i >= 0; i--) {
            mtfFreq[i] = 0;
        }

        for (int i = nInUseShadow; --i >= 0; ) {
            yy[i] = (byte) i;
        }

        int wr = 0;
        int zPend = 0;

        for (int i = 0; i <= lastShadow; i++) {
            final byte llI = unseqToSeq[block[fmap[i]] & 0xff];
            byte tmp = yy[0];
            int j = 0;

            while (llI != tmp) {
                j++;
                byte tmp2 = tmp;
                tmp = yy[j];
                yy[j] = tmp2;
            }
            yy[0] = tmp;

            if (j == 0) {
                zPend++;
            }
            else {
                if (zPend > 0) {
                    zPend--;
                    while (true) {
                        if ((zPend & 1) == 0) {
                            sfmap[wr] = RUN_A;
                            wr++;
                            mtfFreq[RUN_A]++;
                        }
                        else {
                            sfmap[wr] = RUN_B;
                            wr++;
                            mtfFreq[RUN_B]++;
                        }

                        if (zPend >= 2) {
                            zPend = (zPend - 2) >> 1;
                        }
                        else {
                            break;
                        }
                    }
                    zPend = 0;
                }
                sfmap[wr] = (char) (j + 1);
                wr++;
                mtfFreq[j + 1]++;
            }
        }

        if (zPend > 0) {
            zPend--;
            while (true) {
                if ((zPend & 1) == 0) {
                    sfmap[wr] = RUN_A;
                    wr++;
                    mtfFreq[RUN_A]++;
                }
                else {
                    sfmap[wr] = RUN_B;
                    wr++;
                    mtfFreq[RUN_B]++;
                }

                if (zPend >= 2) {
                    zPend = (zPend - 2) >> 1;
                }
                else {
                    break;
                }
            }
        }

        sfmap[wr] = (char) eob;
        mtfFreq[eob]++;
        this.nMTF = wr + 1;
    }

    private static final class Data
    {
        // with blockSize 900k
        final boolean[] inUse = new boolean[256]; // 256 byte
        final byte[] unseqToSeq = new byte[256]; // 256 byte
        final int[] mtfFreq = new int[MAX_ALPHA_SIZE]; // 1032 byte
        final byte[] selector = new byte[MAX_SELECTORS]; // 18002 byte
        final byte[] selectorMtf = new byte[MAX_SELECTORS]; // 18002 byte

        final byte[] generateMTFValuesYy = new byte[256]; // 256 byte
        final byte[][] sendMTFValuesLen = new byte[N_GROUPS][MAX_ALPHA_SIZE]; // 1548
        // byte
        final int[][] sendMTFValuesRfreq = new int[N_GROUPS][MAX_ALPHA_SIZE]; // 6192
        // byte
        final int[] sendMTFValuesFave = new int[N_GROUPS]; // 24 byte
        final short[] sendMTFValuesCost = new short[N_GROUPS]; // 12 byte
        final int[][] sendMTFValuesCode = new int[N_GROUPS][MAX_ALPHA_SIZE]; // 6192
        // byte
        final byte[] sendMTFValues2Pos = new byte[N_GROUPS]; // 6 byte
        final boolean[] sentMTFValues4InUse16 = new boolean[16]; // 16 byte

        final int[] stackLl = new int[QSORT_STACK_SIZE]; // 4000 byte
        final int[] stackHh = new int[QSORT_STACK_SIZE]; // 4000 byte
        final int[] stackDd = new int[QSORT_STACK_SIZE]; // 4000 byte

        final int[] mainSortRunningOrder = new int[256]; // 1024 byte
        final int[] mainSortCopy = new int[256]; // 1024 byte
        final boolean[] mainSortBigDone = new boolean[256]; // 256 byte

        final int[] heap = new int[MAX_ALPHA_SIZE + 2]; // 1040 byte
        final int[] weight = new int[MAX_ALPHA_SIZE * 2]; // 2064 byte
        final int[] parent = new int[MAX_ALPHA_SIZE * 2]; // 2064 byte

        final int[] ftab = new int[65537]; // 262148 byte
        // ------------
        // 333408 byte

        final byte[] block; // 900021 byte
        final int[] fmap; // 3600000 byte
        final char[] sfmap; // 3600000 byte
        // ------------
        // 8433529 byte
        // ============

        /**
         * Array instance identical to sfmap, both are used only temporarily and
         * independently, so we do not need to allocate additional memory.
         */
        final char[] quadrant;

        Data(int blockSize100k)
        {
            final int n = blockSize100k * BZip2Constants.BASE_BLOCK_SIZE;
            this.block = new byte[(n + 1 + NUM_OVERSHOOT_BYTES)];
            this.fmap = new int[n];
            this.sfmap = new char[2 * n];
            this.quadrant = this.sfmap;
        }
    }
}
