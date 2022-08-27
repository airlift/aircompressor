/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.airlift.compress.bzip2;

import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec.READ_MODE;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static io.airlift.compress.bzip2.BZip2Constants.HEADER;

/**
 * This class is capable to de-compress BZip2 data in two modes;
 * CONTINOUS and BYBLOCK.  BYBLOCK mode makes it possible to
 * do decompression starting any arbitrary position in the stream.
 * <p>
 * So this facility can easily be used to parallelize decompression
 * of a large BZip2 file for performance reasons.  (It is exactly
 * done so for Hadoop framework.  See LineRecordReader for an
 * example).  So one can break the file (of course logically) into
 * chunks for parallel processing.  These "splits" should be like
 * default Hadoop splits (e.g as in FileInputFormat getSplit metod).
 * So this code is designed and tested for FileInputFormat's way
 * of splitting only.
 */
class BZip2CompressionInputStream
        extends SplitCompressionInputStream
{
    private static final int HEADER_LEN = HEADER.length();
    private static final String SUB_HEADER = "h9";
    private static final int SUB_HEADER_LEN = SUB_HEADER.length();

    private CBZip2InputStream input;
    boolean needsReset;
    private BufferedInputStream bufferedIn;
    private boolean isHeaderStripped;
    private boolean isSubHeaderStripped;
    private final READ_MODE readMode;
    private final long startingPos;

    // Following state machine handles different states of compressed stream
    // position
    // HOLD : Don't advertise compressed stream position
    // ADVERTISE : Read 1 more character and advertise stream position
    // See more comments about it before updatePos method.
    private enum POS_ADVERTISEMENT_STATE_MACHINE
    {
        HOLD, ADVERTISE
    }

    POS_ADVERTISEMENT_STATE_MACHINE posSM = POS_ADVERTISEMENT_STATE_MACHINE.HOLD;
    long compressedStreamPosition = 0;

    public BZip2CompressionInputStream(InputStream in)
            throws IOException
    {
        this(in, 0L, Long.MAX_VALUE, READ_MODE.CONTINUOUS);
    }

    public BZip2CompressionInputStream(InputStream in, long start, long end, READ_MODE readMode)
            throws IOException
    {
        super(in, start, end);
        needsReset = false;
        bufferedIn = new BufferedInputStream(super.in);
        this.startingPos = super.getPos();
        this.readMode = readMode;
        long numSkipped = 0;
        if (this.startingPos == 0) {
            // We only strip header if it is start of file
            bufferedIn = readStreamHeader();
        }
        else if (this.readMode == READ_MODE.BYBLOCK &&
                this.startingPos <= HEADER_LEN + SUB_HEADER_LEN) {
            // When we're in BYBLOCK mode and the start position is >=0
            // and < HEADER_LEN + SUB_HEADER_LEN, we should skip to after
            // start of the first bz2 block to avoid duplicated records
            numSkipped = HEADER_LEN + SUB_HEADER_LEN + 1 - this.startingPos;
            long skipBytes = numSkipped;
            while (skipBytes > 0) {
                long s = bufferedIn.skip(skipBytes);
                if (s > 0) {
                    skipBytes -= s;
                }
                else {
                    if (bufferedIn.read() == -1) {
                        break; // end of the split
                    }
                    else {
                        skipBytes--;
                    }
                }
            }
        }
        input = new CBZip2InputStream(bufferedIn, readMode);
        if (this.isHeaderStripped) {
            input.updateReportedByteCount(HEADER_LEN);
        }

        if (this.isSubHeaderStripped) {
            input.updateReportedByteCount(SUB_HEADER_LEN);
        }

        if (numSkipped > 0) {
            input.updateReportedByteCount((int) numSkipped);
        }

        // To avoid dropped records, not advertising a new byte position
        // when we are in BYBLOCK mode and the start position is 0
        if (!(this.readMode == READ_MODE.BYBLOCK && this.startingPos == 0)) {
            this.updatePos(false);
        }
    }

    private BufferedInputStream readStreamHeader()
            throws IOException
    {
        // We are flexible enough to allow the compressed stream not to
        // start with the header of BZ. So it works fine either we have
        // the header or not.
        if (in != null) {
            bufferedIn.mark(HEADER_LEN);
            byte[] headerBytes = new byte[HEADER_LEN];
            int actualRead = bufferedIn.read(headerBytes, 0, HEADER_LEN);
            if (actualRead != -1) {
                String header = new String(headerBytes, StandardCharsets.UTF_8);
                if (header.compareTo(HEADER) != 0) {
                    bufferedIn.reset();
                }
                else {
                    this.isHeaderStripped = true;
                    // In case of BYBLOCK mode, we also want to strip off
                    // remaining two character of the header.
                    if (this.readMode == READ_MODE.BYBLOCK) {
                        actualRead = bufferedIn.read(headerBytes, 0,
                                SUB_HEADER_LEN);
                        if (actualRead != -1) {
                            this.isSubHeaderStripped = true;
                        }
                    }
                }
            }
        }

        if (bufferedIn == null) {
            throw new IOException("Failed to read bzip2 stream.");
        }

        return bufferedIn;
    }

    public void close()
            throws IOException
    {
        if (!needsReset) {
            try {
                input.close();
                needsReset = true;
            }
            finally {
                super.close();
            }
        }
    }

    /**
     * This method updates compressed stream position exactly when the
     * client of this code has read off at least one byte passed any BZip2
     * end of block marker.
     * <p>
     * This mechanism is very helpful to deal with data level record
     * boundaries. Please see constructor and next methods of
     * org.apache.hadoop.mapred.LineRecordReader as an example usage of this
     * feature.  We elaborate it with an example in the following:
     * <p>
     * Assume two different scenarios of the BZip2 compressed stream, where
     * [m] represent end of block, \n is line delimiter and . represent compressed
     * data.
     * <p>
     * ............[m]......\n.......
     * <p>
     * ..........\n[m]......\n.......
     * <p>
     * Assume that end is right after [m].  In the first case the reading
     * will stop at \n and there is no need to read one more line.  (To see the
     * reason of reading one more line in the next() method is explained in LineRecordReader.)
     * While in the second example LineRecordReader needs to read one more line
     * (till the second \n).  Now since BZip2Codecs only update position
     * at least one byte passed a maker, so it is straight forward to differentiate
     * between the two cases mentioned.
     */
    @Override
    public int read(byte[] b, int off, int len)
            throws IOException
    {
        if (needsReset) {
            internalReset();
        }

        int result = 0;
        result = this.input.read(b, off, len);
        if (result == BZip2Constants.END_OF_BLOCK) {
            this.posSM = POS_ADVERTISEMENT_STATE_MACHINE.ADVERTISE;
        }

        if (this.posSM == POS_ADVERTISEMENT_STATE_MACHINE.ADVERTISE) {
            result = this.input.read(b, off, off + 1);
            // This is the precise time to update compressed stream position
            // to the client of this code.
            this.updatePos(true);
            this.posSM = POS_ADVERTISEMENT_STATE_MACHINE.HOLD;
        }

        return result;

    }

    @Override
    public int read()
            throws IOException
    {
        byte b[] = new byte[1];
        int result = this.read(b, 0, 1);
        return (result < 0) ? result : (b[0] & 0xff);
    }

    private void internalReset()
            throws IOException
    {
        if (needsReset) {
            needsReset = false;
            BufferedInputStream bufferedIn = readStreamHeader();
            input = new CBZip2InputStream(bufferedIn, this.readMode);
        }
    }

    @Override
    public void resetState()
    {
        // Cannot read from bufferedIn at this point because bufferedIn
        // might not be ready
        // yet, as in SequenceFile.Reader implementation.
        needsReset = true;
    }

    @Override
    public long getPos()
    {
        return this.compressedStreamPosition;
    }

    /*
     * As the comments before read method tell that
     * compressed stream is advertised when at least
     * one byte passed EOB have been read off.  But
     * there is an exception to this rule.  When we
     * construct the stream we advertise the position
     * exactly at EOB.  In the following method
     * shouldAddOn boolean captures this exception.
     *
     */
    private void updatePos(boolean shouldAddOn)
    {
        int addOn = shouldAddOn ? 1 : 0;
        this.compressedStreamPosition = this.startingPos + this.input.getProcessedByteCount() + addOn;
    }
}
