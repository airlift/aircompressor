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

import org.apache.hadoop.io.compress.CompressionInputStream;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static io.airlift.compress.bzip2.BZip2Constants.HEADER;

// forked from Apache Hadoop
class BZip2CompressionInputStream
        extends CompressionInputStream
{
    private static final int HEADER_LEN = HEADER.length();

    private CBZip2InputStream input;
    private boolean needsReset;
    private BufferedInputStream bufferedIn;
    private final long startingPos;

    // Following state machine handles different states of compressed stream
    // position
    // HOLD : Don't advertise compressed stream position
    // ADVERTISE : Read 1 more character and advertise stream position
    // See more comments about it before updatePos method.
    private enum PosAdvertisementStateMachine
    {
        HOLD, ADVERTISE
    }

    private PosAdvertisementStateMachine posSM = PosAdvertisementStateMachine.HOLD;
    private long compressedStreamPosition;

    public BZip2CompressionInputStream(InputStream in)
            throws IOException
    {
        super(in);
        needsReset = false;
        bufferedIn = new BufferedInputStream(in);
        this.startingPos = super.getPos();
        if (this.startingPos == 0) {
            // We only strip header if it is start of file
            bufferedIn = readStreamHeader();
        }
        input = new CBZip2InputStream(bufferedIn);

        this.updatePos(false);
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
            }
        }

        if (bufferedIn == null) {
            throw new IOException("Failed to read bzip2 stream.");
        }

        return bufferedIn;
    }

    @Override
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
        if (len == 0) {
            return 0;
        }

        if (needsReset) {
            needsReset = false;
            BufferedInputStream bufferedIn = readStreamHeader();
            input = new CBZip2InputStream(bufferedIn);
        }

        int result;
        result = this.input.read(b, off, len);
        if (result == CBZip2InputStream.END_OF_BLOCK) {
            this.posSM = PosAdvertisementStateMachine.ADVERTISE;
        }

        if (this.posSM == PosAdvertisementStateMachine.ADVERTISE) {
            result = this.input.read(b, off, 1);
            // This is the precise time to update compressed stream position
            // to the client of this code.
            this.updatePos(true);
            this.posSM = PosAdvertisementStateMachine.HOLD;
        }

        return result;
    }

    @Override
    public int read()
            throws IOException
    {
        byte[] b = new byte[1];
        int result = this.read(b, 0, 1);
        return (result < 0) ? result : (b[0] & 0xff);
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
