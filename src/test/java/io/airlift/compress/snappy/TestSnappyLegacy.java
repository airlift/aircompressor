/*
 * Copyright (C) 2011 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
 *
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
package io.airlift.compress.snappy;

import org.testng.Assert;

import java.util.Arrays;

public class TestSnappyLegacy
        extends AbstractSnappyTest
{
    @Override
    protected void verifyCompression(byte[] input, int position, int size)
            throws Exception
    {
        byte[] nativeCompressed = new byte[org.xerial.snappy.Snappy.maxCompressedLength(size)];
        byte[] javaCompressed = new byte[Snappy.maxCompressedLength(size)];

        int nativeCompressedSize = org.xerial.snappy.Snappy.compress(
                input,
                position,
                size,
                nativeCompressed,
                0);

        int javaCompressedSize = Snappy.compress(
                input,
                position,
                size,
                javaCompressed,
                0);

        // verify outputs are exactly the same
        String failureMessage = "Invalid compressed output for input size " + size + " at offset " + position;
        if (!SnappyInternalUtils.equals(javaCompressed, 0, nativeCompressed, 0, nativeCompressedSize)) {
            if (nativeCompressedSize < 100) {
                Assert.assertEquals(
                        Arrays.toString(Arrays.copyOf(javaCompressed, nativeCompressedSize)),
                        Arrays.toString(Arrays.copyOf(nativeCompressed, nativeCompressedSize)),
                        failureMessage
                );
            }
            else {
                Assert.fail(failureMessage);
            }
        }
        Assert.assertEquals(javaCompressedSize, nativeCompressedSize);

        // verify the contents can be uncompressed
        byte[] uncompressed = new byte[size];
        Snappy.uncompress(javaCompressed, 0, javaCompressedSize, uncompressed, 0);

        if (!SnappyInternalUtils.equals(uncompressed, 0, input, position, size)) {
            Assert.fail("Invalid uncompressed output for input size " + size + " at offset " + position);
        }
    }
}
