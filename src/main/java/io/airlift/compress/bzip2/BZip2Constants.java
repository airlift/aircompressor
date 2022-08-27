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

/*
 * This package is based on the work done by Keiron Liddle, Aftex Software
 * <keiron@aftexsw.com> to whom the Ant project is very grateful for his
 * great code.
 */
package io.airlift.compress.bzip2;

/**
 * Base class for both the compress and decompress classes. Holds common arrays,
 * and static data.
 * <p>
 * This interface is public for historical purposes. You should have no need to
 * use it.
 * </p>
 */
// forked from Apache Hadoop
final class BZip2Constants
{
    public static final String HEADER = "BZ";

    public static final int BASE_BLOCK_SIZE = 100000;
    public static final int MAX_ALPHA_SIZE = 258;
    public static final int RUN_A = 0;
    public static final int RUN_B = 1;
    public static final int N_GROUPS = 6;
    public static final int G_SIZE = 50;
    public static final int MAX_SELECTORS = (2 + (900000 / G_SIZE));

    private BZip2Constants() {}
}
