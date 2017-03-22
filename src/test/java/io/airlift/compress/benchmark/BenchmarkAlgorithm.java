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
package io.airlift.compress.benchmark;

import io.airlift.compress.Algorithm;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

@State(Scope.Thread)
public class BenchmarkAlgorithm
{
    @Param({
            "airlift_lz4",
            "airlift_lzo",
            "airlift_snappy",
            "airlift_zstd",

            "iq80_snappy",
            "xerial_snappy",
            "jpountz_lz4_jni",
            "hadoop_lzo",

            "airlift_lz4_stream",
            "airlift_lzo_stream",
            "airlift_snappy_stream",

            "hadoop_lz4_stream",
            "hadoop_lzo_stream",
            "hadoop_snappy_stream",
            "java_zip_stream",
            "hadoop_gzip_stream",
    })
    private Algorithm algorithm;

    public BenchmarkAlgorithm()
    {
    }

    public BenchmarkAlgorithm(Algorithm algorithm)
    {
        this.algorithm = algorithm;
    }

    public Algorithm getAlgorithm()
    {
        return algorithm;
    }
}
