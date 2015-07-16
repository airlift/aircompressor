package io.airlift.compress;

import com.google.common.io.ByteStreams;

import java.io.PrintStream;

public class HadoopNative
{
    private static boolean initialized;

    public static synchronized void initialize()
    {
        if (initialized) {
            return;
        }

        PrintStream err = System.err;
        try {
            System.setErr(new PrintStream(ByteStreams.nullOutputStream()));
            com.facebook.presto.hadoop.HadoopNative.requireHadoopNative();
            initialized = true;
        }
        finally {
            System.setErr(err);
        }
    }
}
