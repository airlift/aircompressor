package org.iq80.snappy;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class Main
{
    public static void main(String[] args)
            throws Exception
    {
        if ((args.length == 1) && (args[0].equals("-c"))) {
            compress();
        }
        else if ((args.length == 1) && (args[0].equals("-d"))) {
            uncompress();
        }
        else {
            usage();
        }
    }

    private static void usage()
    {
        System.err.println("Usage: java -jar snappy.jar OPTION");
        System.err.println("Compress or uncompress with Snappy.");
        System.err.println();
        System.err.println("  -c     compress from stdin to stdout");
        System.err.println("  -d     uncompress from stdin to stdout");
        System.exit(100);
    }

    private static void compress()
            throws IOException
    {
        copy(System.in, new SnappyOutputStream(System.out));
    }

    private static void uncompress()
            throws IOException
    {
        copy(new SnappyInputStream(System.in), System.out);
    }

    private static void copy(InputStream in, OutputStream out)
            throws IOException
    {
        byte[] buf = new byte[4096];
        while (true) {
            int r = in.read(buf);
            if (r == -1) {
                out.close();
                in.close();
                return;
            }
            out.write(buf, 0, r);
        }
    }
}
