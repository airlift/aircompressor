import io.airlift.compress.v3.Compressor;
import io.airlift.compress.v3.Decompressor;
import io.airlift.compress.v3.lz4.Lz4NativeCompressor;
import io.airlift.compress.v3.lz4.Lz4NativeDecompressor;
import io.airlift.compress.v3.snappy.SnappyNativeCompressor;
import io.airlift.compress.v3.snappy.SnappyNativeDecompressor;
import io.airlift.compress.v3.xxhash.XxHash3Native;
import io.airlift.compress.v3.xxhash.XxHash32NativeHasher;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

public final class CutoffGcProbe {
    private static volatile long sink;

    private static final class Worker {
        volatile long operations;
        Thread thread;
    }

    // Valid copy-heavy blocks at exactly 256 KiB output. These exercise many short
    // matches rather than treating compression ratio as a bound on decoder time.
    private static byte[] shortMatches(int size, boolean lz4) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        if (lz4) {
            out.write(0x10);
            out.write(42);
            out.write(1);
            out.write(0);
            for (int i = 0; i < (size - 5 - 11) / 4; i++) {
                out.write(0);
                out.write(1);
                out.write(0);
            }
            out.write(0xB0);
            for (int i = 0; i < 11; i++) {
                out.write(42);
            }
        }
        else {
            int remaining = size;
            while (remaining >= 128) {
                out.write((remaining & 127) | 128);
                remaining >>>= 7;
            }
            out.write(remaining);
            out.write(0);
            out.write(42);
            for (int i = 0; i < (size - 1 - 3) / 4; i++) {
                out.write(1);
                out.write(1);
            }
            out.write(8);
            for (int i = 0; i < 3; i++) {
                out.write(42);
            }
        }
        return out.toByteArray();
    }

    private static long total(List<Worker> workers) {
        long result = 0;
        for (Worker worker : workers) {
            result += worker.operations;
        }
        return result;
    }

    public static void main(String[] args) throws Exception {
        String algorithm = args[0];
        int workerCount = Integer.parseInt(args[1]);
        int idleCount = Integer.parseInt(args[2]);
        int samples = Integer.parseInt(args[3]);
        int size = algorithm.startsWith("xxh3") ? 1024 * 1024 : 256 * 1024;
        if (algorithm.equals("lz4compress64k")) {
            size = 64 * 1024;
        }
        byte[] input = new byte[size];
        new Random(42).nextBytes(input);
        boolean lz4 = algorithm.startsWith("lz4");
        boolean codec = lz4 || algorithm.startsWith("snappy");
        byte[] encoded = null;
        if (codec) {
            for (int i = 0; i < size; i++) {
                input[i] &= 3;
            }
            Compressor compressor = lz4 ? new Lz4NativeCompressor() : new SnappyNativeCompressor();
            byte[] buffer = new byte[compressor.maxCompressedLength(size)];
            int length = compressor.compress(input, 0, size, buffer, 0, buffer.length);
            encoded = algorithm.endsWith("shortmatches") ? shortMatches(size, lz4) : Arrays.copyOf(buffer, length);
            if (algorithm.endsWith("shortmatches")) {
                Arrays.fill(input, (byte) 42);
            }
        }

        AtomicBoolean stop = new AtomicBoolean();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch ready = new CountDownLatch(workerCount);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch idleStop = new CountDownLatch(1);
        List<Thread> idleThreads = new ArrayList<>();
        List<Worker> workers = new ArrayList<>();
        List<AutoCloseable> resources = new ArrayList<>();
        for (int i = 0; i < idleCount; i++) {
            Thread idle = new Thread(() -> {
                try {
                    idleStop.await();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, "idle-" + i);
            idle.start();
            idleThreads.add(idle);
        }
        try {
            for (int i = 0; i < workerCount; i++) {
                LongSupplier operation;
                if (algorithm.equals("lz4compress64k")) {
                    Compressor compressor = new Lz4NativeCompressor();
                    byte[] output = new byte[compressor.maxCompressedLength(input.length)];
                    operation = () -> compressor.compress(input, 0, input.length, output, 0, output.length);
                }
                else if (codec) {
                    Decompressor decompressor = lz4 ? new Lz4NativeDecompressor() : new SnappyNativeDecompressor();
                    byte[] source = encoded;
                    byte[] output = new byte[size];
                    if (decompressor.decompress(source, 0, source.length, output, 0, output.length) != size || !Arrays.equals(input, output)) {
                        throw new AssertionError("Invalid benchmark block: " + algorithm);
                    }
                    operation = () -> decompressor.decompress(source, 0, source.length, output, 0, output.length);
                }
                else {
                    operation = switch (algorithm) {
                        case "xxh3" -> () -> XxHash3Native.hash(input);
                        case "xxh3128" -> () -> XxHash3Native.hash128(input).low();
                        case "xxh32" -> () -> XxHash32NativeHasher.hash(input, 0, input.length, 0);
                        case "xxh3update" -> {
                            var hasher = XxHash3Native.newHasher();
                            resources.add(hasher);
                            yield () -> { hasher.update(input); return 1; };
                        }
                        case "xxh3128update" -> {
                            var hasher = XxHash3Native.newHasher128();
                            resources.add(hasher);
                            yield () -> { hasher.update(input); return 1; };
                        }
                        case "xxh32update" -> {
                            var hasher = new XxHash32NativeHasher(0);
                            resources.add(hasher);
                            yield () -> { hasher.update(input); return 1; };
                        }
                        default -> throw new IllegalArgumentException(algorithm);
                    };
                }
                Worker worker = new Worker();
                worker.thread = new Thread(() -> {
                    ready.countDown();
                    try {
                        start.await();
                        long count = 0;
                        long checksum = 0;
                        while (!stop.get()) {
                            for (int batch = 0; batch < 64; batch++) {
                                checksum ^= operation.getAsLong();
                            }
                            count += 64;
                            worker.operations = count;
                        }
                        sink = checksum;
                    }
                    catch (Throwable t) {
                        failure.compareAndSet(null, t);
                        stop.set(true);
                    }
                }, "worker-" + i);
                workers.add(worker);
                worker.thread.start();
            }
            ready.await();
            start.countDown();
            Thread.sleep(3000);
            long before = total(workers);
            long throughputStart = System.nanoTime();
            Thread.sleep(2000);
            double seconds = (System.nanoTime() - throughputStart) / 1e9;
            double operationsPerSecond = (total(workers) - before) / seconds;
            System.out.println("GC_MEASUREMENT_START");
            double[] gcMillis = new double[samples];
            Random intervals = new Random(17);
            for (int i = 0; i < samples; i++) {
                long gcStart = System.nanoTime();
                System.gc();
                gcMillis[i] = (System.nanoTime() - gcStart) / 1e6;
                Thread.sleep(10 + intervals.nextInt(21));
            }
            System.out.println("GC_MEASUREMENT_END");
            if (failure.get() != null) {
                throw new AssertionError("Worker failed", failure.get());
            }
            System.out.printf(Locale.ROOT,
                    "RESULT {\"algorithm\":\"%s\",\"workers\":%d,\"idle\":%d,\"size\":%d,\"encodedSize\":%d,\"operationsPerSecond\":%.6f,\"gcCallMillis\":%s}%n",
                    algorithm, workerCount, idleCount, size, encoded == null ? 0 : encoded.length, operationsPerSecond, Arrays.toString(gcMillis));
        }
        finally {
            stop.set(true);
            start.countDown();
            for (Worker worker : workers) {
                worker.thread.join();
            }
            idleStop.countDown();
            for (Thread thread : idleThreads) {
                thread.join();
            }
            for (AutoCloseable resource : resources) {
                resource.close();
            }
        }
    }
}
