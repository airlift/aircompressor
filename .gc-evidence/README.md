# PR 396 GC cutoff evidence

This branch is an experiment based on PR head
`2386b7cc1dd7ddc91e9d3707bd6ee6f48f731abd`. It does not change the PR branch or
the production source files. Its workflow replaces the ordinary CI workflow on
this evidence branch only. No unit tests or broad benchmarks run on a developer
machine or in this workflow.

On a Java 25 Linux host, run `python3 .gc-evidence/run.py`. It builds production
classes with Maven's test compilation and execution disabled, compiles a second
copy of four production classes with exactly the reviewer-proposed cutoff changes,
and runs fresh JVMs with either the current or proposed classes first on the
classpath. `proposal.patch` records every source difference in that second copy.

The proposed limits are 256 KiB decompression output, four times the existing
compressed-input bound, 1 MiB XXH3 input, and 256 KiB XXH32 input. Compression
limits remain unchanged. The hash constants also affect streaming updates; both
64-bit and 128-bit XXH3 paths are measured. The existing 64 KiB LZ4 compression
path is an unchanged control.

Each JVM warms up for three seconds, records two seconds of throughput without
explicit GC, and then requests 100 G1 full collections at seeded 10–30 ms intervals
while workers continue operating. Separate log measurements record safepoint
reach, time at safepoint, GC pause, total safepoint, and caller-observed GC duration.
Three forks per variant run with eight workers and 300 parked platform threads;
one fork per variant runs with one worker and no parked threads. Variant order
reverses on alternate repeats, and the runner executes only one JVM at a time.

The decoder workloads include four-symbol random data and valid synthetic blocks
with many short matches. These cover different work profiles, but are not claimed
to establish a worst-case decoder bound. Fixed heap and GC/JIT thread settings,
the JDK build, CPU details, per-fork system load, exact commands, per-fork results,
and raw JVM logs are retained with the aggregate report. Hosted-runner results
remain subject to scheduler and hypervisor interference; oversubscribed results
must be read alongside the single-worker runs and per-fork distributions.

The status on the original PR commit indicates experiment completion, not a
performance acceptance criterion. On successful completion, the workflow publishes
the measurement summary and raw-artifact link to PR 396 with the repository's
ephemeral GitHub Actions token. Results do not automatically change production
cutoffs. Review the evidence before deciding whether their latency cost is acceptable.
