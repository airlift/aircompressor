#!/usr/bin/env python3
"""Run only on a remote benchmark host; no unit test suite is invoked."""
import collections
import json
import math
import os
from pathlib import Path
import platform
import re
import subprocess
import time

ROOT = Path.cwd()
HERE = ROOT / '.gc-evidence'
OUT = ROOT / 'gc-evidence-results'
OUT.mkdir(exist_ok=True)
SOURCE_HEAD = '2386b7cc1dd7ddc91e9d3707bd6ee6f48f731abd'


def command(args, **kwargs):
    return subprocess.run(args, check=True, text=True, **kwargs)


def metadata():
    info = {'sourceHead': SOURCE_HEAD, 'benchmarkHead': os.environ.get('GITHUB_SHA'),
            'platform': platform.platform(), 'cpuCount': os.cpu_count(),
            'affinity': sorted(os.sched_getaffinity(0)),
            'java': command(['java', '-version'], capture_output=True).stderr,
            'lscpu': command(['lscpu'], capture_output=True).stdout}
    for name in ['/sys/fs/cgroup/cpu.max', '/proc/cpuinfo', '/proc/meminfo']:
        path = Path(name)
        if path.exists():
            info[name] = path.read_text()
    (OUT / 'environment.json').write_text(json.dumps(info, indent=2))


def prepare():
    with (OUT / 'build.log').open('w') as log:
        command(['./mvnw', '-B', '-Dair.check.skip-all', '-Dmaven.test.skip=true', 'package'],
                stdout=log, stderr=subprocess.STDOUT)
    probe = OUT / 'probe-classes'
    probe.mkdir(exist_ok=True)
    command(['javac', '-proc:none', '-cp', 'target/classes', '-d', str(probe), str(HERE / 'CutoffGcProbe.java')])
    changes = {
        'lz4/Lz4Native.java': (
            'compressedLength <= MAX_CRITICAL_COMPRESSED_SIZE && outputLength <= MAX_CRITICAL_INPUT_SIZE',
            'compressedLength <= 4 * MAX_CRITICAL_COMPRESSED_SIZE && outputLength <= 4 * MAX_CRITICAL_INPUT_SIZE'),
        'snappy/SnappyNative.java': (
            'compressedLength <= MAX_CRITICAL_COMPRESSED_SIZE && uncompressedLength <= MAX_CRITICAL_INPUT_SIZE',
            'compressedLength <= 4 * MAX_CRITICAL_COMPRESSED_SIZE && uncompressedLength <= 4 * MAX_CRITICAL_INPUT_SIZE'),
        'xxhash/XxHash3Bindings.java': (
            'MAX_CRITICAL_INPUT_SIZE = 8 * 1024;', 'MAX_CRITICAL_INPUT_SIZE = 1024 * 1024;'),
        'xxhash/XxHash32Bindings.java': (
            'MAX_CRITICAL_INPUT_SIZE = 8 * 1024;', 'MAX_CRITICAL_INPUT_SIZE = 256 * 1024;'),
    }
    sources = []
    patches = []
    import difflib
    for relative, (old, new) in changes.items():
        source = ROOT / 'src/main/java/io/airlift/compress/v3' / relative
        current = source.read_text()
        assert current.count(old) == 1, relative
        proposed = current.replace(old, new)
        target = OUT / 'proposal-src' / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(proposed)
        sources.append(str(target))
        patches.extend(difflib.unified_diff(current.splitlines(True), proposed.splitlines(True),
                                           fromfile='current/' + relative, tofile='proposed/' + relative))
    (OUT / 'proposal.patch').write_text(''.join(patches))
    proposed_classes = OUT / 'proposed-classes'
    proposed_classes.mkdir(exist_ok=True)
    command(['javac', '-proc:none', '-cp', 'target/classes', '-d', str(proposed_classes), *sources])


def percentile(values, quantile):
    values = sorted(values)
    return values[max(0, math.ceil(quantile * len(values)) - 1)]


def stats(values):
    return {'n': len(values), 'p50': percentile(values, .5), 'p95': percentile(values, .95),
            'p99': percentile(values, .99), 'max': max(values)}


def run_case(algorithm, variant, workers, idle, repeat):
    label = f'{workers}w-{idle}idle-{algorithm}-r{repeat}-{variant}'
    log = OUT / (label + '.log')
    classpath = [str(OUT / 'probe-classes'), 'target/classes']
    if variant == 'proposed':
        classpath.insert(0, str(OUT / 'proposed-classes'))
    args = ['java', '--enable-native-access=ALL-UNNAMED', '-Xms256m', '-Xmx256m', '-Xss256k',
            '-XX:+UseG1GC', '-XX:ParallelGCThreads=2', '-XX:ConcGCThreads=1', '-XX:CICompilerCount=2',
            '-Xlog:gc=info,safepoint=info', '-cp', os.pathsep.join(classpath),
            'CutoffGcProbe', algorithm, str(workers), str(idle), '100']
    before = {'time': time.time(), 'load': os.getloadavg(), 'command': args}
    with log.open('w') as output:
        command(args, stdout=output, stderr=subprocess.STDOUT, timeout=90)
    body = log.read_text()
    result = json.loads(next(line.removeprefix('RESULT ') for line in body.splitlines() if line.startswith('RESULT ')))
    measured = body.split('GC_MEASUREMENT_START\n', 1)[1].split('GC_MEASUREMENT_END', 1)[0]
    events = re.findall(r'Safepoint "G1CollectFull".*?Reaching safepoint: (\d+) ns, At safepoint: (\d+) ns, Leaving safepoint: (\d+) ns, Total: (\d+) ns', measured)
    assert len(events) == 100, (label, len(events))
    pauses = re.findall(r'GC\(\d+\) Pause Full \(System\.gc\(\)\).*? ([0-9.]+)ms', measured)
    assert len(pauses) == 100, (label, len(pauses))
    result.update({'variant': variant, 'repeat': repeat, 'hostBefore': before,
                   'hostAfter': {'time': time.time(), 'load': os.getloadavg()},
                   'gcPauseMillis': [float(pause) for pause in pauses],
                   'safepointReachMicros': [int(event[0]) / 1000 for event in events],
                   'safepointWorkMicros': [int(event[1]) / 1000 for event in events],
                   'safepointTotalMicros': [int(event[3]) / 1000 for event in events]})
    (OUT / (label + '.json')).write_text(json.dumps(result, indent=2))
    print(label, 'safepoint reach us:', stats(result['safepointReachMicros']), flush=True)
    return result


def summarize(results):
    groups = collections.defaultdict(list)
    for result in results:
        groups[(result['workers'], result['idle'], result['algorithm'], result['variant'])].append(result)
    summary = []
    for (workers, idle, algorithm, variant), forks in groups.items():
        entry = {'workers': workers, 'idle': idle, 'algorithm': algorithm, 'variant': variant,
                 'forks': len(forks), 'throughputOpsPerSecondByFork': [f['operationsPerSecond'] for f in forks]}
        for field in ['safepointReachMicros', 'safepointWorkMicros', 'safepointTotalMicros', 'gcPauseMillis', 'gcCallMillis']:
            entry[field] = stats([sample for fork in forks for sample in fork[field]])
            entry[field + 'ByFork'] = [stats(fork[field]) for fork in forks]
        summary.append(entry)
    (OUT / 'summary.json').write_text(json.dumps(summary, indent=2))
    lines = ['# GC cutoff comparison', '', f'Source: `{SOURCE_HEAD}`. Java 25, G1, fixed 256 MiB heap.', '',
             'Same runner; variants run sequentially, with order reversed on alternate repeats. '
             'Three fresh JVMs per variant at eight workers plus 300 parked platform threads; '
             'one JVM per variant at one worker with no parked threads. '
             'Each JVM warms up for 3 s, measures throughput for 2 s without forced GC, '
             'then requests 100 full collections at seeded 10–30 ms intervals under continued load.', '',
             'Safepoint reach is reported separately from time at safepoint. '
             'These are hosted-runner stress observations, not an isolated-host latency guarantee. '
             'Eight workers may oversubscribe the runner; environment and per-fork load are included. '
             'Single-worker results help identify scheduler effects. '
             'Short-match blocks are valid synthetic inputs, not a proven worst-case bound. '
             'No codec tests run in this workflow.', '',
             '| Workers / idle | Operation | Variant | Reach p50 / p95 / p99 / max (us) | At safepoint p50 / p99 (us) | GC call p50 / p99 (ms) | Ops/s by JVM |',
             '| --- | --- | --- | --- | --- | --- | --- |']
    for entry in sorted(summary, key=lambda e: (e['workers'], e['algorithm'], e['variant'])):
        reach = entry['safepointReachMicros']
        work = entry['safepointWorkMicros']
        gc = entry['gcCallMillis']
        lines.append(f"| {entry['workers']} / {entry['idle']} | {entry['algorithm']} | {entry['variant']} | " +
                     ' / '.join(f'{reach[k]:.1f}' for k in ['p50', 'p95', 'p99', 'max']) +
                     f" | {work['p50']:.1f} / {work['p99']:.1f} | {gc['p50']:.3f} / {gc['p99']:.3f} | " +
                     ', '.join(f'{v:.0f}' for v in entry['throughputOpsPerSecondByFork']) + ' |')
    (OUT / 'summary.md').write_text('\n'.join(lines) + '\n')
    if os.environ.get('GITHUB_STEP_SUMMARY'):
        with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as output:
            output.write((OUT / 'summary.md').read_text())


if __name__ == '__main__':
    metadata()
    prepare()
    algorithms = ['lz4', 'lz4shortmatches', 'snappy', 'snappyshortmatches',
                  'xxh3', 'xxh3128', 'xxh32', 'xxh3update', 'xxh3128update', 'xxh32update',
                  'lz4compress64k']
    results = []
    for workers, idle, repetitions in [(1, 0, 1), (8, 300, 3)]:
        for repeat in range(repetitions):
            for algorithm in (algorithms if repeat % 2 == 0 else list(reversed(algorithms))):
                for variant in (['current', 'proposed'] if repeat % 2 == 0 else ['proposed', 'current']):
                    results.append(run_case(algorithm, variant, workers, idle, repeat))
                    summarize(results)
