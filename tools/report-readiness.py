#!/usr/bin/env python3
"""Summarize a clean stress/fault run; missing or failed cases are errors."""
import argparse
import json
from pathlib import Path


def expected():
    cases = set()
    for writer in ('record', 'cached', 'cached-dynamic', 'grouped-cached-dynamic', 'modern', 'upsert-record', 'upsert-cached', 'upsert-modern'):
        for parallelism in (4, 8, 16):
            cases.add(f'stress_{writer}_p{parallelism}')
    for mode in ('append', 'upsert'):
        writers = ('record', 'cached', 'modern') if mode == 'upsert' else ('record', 'cached', 'cached-dynamic', 'grouped-cached-dynamic', 'arrow', 'stream-record', 'modern')
        for writer in writers:
            stages = ('create_stream', 'write', 'write_late') if writer == 'modern' else ('create', 'write', 'write_late') if writer == 'stream-record' else ('create', 'write', 'write_late', 'commit')
            for stage in stages:
                for effect in (('http_error', 'lost_ack', 'http_throttle') if 'dynamic' in writer else ('http_error', 'lost_ack')):
                    cases.add(f'fault_{writer}_{mode}_{stage}_{effect}')
    for writer in ('cached-dynamic', 'grouped-cached-dynamic'):
        cases.add(f'fault_{writer}_append_write_http_error_exhausted')
    for writer in ('record', 'cached', 'modern', 'append-record', 'append-cached', 'append-modern'):
        cases.add(f'recovery_{writer}_p2')
    for writer in ('append-cached-dynamic', 'append-grouped-cached-dynamic'):
        for parallelism in (4, 8, 16):
            cases.add(f'recovery_{writer}_p{parallelism}')
    return cases


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--flink-line', choices=['1.16', '1.17', '1.20'], default='1.20')
    parser.add_argument('--directory', type=Path)
    args = parser.parse_args()
    if args.directory is None:
        args.directory = Path(__file__).resolve().parents[1] / 'flink' / args.flink_line / 'target/readiness'
    rows = [json.loads(path.read_text()) for path in sorted(args.directory.glob('*.json'))]
    actual = {row['scenario'] for row in rows}
    missing = expected() - actual
    extra = actual - expected()
    failures = []
    for row in rows:
        counts = row.get('record_counts', [])
        hits = row.get('fault_hits', [])
        if row['scenario'].startswith('recovery_'):
            valid = (row['status'] == 'PASS'
                     and row.get('checkpoint_completed') is True
                     and row.get('task_failure_injected') is True
                     and row.get('source_restored') is True
                     and row.get('cache_files_remaining') == 0
                     and row.get('remaining_writer_threads') == 0
                     and row.get('cleanup_failures') == []
                     and len(counts) == 1
                     and row['rows_per_output'] <= counts[0] <= (row['rows_per_output'] if row['upsert'] else 4 * row['rows_per_output']))
            if row.get('dynamic_partitions'):
                valid = (valid and row.get('verified_partitions_per_output') == 21
                         and int(row.get('restored_source_offset', 0)) > 0
                         and row.get('peak_data_writes', 0) >= 2
                         and len(row.get('fault_hits', [])) == 1
                         and row['fault_hits'][0] > 0)
            if not valid:
                failures.append(row['scenario'])
            continue
        valid = (row['status'] == 'PASS'
                 and len(counts) == row['outputs']
                 and len(hits) == row['outputs']
                 and all(hit > 0 for hit in hits)
                 and row.get('remaining_writer_threads') == 0
                 and row.get('cleanup_failures') == [])
        if row['scenario'].startswith('stress_'):
            valid = (valid and row.get('peak_data_writes', 0) >= max(2, row['parallelism'] // 2)
                     and row.get('write_rounds') == (2 if row['upsert'] else 1))
        if row['upsert'] or row['scenario'].startswith('stress_'):
            valid = valid and all(count == row['rows_per_output'] for count in counts)
        else:
            valid = valid and all(row['rows_per_output'] <= count <= 4 * row['rows_per_output'] for count in counts)
        if row.get('effect') == 'http_error_exhausted':
            valid = valid and hits == [8]
        if row.get('dynamic_partitions'):
            valid = valid and row.get('verified_partitions_per_output') == 21
        if not valid:
            failures.append(row['scenario'])
    lines = ['# Emulator write acceptance evidence', '',
             'This report measures connector correctness against the reviewed emulator. It does not establish cloud throughput, quotas, CDC, permissions or distributed service recovery.', '',
             'Append replay is at-least-once: duplicate rows are reported. PK upsert must converge to exactly one correct row per key.', '',
             'Stress uses a 120-second SDK read timeout and two full write rounds for PK tables. Fault cases use a 3-second read timeout. Each SQL job has a 240-second wait and at most two Flink restarts.', '',
             '| Scenario | Rows/output | Outputs | Rounds | Peak data uploads | Fault hits | Duplicates | Duration ms | Status |',
             '| --- | ---: | ---: | ---: | ---: | --- | --- | ---: | --- |']
    for row in rows:
        lines.append('| {} | {} | {} | {} | {} | {} | {} | {} | {} |'.format(
            row['scenario'], row['rows_per_output'], row['outputs'], row.get('write_rounds', '-'), row.get('peak_data_writes', '-'),
            row.get('fault_hits', []), row.get('duplicate_counts', []), row['duration_ms'], row['status']))
    lines += ['', '## Checkpoint restore evidence', '',
              '| Scenario | Completed checkpoint | Injected task failure | Restored offset | Partitions | Cache files |',
              '| --- | --- | --- | ---: | ---: | ---: |']
    for row in rows:
        if row['scenario'].startswith('recovery_'):
            lines.append(f"| {row['scenario']} | {row.get('checkpoint_completed')} | {row.get('task_failure_injected')} | {row.get('restored_source_offset', '-')} | {row.get('verified_partitions_per_output', '-')} | {row.get('cache_files_remaining', '-')} |")
    lines += ['', f'Missing cases: {sorted(missing)}', f'Unexpected cases: {sorted(extra)}', f'Failed cases: {failures}', '']
    args.directory.mkdir(parents=True, exist_ok=True)
    report = args.directory / 'report.md'
    report.write_text('\n'.join(lines))
    print(report)
    if missing or extra or failures:
        raise SystemExit('Incomplete or failing acceptance matrix')


if __name__ == '__main__':
    main()
