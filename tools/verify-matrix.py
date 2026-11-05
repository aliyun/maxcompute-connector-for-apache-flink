#!/usr/bin/env python3
"""Compile and regress each runtime in isolation; preserve counts, identity and artifact hashes."""
import argparse
import datetime as dt
import hashlib
import json
import os
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VERSIONS = {'1.16': '1.16.3', '1.17': '1.17.2', '1.20': '1.20.4'}


def source_digest():
    digest = hashlib.sha256()
    files = [ROOT / 'pom.xml']
    for directory in ('src', *('flink/' + line for line in VERSIONS)):
        files.extend(p for p in (ROOT / directory).rglob('*')
                     if p.is_file() and 'target' not in p.relative_to(ROOT).parts)
    for path in sorted(files):
        digest.update(str(path.relative_to(ROOT)).encode() + b'\0' + path.read_bytes())
    return digest.hexdigest()


def reports(directory):
    suites = []
    for path in sorted(directory.glob('TEST-*.xml')):
        root = ET.parse(path).getroot()
        suites.append({'name': root.attrib['name'], **{
            key: int(root.attrib.get(key, '0')) for key in ('tests', 'failures', 'errors', 'skipped')}})
    return suites


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--flink-line', choices=VERSIONS, action='append', help='Default: all three lines')
    args = parser.parse_args()
    lines = args.flink_line or list(VERSIONS)
    if len(set(lines)) != len(lines):
        parser.error('Duplicate Flink line')
    destination = ROOT / '.verification/matrix'
    destination.mkdir(parents=True, exist_ok=True)
    source = source_digest()
    result = {'observed_at': dt.datetime.now(dt.timezone.utc).isoformat(),
              'source_commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
              'source_sha256': source, 'verification': 'local offline/MiniCluster and distributed jar; no cloud acceptance',
              'versions': []}
    env = os.environ.copy()
    for name in ('ODPS_ACCESS_ID', 'ODPS_ACCESS_KEY', 'ODPS_SECURITY_TOKEN',
                 'MAXCOMPUTE_PROJECT', 'MAXCOMPUTE_ENDPOINT'):
        env.pop(name, None)
    for line in lines:
        module = ROOT / 'flink' / line
        target = module / 'target'
        command = ['mvn', '-s', 'tools/maven-central-settings.xml', '-gs',
                   'tools/maven-central-settings.xml', '-B', '-ntp', '-pl', module.relative_to(ROOT).as_posix(), '-am',
                   '-Ppublic,release-artifacts', 'clean', 'verify']
        started = time.monotonic()
        log = destination / ('flink-' + line + '.log')
        print('Verifying Flink ' + VERSIONS[line] + '; log: ' + str(log), flush=True)
        with log.open('w') as output:
            code = subprocess.run(command, cwd=ROOT, env=env, stdout=output, stderr=subprocess.STDOUT).returncode
        suites = reports(target / 'surefire-reports')
        consumer = reports(target / 'failsafe-reports')
        errors = []
        if not suites or not consumer:
            errors.append('Missing unit or distributed artifact test reports')
        if any(s[key] for s in suites + consumer for key in ('failures', 'errors', 'skipped')):
            errors.append('Tests failed or skipped')
        if not any(s['name'].endswith('DynamicShuffleSinkFailoverMiniClusterTest') and s['tests'] > 0 for s in suites):
            errors.append('Dynamic shuffle MiniCluster recovery did not run')
        if not any(s['name'].endswith('StorageWriterTest') and s['tests'] > 0 for s in suites):
            errors.append('Shared Storage lifecycle regressions did not run')
        artifacts = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(target.glob('*.jar'))}
        if not any(name.endswith('-shaded.jar') for name in artifacts):
            errors.append('Missing distributed jar')
        core = {}
        jar = next(target.glob('*-shaded.jar'), None)
        if jar:
            with zipfile.ZipFile(jar) as archive:
                for name in ('org/apache/flink/odps/storage/StorageWriter.class',
                             'org/apache/flink/odps/sink/shuffle/AssignmentComputer.class',
                             'org/apache/flink/odps/sink/shuffle/PartitionBucketAssignment.class'):
                    core[name] = hashlib.sha256(archive.read(name)).hexdigest()
        if code == 0:
            check = subprocess.run([sys.executable, 'tools/check-public.py', '--flink-line', line],
                                   cwd=ROOT, text=True, capture_output=True)
            if check.returncode:
                errors.append('Source/bundle closure check: ' + check.stdout + check.stderr)
        row = {'flink_line': line, 'flink_version': VERSIONS[line], 'command': command,
               'exit_code': code, 'seconds': round(time.monotonic() - started, 2),
               'suites': suites, 'consumer_suites': consumer, 'artifacts': artifacts,
               'shared_classes': core, 'errors': errors, 'log_sha256': hashlib.sha256(log.read_bytes()).hexdigest()}
        result['versions'].append(row)
        print(json.dumps({'flink': line, 'exit_code': code, 'tests': sum(s['tests'] for s in suites),
                          'consumer_tests': sum(s['tests'] for s in consumer), 'errors': errors}), flush=True)
    result['source_unchanged'] = source == source_digest()
    core_hashes = [row['shared_classes'] for row in result['versions']]
    result['shared_classes_identical'] = bool(core_hashes and core_hashes[0]) and all(h == core_hashes[0] for h in core_hashes)
    result['passed'] = (result['source_unchanged'] and result['shared_classes_identical']
                        and all(row['exit_code'] == 0 and not row['errors'] for row in result['versions']))
    path = destination / ('verification-' + '-'.join(lines) + '.json')
    path.write_text(json.dumps(result, indent=2) + '\n')
    print(str(path), flush=True)
    return 0 if result['passed'] else 1


if __name__ == '__main__':
    sys.exit(main())
