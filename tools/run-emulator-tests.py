#!/usr/bin/env python3
"""Run connector tests on a disposable loopback-only emulator; fail on missing Docker."""
import argparse
import json
import subprocess
import tempfile
import time
import urllib.request
from pathlib import Path

root = Path(__file__).resolve().parents[1]
image_config = json.loads((root / 'tools/emulator-image.json').read_text())
parser = argparse.ArgumentParser()
parser.add_argument('--flink-line', choices=['1.16', '1.17', '1.20'], default='1.20')
parser.add_argument('--image', default=image_config['image'], help='Emulator image override (default: released digest in tools/emulator-image.json)')
parser.add_argument('--platform', default=image_config['platform'])
parser.add_argument('--build-source', action='store_true', help='Build the locked public Emulator source with configurable admission (no image publication)')
parser.add_argument('--max-concurrent-requests', type=int, help='Candidate Emulator admission capacity; requires support for this flag')
parser.add_argument('--clean', action='store_true', help='Discard previous coverage and test output before verification')
parser.add_argument('maven_args', nargs='*')
args = parser.parse_args()
if args.max_concurrent_requests is not None and not 1 <= args.max_concurrent_requests <= 16384:
    parser.error('--max-concurrent-requests must be between 1 and 16384')
image_ref = args.image
if args.build_source and args.image != image_config['image']:
    parser.error('--build-source cannot be combined with --image')
if args.build_source:
    image_ref = 'maxcompute-emulator:flink-ci-' + image_config['build_commit'][:12]
    with tempfile.TemporaryDirectory(prefix='flink-emulator-source-') as directory:
        subprocess.run(['git', 'init', '-q', directory], check=True)
        subprocess.run(['git', '-C', directory, 'fetch', '--depth=1',
            image_config['build_repository'], image_config['build_commit']], check=True)
        subprocess.run(['git', '-C', directory, 'checkout', '--detach', '-q', 'FETCH_HEAD'], check=True)
        actual = subprocess.check_output(['git', '-C', directory, 'rev-parse', 'HEAD'], text=True).strip()
        if actual != image_config['build_commit']:
            raise SystemExit('Emulator source does not match the locked commit')
        subprocess.run(['docker', 'build', '--platform', args.platform, '--target', 'release',
            '-t', image_ref, directory], check=True)
# A Docker-loaded archive has no registry RepoDigest. Reuse it only when its
# content-addressed config ID exactly matches the published image lock.
if not args.build_source and args.image == image_config['image'] and image_config.get('image_id'):
    cached = subprocess.run(['docker', 'image', 'inspect', image_config['image_id']],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if cached.returncode == 0:
        image_ref = image_config['image_id']
capacity_args = [] if args.max_concurrent_requests is None else ['--max-concurrent-requests', str(args.max_concurrent_requests)]
container = subprocess.check_output(['docker', 'run', '-d', '--platform', args.platform,
    '-p', '127.0.0.1::8080', image_ref, '--listen', '0.0.0.0:8080', '--max-sessions', '4096', '--test-mode', '--test-network', *capacity_args], text=True).strip()
ports = None
try:
    ports = json.loads(subprocess.check_output(['docker', 'inspect', container], text=True))
    port = ports[0]['NetworkSettings']['Ports']['8080/tcp'][0]['HostPort']
    endpoint = 'http://127.0.0.1:' + port
    for attempt in range(60):
        try:
            with urllib.request.urlopen(endpoint + '/readyz', timeout=2) as response:
                if response.status == 200:
                    break
        except (OSError, TimeoutError):
            time.sleep(1)
    else:
        raise SystemExit('Emulator did not become ready')
    result = subprocess.run(['mvn', '-s', 'tools/maven-central-settings.xml', '-gs',
        'tools/maven-central-settings.xml', '-B', '-ntp', '-pl', 'flink/' + args.flink_line, '-am', '-Pemulator',
        '-Demulator.endpoint=' + endpoint, *(['clean'] if args.clean else []), 'verify', *args.maven_args], cwd=root)
    raise SystemExit(result.returncode)
finally:
    log = root / 'flink' / args.flink_line / 'target/emulator.log'
    log.parent.mkdir(parents=True, exist_ok=True)
    if ports is not None:
        identity = {'requested_image': image_ref if args.build_source else args.image, 'runtime_image': image_ref, 'platform': args.platform,
                    'image_id': ports[0]['Image'], 'max_concurrent_requests': args.max_concurrent_requests}
        if args.build_source:
            identity.update({'source_repository': image_config['build_repository'],
                             'source_commit': image_config['build_commit'], 'source_built': True})
        elif args.image == image_config['image']:
            identity.update({'version': image_config['version'],
                             'source_commit': image_config['source_commit']})
        (root / 'flink' / args.flink_line / 'target/emulator-image.json').write_text(json.dumps(identity, indent=2) + '\n')
    with log.open('wb') as output:
        subprocess.run(['docker', 'logs', container], stdout=output, stderr=subprocess.STDOUT)
    subprocess.run(['docker', 'rm', '-f', container], check=True, stdout=subprocess.DEVNULL)
