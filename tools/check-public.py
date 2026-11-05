#!/usr/bin/env python3
"""Check the prepared source/artifact closure. --release rejects pending legal material."""
import argparse
import json
import re
import sys
import zipfile
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument('--release', action='store_true')
parser.add_argument('--flink-line', choices=['1.16', '1.17', '1.20'], default='1.20')
args = parser.parse_args()
root = Path(__file__).resolve().parents[1]
target = root / 'flink' / args.flink_line / 'target'
manifest = json.loads((root / 'licenses/third-party.json').read_text())
tree = json.loads((target / 'public-dependency-tree.json').read_text())
expected = set()

def visit(node):
    for child in node.get('children', []):
        if child.get('scope') in ('compile', 'runtime'):
            expected.add(':'.join(child[k] for k in ('groupId', 'artifactId', 'version'))
                         + ':' + child.get('classifier', ''))
        visit(child)

visit(tree)
actual = {c['coordinate'] + ':' + c.get('classifier', '') for c in manifest['components']}
errors = []
if expected != actual:
    errors.append('Legal catalog differs from the actual bundled dependency closure: '
                  + str(sorted(expected ^ actual)))
for component in manifest['components']:
    if not component['files']:
        errors.append('No legal material: ' + component['coordinate'])
    for name in component['files']:
        path = (root / 'licenses' / name).resolve()
        if not path.is_relative_to((root / 'licenses').resolve()) or not path.is_file():
            errors.append('Missing/invalid legal file: ' + name)

source_files = (list((root / 'src').rglob('*.java')) + list((root / 'src').rglob('*.conf'))
                + list((root / 'flink').glob('*/pom.xml')) + [root / 'pom.xml']
                + list((root / 'docs').rglob('*.md')))
for path in source_files:
    text = path.read_text()
    relative = str(path.relative_to(root))
    if path.suffix == '.java' and 'licensed' not in text[:2200].lower():
        errors.append('Missing license header: ' + relative)
    if re.search(r'alibaba-inc\.com|(?:^|[^0-9])(?:10|100)\.(?:[0-9]{1,3}\.){2}[0-9]{1,3}', text):
        errors.append('Private endpoint/contact reference: ' + relative)
    if re.search(r'(?m)^\s*odps\.access\.(?:id|key)\s*=\s*(?!offline-)[^\s#]+', text):
        errors.append('Non-fixture identity in source configuration: ' + relative)

pending = [c['coordinate'] for c in manifest['components'] if c['status'] != 'documented']
if args.release and pending:
    errors.append('Redistribution review required: ' + ', '.join(pending))
for artifact in target.glob('*.jar'):
    with zipfile.ZipFile(artifact) as archive:
        if 'META-INF/LICENSE' not in archive.namelist() or 'META-INF/NOTICE' not in archive.namelist():
            errors.append('Missing project legal files in ' + artifact.name)

for jar in target.glob('*-shaded.jar'):
    with zipfile.ZipFile(jar) as archive:
        if 'META-INF/LICENSE' not in archive.namelist() or 'META-INF/NOTICE' not in archive.namelist():
            errors.append('Missing project legal files in ' + jar.name)
        embedded = json.loads(archive.read('META-INF/licenses/third-party.json'))
        if embedded != manifest:
            errors.append('Embedded catalog differs from source: ' + jar.name)
        for c in manifest['components']:
            for name in c['files']:
                if 'META-INF/licenses/' + name not in archive.namelist():
                    errors.append('Legal material absent from jar: ' + name)
result = {'flink_line': args.flink_line, 'mode': 'release' if args.release else 'candidate', 'bundled_components': len(actual),
          'pending_redistribution': pending, 'errors': errors}
print(json.dumps(result, indent=2))
sys.exit(1 if errors else 0)
