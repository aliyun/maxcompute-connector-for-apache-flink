#!/usr/bin/env python3
"""Export a prepared Git commit to a new candidate directory, with no Git history."""
import argparse
import hashlib
import io
import json
import re
import subprocess
import tarfile
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument('--ref', default='HEAD')
parser.add_argument('--output', required=True)
args = parser.parse_args()
root = Path(__file__).resolve().parents[1]
output = Path(args.output).resolve()
if output.exists():
    raise SystemExit('Output must not exist; preserve any existing candidate.')
commit = subprocess.check_output(['git', '-C', str(root), 'rev-parse', args.ref + '^{commit}'], text=True).strip()
files = subprocess.check_output(['git', '-C', str(root), 'ls-tree', '-r', '--name-only', commit], text=True).splitlines()
selected = []
for name in files:
    allowed = name in ('pom.xml', 'README.md', 'LICENSE', 'NOTICE', 'CONTRIBUTING.md', '.gitignore') or name.startswith((
        'src/main/', 'src/test/', 'flink/', 'licenses/', 'tools/', '.github/workflows/')) or name in ('docs/testing.md', 'docs/releasing.md', 'docs/quickstart.md', 'docs/configuration.md', 'docs/compatibility.md', 'docs/acceptance.md', 'docs/multi-version.md')
    if not allowed:
        continue
    if name.startswith('src/test/java/'):
        contents = subprocess.check_output(['git', '-C', str(root), 'show', commit + ':' + name], text=True)
        if ('@org.junit.experimental.categories.Category' in contents
                or name.endswith(('/TestUtils.java', '/OdpsCatalogUtils.java', '/ReadWriteTestCase.java'))
                or not re.search(r'^\s*package\s+[\w.]+\s*;', contents, re.MULTILINE)):
            continue
    selected.append(name)
if not all(name in selected for name in ('LICENSE', 'NOTICE', 'licenses/third-party.json', 'tools/check-public.py')):
    raise SystemExit('Selected commit is not a prepared release candidate.')
archive = subprocess.check_output(['git', '-C', str(root), 'archive', commit, '--', *selected])
output.mkdir(parents=True)
with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
    for member in tar.getmembers():
        destination = (output / member.name).resolve()
        if not destination.is_relative_to(output) or member.issym() or member.islnk():
            raise SystemExit('Unsafe archive entry: ' + member.name)
        if member.isdir():
            destination.mkdir(parents=True, exist_ok=True)
        elif member.isfile():
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(tar.extractfile(member).read())
            destination.chmod(member.mode)
hashes = {name: hashlib.sha256((output / name).read_bytes()).hexdigest() for name in selected}
receipt = {'source_commit': commit, 'files': hashes, 'excluded': sorted(set(files) - set(selected)),
           'public_push': False, 'release_authorized': False}
(output / 'candidate-manifest.json').write_text(json.dumps(receipt, indent=2) + '\n')
print(json.dumps({'commit': commit, 'output': str(output), 'selected_files': len(selected),
                  'excluded_files': len(receipt['excluded'])}, indent=2))
