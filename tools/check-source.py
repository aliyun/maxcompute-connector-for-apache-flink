#!/usr/bin/env python3
"""Check every indexed source file without printing potentially sensitive values."""
import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PATTERNS = {
    'private endpoint/contact': re.compile(r'(?i)(?:[\w.-]*alibaba-inc\.(?:com|cn)|gitlab\.[\w.-]+)'),
    'local user path': re.compile(r'/(?:Users|home)/(?!runner(?:/|\b))[\w.-]+/'),
    'private IPv4 address': re.compile(r'(?<![\w.])(?:10\.(?:\d{1,3}\.){2}\d{1,3}|192\.168\.\d{1,3}\.\d{1,3}|172\.(?:1[6-9]|2\d|3[01])\.\d{1,3}\.\d{1,3}|100\.(?:6[4-9]|[7-9]\d|1[01]\d|12[0-7])\.\d{1,3}\.\d{1,3})(?![\w.])'),
    'access-key identifier': re.compile(r'LTAI[0-9A-Za-z]{12,}'),
    'private key': re.compile(r'-----BEGIN (?:RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----'),
    'GitHub token': re.compile(r'(?:gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{40,})'),
}
SENSITIVE_NAMES = {'.env', '.gitlab-ci.yml', 'candidate-manifest.json'}
SENSITIVE_SUFFIXES = {'.key', '.p12', '.pfx', '.jks', '.pem', '.log', '.jar', '.class'}


def main():
    entries = subprocess.check_output(['git', 'ls-files', '--stage', '-z'], cwd=ROOT).split(b'\0')
    findings = []
    checked = 0
    for entry in filter(None, entries):
        metadata, raw_name = entry.split(b'\t', 1)
        name = raw_name.decode('utf-8')
        path = ROOT / name
        mode = metadata.split()[0]
        if mode not in (b'100644', b'100755') or path.is_symlink():
            findings.append({'path': name, 'rule': 'unsupported file mode'})
            continue
        if (path.name in SENSITIVE_NAMES or path.name.startswith('.env.')
                or path.suffix in SENSITIVE_SUFFIXES or '.aoneci' in path.parts):
            findings.append({'path': name, 'rule': 'sensitive/generated path'})
        try:
            contents = path.read_text(encoding='utf-8')
        except UnicodeError:
            if name.startswith('licenses/') and b'\0' not in path.read_bytes():
                # Preserve the byte-exact upstream ISO-8859-1 license texts.
                contents = path.read_bytes().decode('latin-1')
            else:
                findings.append({'path': name, 'rule': 'non-text source'})
                continue
        except OSError:
            findings.append({'path': name, 'rule': 'missing or non-text source'})
            continue
        checked += 1
        for line, text in enumerate(contents.splitlines(), 1):
            for rule, pattern in PATTERNS.items():
                if pattern.search(text):
                    findings.append({'path': name, 'line': line, 'rule': rule})
    print(json.dumps({'checked_files': checked, 'findings': findings}, indent=2))
    return 1 if findings or not checked else 0


if __name__ == '__main__':
    sys.exit(main())
