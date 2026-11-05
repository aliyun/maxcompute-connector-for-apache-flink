#!/usr/bin/env python3
"""Refresh exact bundled artifacts from Maven Central, retaining pending reviews.
Run after dependency:tree. New licenses still require organizational review.
"""
import argparse
import hashlib
import io
import json
import re
import urllib.request
import subprocess
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

ROOT = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser()
parser.add_argument('--flink-line', choices=['1.16', '1.17', '1.20'], default='1.20')
ARGS = parser.parse_args()
CENTRAL = 'https://repo.maven.apache.org/maven2/'
NS = {'m': 'http://maven.apache.org/POM/4.0.0'}

def fetch(path):
    cached = Path.home() / '.m2/repository' / path
    if cached.is_file():
        data = cached.read_bytes()
    else:
        data = subprocess.check_output(['curl', '-fsSL', '--retry', '2', '--max-time', '120', CENTRAL + path])
    digest = subprocess.check_output(['curl', '-fsSL', '--retry', '2', '--max-time', '30', CENTRAL + path + '.sha1']).decode().split()[0]
    if hashlib.sha1(data).hexdigest() != digest:
        raise RuntimeError('Central checksum mismatch: ' + path)
    return data

def artifact_path(g, a, v):
    return g.replace('.', '/') + '/' + a + '/' + v + '/' + a + '-' + v

def pom_licenses(g, a, v):
    element = ET.fromstring(fetch(artifact_path(g,a,v) + '.pom'))
    licenses = element.findall('m:licenses/m:license', NS)
    if licenses:
        return [(l.findtext('m:name', '', NS), l.findtext('m:url', '', NS)) for l in licenses]
    parent = element.find('m:parent', NS)
    if parent is None:
        raise RuntimeError('No declared license for ' + ':'.join((g,a,v)))
    return pom_licenses(*(parent.findtext('m:'+k, '', NS) for k in ('groupId','artifactId','version')))

def refresh(node):
    g,a,v = (node[k] for k in ('groupId','artifactId','version'))
    classifier = node.get('classifier', '')
    prefix=artifact_path(g,a,v)
    binary=fetch(prefix + ('-'+classifier if classifier else '') + '.jar')
    source_url = CENTRAL + prefix + '-sources.jar'
    if a == 'listenablefuture' and v == '9999.0-empty-to-avoid-conflict-with-guava':
        with zipfile.ZipFile(io.BytesIO(binary)) as z:
            if any(n.endswith('.class') for n in z.namelist()):
                raise RuntimeError('Empty conflict-avoidance artifact contains classes')
        source = b''
    else:
        source=fetch(prefix + '-sources.jar')
    directory=g+'_'+a+'_'+v
    dest=ROOT/'licenses'/directory
    dest.mkdir(parents=True,exist_ok=True)
    files=[]
    for kind, data in [('binary',binary),('sources',source)]:
        if not data:
            continue
        with zipfile.ZipFile(io.BytesIO(data)) as z:
            for name in z.namelist():
                if not name.endswith('/') and re.search(r'(?i)(^|/)(license|notice|copying)([.\-_]|$)',name):
                    path=(dest/kind/name).resolve()
                    if not path.is_relative_to(dest.resolve()):
                        raise RuntimeError('Unsafe artifact member '+name)
                    path.parent.mkdir(parents=True,exist_ok=True); path.write_bytes(z.read(name))
                    files.append(str(path.relative_to(ROOT/'licenses')))
    headers={}
    with zipfile.ZipFile(io.BytesIO(source or binary)) as z:
        for name in z.namelist():
            if name.endswith('.java'):
                text=z.read(name).decode('utf-8',errors='replace')
                head=text.split('package ')[0].strip()
                if head and ('license' in head.lower() or 'copyright' in head.lower()):
                    headers.setdefault(head,[]).append(name)
    if headers:
        path=dest/'source-attributions.txt'; path.parent.mkdir(parents=True,exist_ok=True)
        path.write_text('\n\n'.join('Source paths: '+', '.join(names)+'\n'+head for head,names in headers.items())+'\n')
        files.append(str(path.relative_to(ROOT/'licenses')))
    declared=pom_licenses(g,a,v)
    if any('apache' in name.lower() for name,url in declared):
        files.append('Apache-2.0.txt')
    if not files:
        raise RuntimeError('No upstream legal texts/headers for '+g+':'+a+':'+v)
    # The exact POM preserves declarations when the jar only contains source headers.
    path=dest/'upstream.pom'; path.write_bytes(fetch(prefix+'.pom')); files.append(str(path.relative_to(ROOT/'licenses')))
    return dict(coordinate=':'.join((g,a,v)),classifier=classifier,
        sha256=hashlib.sha256(binary).hexdigest(),licenses=[n for n,u in declared],
        license_urls=[u for n,u in declared],files=files,source_url=source_url,
        source_available=bool(source),source_sha256=hashlib.sha256(source).hexdigest() if source else None,
        status='documented',notes='Exact Central binary/source legal material; redistribution approval remains separate.')

if __name__ == '__main__':
    catalog=json.loads((ROOT/'licenses/third-party.json').read_text())
    old={(n['coordinate'],n.get('classifier','')):n for n in catalog['components']}
    tree=json.loads((ROOT/'flink'/ARGS.flink_line/'target/public-dependency-tree.json').read_text())
    nodes={}
    def visit(n):
        for c in n.get('children',[]):
            if c.get('scope') in ('compile','runtime'):
                key=(':'.join(c[k] for k in ('groupId','artifactId','version')),c.get('classifier',''))
                nodes[key]=c
            visit(c)
    visit(tree)
    missing={k:n for k,n in nodes.items() if k not in old}
    with ThreadPoolExecutor(max_workers=4) as pool:
        new=list(pool.map(refresh,missing.values()))
    updated={(n['coordinate'],n['classifier']):n for n in new}
    catalog['components']=[old[k] if k in old else updated[k] for k in nodes]
    pom=ET.parse(ROOT/'pom.xml').getroot()
    catalog['flink']=pom.findtext('m:properties/m:flink.version', namespaces=NS)
    (ROOT/'licenses/third-party.json').write_text(json.dumps(catalog,indent=2)+'\n')
    print('Refreshed',len(new),'components; retained existing pending reviews.')
