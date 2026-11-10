#!/usr/bin/env python3
"""Publish a staged deployment only after checking its tag, CI identity and coordinates."""
import base64
import json
import os
import re
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

REPOSITORY = 'aliyun/maxcompute-connector-for-apache-flink'
LINES = ('1.16', '1.17', '1.20')


def expected_coordinates(tag):
    if not re.fullmatch(r'v\d+\.\d+\.\d+', tag):
        raise ValueError('Expected an immutable vMAJOR.MINOR.PATCH release tag')
    version = tag[1:]
    return {'pkg:maven/com.aliyun.odps/flink-connector-maxcompute-parent@' + version} | {
        'pkg:maven/com.aliyun.odps/flink-connector-maxcompute@' + version + '-' + line for line in LINES}


def validate_run(run, commit):
    if (run.get('head_sha') != commit or run.get('conclusion') != 'success'
            or run.get('event') != 'workflow_dispatch'
            or run.get('path', '').split('@', 1)[0] != '.github/workflows/release.yml'):
        raise ValueError('Staging must succeed on the exact release tag commit')


def validate_status(status, deployment, expected):
    coordinates = {urllib.parse.unquote(purl.split('?', 1)[0]) for purl in status.get('purls', [])}
    if status.get('deploymentId') != deployment or coordinates != expected:
        raise ValueError('Deployment ID or four release coordinates do not match')
    return status['deploymentState']


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def main():
    if os.environ.get('GITHUB_REPOSITORY') != REPOSITORY:
        raise ValueError('Publication is restricted to the connector repository')
    tag = os.environ['RELEASE_TAG']
    deployment = os.environ['DEPLOYMENT_ID']
    run_id = os.environ['STAGING_RUN_ID']
    expected = expected_coordinates(tag)
    if not re.fullmatch(r'[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}', deployment):
        raise ValueError('Invalid deployment ID')
    if not run_id.isdecimal():
        raise ValueError('Invalid staging run ID')
    commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
    tagged = subprocess.check_output(['git', 'rev-parse', 'refs/tags/' + tag + '^{commit}'], text=True).strip()
    if commit != tagged:
        raise ValueError('Checkout is not the exact release tag')
    run = json.loads(subprocess.check_output(
        ['gh', 'api', 'repos/' + REPOSITORY + '/actions/runs/' + run_id], text=True))
    validate_run(run, commit)
    logs = subprocess.check_output(['gh', 'run', 'view', run_id, '--repo', REPOSITORY, '--log'], text=True)
    if deployment not in logs:
        raise ValueError('The deployment is absent from the verified staging run')

    token = base64.b64encode((os.environ['CENTRAL_USERNAME'] + ':'
                             + os.environ['CENTRAL_PASSWORD']).encode()).decode()
    opener = urllib.request.build_opener(NoRedirect())

    def call(path):
        request = urllib.request.Request('https://central.sonatype.com/api/v1/publisher/' + path,
                                        data=b'', headers={'Authorization': 'Bearer ' + token}, method='POST')
        try:
            with opener.open(request, timeout=60) as response:
                return response.read()
        except urllib.error.HTTPError as error:
            raise RuntimeError('Central request failed: HTTP ' + str(error.code)) from None

    receipt = {'tag': tag, 'commit': commit, 'staging_run_id': int(run_id),
               'deployment_id': deployment, 'coordinates': sorted(expected)}
    path = Path('target/central-publication.json')
    path.parent.mkdir(parents=True, exist_ok=True)
    status = json.loads(call('status?id=' + deployment))
    state = validate_status(status, deployment, expected)
    receipt['prepublish_state'] = state
    path.write_text(json.dumps(receipt, indent=2) + '\n')
    if state == 'VALIDATED':
        call('deployment/' + deployment)
    elif state not in ('PUBLISHING', 'PUBLISHED'):
        raise ValueError('Deployment is not validated for publication: ' + state)

    deadline = time.monotonic() + 600
    while True:
        status = json.loads(call('status?id=' + deployment))
        state = validate_status(status, deployment, expected)
        receipt['deployment_state'] = state
        path.write_text(json.dumps(receipt, indent=2) + '\n')
        print(json.dumps({'deployment_id': deployment, 'deployment_state': state}), flush=True)
        if state == 'PUBLISHED':
            return
        if state not in ('VALIDATED', 'PUBLISHING') or time.monotonic() >= deadline:
            raise RuntimeError('Publication not complete; inspect this deployment before retrying')
        time.sleep(5)


if __name__ == '__main__':
    main()
