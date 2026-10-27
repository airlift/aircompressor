import json
import os
from pathlib import Path
import subprocess
import tempfile

run_url = os.environ['EVIDENCE_RUN_URL']
source_head = os.environ['SOURCE_HEAD']
success = os.environ['EVIDENCE_JOB_STATUS'] == 'success'
repository = os.environ['GITHUB_REPOSITORY']


def post(endpoint, payload):
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json') as body:
        json.dump(payload, body)
        body.flush()
        result = subprocess.run(['gh', 'api', '--method', 'POST', endpoint, '--input', body.name],
                                check=True, text=True, capture_output=True)
        return json.loads(result.stdout)


if success:
    summary = Path('gc-evidence-results/summary.md').read_text()
    report = ('@raunaqmorarka The focused GC cutoff comparison has completed. '
              f'[Run, reproducible source, and raw-log artifact]({run_url}). '
              'Production cutoffs remain unchanged; these are measurements for review, '
              'not an automatic acceptance of the proposed limits.\n\n' + summary)
    comment = post('repos/airlift/aircompressor/issues/396/comments', {'body': report})
    print('Published evidence:', comment['html_url'])

post(f'repos/{repository}/statuses/{source_head}', {
    'state': 'success' if success else 'failure',
    'context': 'gc-cutoff-evidence',
    'target_url': run_url,
    'description': 'GC cutoff comparison; raw logs and summary in run artifacts',
})
