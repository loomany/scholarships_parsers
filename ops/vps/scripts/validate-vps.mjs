#!/usr/bin/env node
/** Lightweight VPS parser pre-cutover validation (no parser runs). */
import { spawnSync } from 'node:child_process';
import os from 'node:os';
import path from 'node:path';

const SSH_KEY = path.join(os.homedir(), '.ssh', 'scholarshiptop_vps');
const VPS = 'ubuntu@213.155.22.74';

const remoteCmd = `
set -euo pipefail
BASE=/opt/scholarshiptop-parsers
echo "=== bash -n wrappers ==="
bash -n $BASE/scripts/*.sh
echo "=== env files ==="
sudo ls -1 $BASE/env/ | grep -c '\\.env$'
echo "=== venv ==="
test -x $BASE/venv/bin/python && $BASE/venv/bin/python --version
echo "=== app ==="
test -f $BASE/app/run_all.py && test -f $BASE/app/repeater.py
echo "=== logs writable ==="
sudo touch $BASE/logs/.write-test && sudo rm $BASE/logs/.write-test
echo "=== cron staged ==="
ls -1 /tmp/scholarshiptop-parser-cron-staging/ 2>/dev/null | wc -l
echo "=== systemd staged ==="
ls -1 /tmp/scholarshiptop-parser-systemd-staging/ 2>/dev/null | wc -l
echo "=== site health ==="
curl -sI https://scholarshiptop.com/ | head -1
curl -sI https://scholarshiptop.com/sitemap.xml | head -1
docker ps --format '{{.Names}}' | head -5 || sudo docker ps --format '{{.Names}}' | head -5
echo validate_ok
`;

const ssh = spawnSync(
  'ssh',
  ['-i', SSH_KEY, '-o', 'StrictHostKeyChecking=no', '-o', 'ConnectTimeout=20', VPS, remoteCmd],
  { encoding: 'utf8', timeout: 120000 }
);
process.stdout.write(ssh.stdout || '');
process.stderr.write(ssh.stderr || '');
process.exit(ssh.status ?? 1);
