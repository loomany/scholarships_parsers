#!/usr/bin/env node
/** Enable staged parser cron + systemd on VPS after Railway down. */
import { spawnSync } from 'node:child_process';
import os from 'node:os';
import path from 'node:path';

const SSH_KEY = path.join(os.homedir(), '.ssh', 'scholarshiptop_vps');
const VPS = 'ubuntu@213.155.22.74';

const remoteCmd = `
set -euo pipefail
if ls /tmp/scholarshiptop-parser-cron-staging/* 1>/dev/null 2>&1; then
  sudo cp /tmp/scholarshiptop-parser-cron-staging/* /etc/cron.d/
  sudo chmod 644 /etc/cron.d/scholarshiptop-parser-*
fi
if ls /tmp/scholarshiptop-parser-systemd-staging/*.service 1>/dev/null 2>&1; then
  sudo cp /tmp/scholarshiptop-parser-systemd-staging/*.service /etc/systemd/system/
  sudo systemctl daemon-reload
  for u in /tmp/scholarshiptop-parser-systemd-staging/*.service; do
  name=$(basename "$u")
  sudo systemctl enable --now "$name"
  done
fi
echo "=== cron ==="
ls -l /etc/cron.d/ | grep scholarshiptop-parser || true
echo "=== systemd ==="
systemctl list-units --type=service --all | grep scholarshiptop-parser || true
echo enable_ok
`;

const ssh = spawnSync(
  'ssh',
  ['-i', SSH_KEY, '-o', 'StrictHostKeyChecking=no', VPS, remoteCmd],
  { encoding: 'utf8', timeout: 120000 }
);
process.stdout.write(ssh.stdout || '');
process.stderr.write(ssh.stderr || '');
process.exit(ssh.status ?? 1);
