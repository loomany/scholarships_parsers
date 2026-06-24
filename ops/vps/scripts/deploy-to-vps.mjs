#!/usr/bin/env node
/** Deploy parser ops scripts (LF-only), cron, systemd to VPS. */
import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import os from 'node:os';

const OPS = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const SSH_KEY = path.join(os.homedir(), '.ssh', 'scholarshiptop_vps');
const VPS = 'ubuntu@213.155.22.74';
const REMOTE_BASE = '/opt/scholarshiptop-parsers';
const STAGE = path.join(OPS, '.vps-deploy');

function toLf(content) {
  return content.replace(/\r\n/g, '\n').replace(/\r/g, '\n');
}

function stageFile(rel, content) {
  const dest = path.join(STAGE, rel);
  fs.mkdirSync(path.dirname(dest), { recursive: true });
  fs.writeFileSync(dest, toLf(content), 'utf8');
}

const CRON_PARSERS = [
  'iefa',
  'scholars4dev',
  'opportunitydesk',
  'daad',
  'scholarships360',
  'wemakescholars',
  'mina7portal',
];

const REPEATER_PARSERS = [
  { slug: 'simpler-grants-gov', unit: 'scholarshiptop-parser-simpler-grants-gov' },
  { slug: 'bigfuture', unit: 'scholarshiptop-parser-bigfuture' },
  { slug: 'scholarship-america', unit: 'scholarshiptop-parser-scholarship-america' },
];

fs.rmSync(STAGE, { recursive: true, force: true });
fs.mkdirSync(STAGE, { recursive: true });

const scriptNames = fs
  .readdirSync(path.join(OPS, 'scripts'))
  .filter((f) => f.endsWith('.sh'));
for (const name of scriptNames) {
  stageFile(`scripts/${name}`, fs.readFileSync(path.join(OPS, 'scripts', name), 'utf8'));
}

for (const slug of CRON_PARSERS) {
  const wrapper = `#!/usr/bin/env bash\nset -euo pipefail\nexec /opt/scholarshiptop-parsers/scripts/run-parser-cron.sh ${slug}\n`;
  stageFile(`scripts/run-${slug}-once.sh`, wrapper);
  const cron = `SHELL=/bin/bash\nPATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin\n0 */12 * * * root /opt/scholarshiptop-parsers/scripts/run-${slug}-once.sh\n`;
  stageFile(`cron/scholarshiptop-parser-${slug}`, cron);
}

for (const { slug, unit } of REPEATER_PARSERS) {
  const wrapper = `#!/usr/bin/env bash\nset -euo pipefail\nexec /opt/scholarshiptop-parsers/scripts/run-parser-repeater.sh ${slug}\n`;
  stageFile(`scripts/run-${slug}-once.sh`, wrapper);
  const unitContent = `[Unit]
Description=ScholarshipTop parser repeater (${slug})
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/scholarshiptop-parsers/app
ExecStart=/opt/scholarshiptop-parsers/scripts/run-parser-repeater.sh ${slug}
Restart=always
RestartSec=30
StandardOutput=append:/opt/scholarshiptop-parsers/logs/${slug}-systemd.log
StandardError=append:/opt/scholarshiptop-parsers/logs/${slug}-systemd.log

[Install]
WantedBy=multi-user.target
`;
  stageFile(`systemd/${unit}.service`, unitContent);
}

const tarLocal = path.join(STAGE, 'bundle.tar');
spawnSync('tar', ['-cf', tarLocal, '-C', STAGE, 'scripts', 'cron', 'systemd'], {
  stdio: 'inherit',
});

const scp = spawnSync(
  'scp',
  ['-i', SSH_KEY, '-o', 'StrictHostKeyChecking=no', tarLocal, `${VPS}:/tmp/scholarshiptop-parsers-bundle.tar`],
  { encoding: 'utf8' }
);
if (scp.status !== 0) throw new Error(scp.stderr || 'scp failed');

const remoteCmd = `
set -euo pipefail
sudo mkdir -p ${REMOTE_BASE}/scripts ${REMOTE_BASE}/logs
cd /tmp && tar -xf scholarshiptop-parsers-bundle.tar
sudo cp -r scripts/* ${REMOTE_BASE}/scripts/
sudo chmod +x ${REMOTE_BASE}/scripts/*.sh
sudo mkdir -p /tmp/scholarshiptop-parser-cron-staging /tmp/scholarshiptop-parser-systemd-staging
sudo cp cron/* /tmp/scholarshiptop-parser-cron-staging/ 2>/dev/null || true
sudo cp systemd/* /tmp/scholarshiptop-parser-systemd-staging/ 2>/dev/null || true
rm -rf /tmp/scripts /tmp/cron /tmp/systemd /tmp/scholarshiptop-parsers-bundle.tar
echo deploy_ok scripts=$(ls ${REMOTE_BASE}/scripts | wc -l) cron_staged=$(ls /tmp/scholarshiptop-parser-cron-staging 2>/dev/null | wc -l) systemd_staged=$(ls /tmp/scholarshiptop-parser-systemd-staging 2>/dev/null | wc -l)
`;

const ssh = spawnSync(
  'ssh',
  ['-i', SSH_KEY, '-o', 'StrictHostKeyChecking=no', VPS, remoteCmd],
  { encoding: 'utf8' }
);
if (ssh.status !== 0) throw new Error(ssh.stderr || ssh.stdout);
console.log(ssh.stdout.trim());
