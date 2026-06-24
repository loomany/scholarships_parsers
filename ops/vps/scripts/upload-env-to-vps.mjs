#!/usr/bin/env node
/** Upload staged parser env files to VPS (no values printed). */
import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import os from 'node:os';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../..');
const STAGING = path.join(ROOT, '.vps-env-staging');
const SSH_KEY = path.join(os.homedir(), '.ssh', 'scholarshiptop_vps');
const VPS = 'ubuntu@213.155.22.74';
const REMOTE_ENV = '/opt/scholarshiptop-parsers/env';

const files = fs.readdirSync(STAGING).filter((f) => f.endsWith('.env'));
if (!files.length) {
  console.error('no staged env files');
  process.exit(1);
}

for (const f of files) {
  const local = path.join(STAGING, f);
  const r = spawnSync(
    'scp',
    ['-i', SSH_KEY, '-o', 'StrictHostKeyChecking=no', local, `${VPS}:/tmp/${f}`],
    { encoding: 'utf8' }
  );
  if (r.status !== 0) throw new Error(r.stderr || `scp failed ${f}`);
}

const mv = files
  .map(
    (f) =>
      `sudo mv /tmp/${f} ${REMOTE_ENV}/${f} && sudo chmod 600 ${REMOTE_ENV}/${f}`
  )
  .join(' && ');

const ssh = spawnSync(
  'ssh',
  [
    '-i',
    SSH_KEY,
    '-o',
    'StrictHostKeyChecking=no',
    VPS,
    `sudo mkdir -p ${REMOTE_ENV} && sudo chmod 700 ${REMOTE_ENV} && ${mv} && echo uploaded-${files.length}`,
  ],
  { encoding: 'utf8' }
);
if (ssh.status !== 0) throw new Error(ssh.stderr || ssh.stdout);

const counts = files.map((f) => {
  const n = fs
    .readFileSync(path.join(STAGING, f), 'utf8')
    .split(/\r?\n/)
    .filter((l) => {
      const t = l.trim();
      return t && !t.startsWith('#') && t.includes('=');
    }).length;
  return { file: f, keys: n };
});

console.log(JSON.stringify({ ok: true, uploaded: files.length, counts }, null, 2));
