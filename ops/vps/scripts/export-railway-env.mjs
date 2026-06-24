#!/usr/bin/env node
/** Export Railway parser service env to files. Never prints values. */
import { execSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '../..');
const OUT = process.env.PARSER_ENV_OUT || path.join(ROOT, '.vps-env-staging');

const SERVICES = [
  { railway: 'ieFA ', file: 'iefa.env' },
  { railway: 'scholars4dev', file: 'scholars4dev.env' },
  { railway: 'opportunitydesk', file: 'opportunitydesk.env' },
  { railway: 'DAAD', file: 'daad.env' },
  { railway: 'simpler_grants_gov', file: 'simpler-grants-gov.env' },
  { railway: 'bigfuture', file: 'bigfuture.env' },
  { railway: 'scholarship_america', file: 'scholarship-america.env' },
  { railway: 'scholarships360', file: 'scholarships360.env' },
  { railway: 'wemakescholars', file: 'wemakescholars.env' },
  { railway: 'mina7portal', file: 'mina7portal.env' },
];

function toEnvLine(key, value) {
  const v = String(value ?? '');
  if (/[\n\r"\\]/.test(v)) {
    return `${key}="${v.replace(/\\/g, '\\\\').replace(/"/g, '\\"').replace(/\n/g, '\\n')}"`;
  }
  return `${key}=${v}`;
}

function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const summary = [];
  for (const { railway, file } of SERVICES) {
    const raw = execSync(`railway variable list -s ${JSON.stringify(railway)} --json`, {
      encoding: 'utf8',
      cwd: ROOT,
    });
    const vars = JSON.parse(raw);
  const filtered = Object.fromEntries(
    Object.entries(vars).filter(([k]) => !k.startsWith('RAILWAY_'))
  );
    const lines = Object.keys(filtered)
      .sort()
      .map((k) => toEnvLine(k, filtered[k]));
    const outPath = path.join(OUT, file);
    fs.writeFileSync(outPath, `${lines.join('\n')}\n`, 'utf8');
    summary.push({ railway: railway.trim(), file, keys: Object.keys(filtered).length });
  }
  console.log(JSON.stringify({ ok: true, out: OUT, summary }, null, 2));
}

main();
