#!/usr/bin/env node
/** Bulk railway down for all parser services (no delete). */
import { execSync } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../..');
const SERVICES = [
  'ieFA ',
  'scholars4dev',
  'opportunitydesk',
  'DAAD',
  'simpler_grants_gov',
  'bigfuture',
  'scholarship_america',
  'scholarships360',
  'wemakescholars',
  'mina7portal',
];

const results = [];
for (const s of SERVICES) {
  const name = s.trim();
  try {
    execSync(`railway down -s ${JSON.stringify(s)} -y`, {
      cwd: ROOT,
      encoding: 'utf8',
      stdio: ['pipe', 'pipe', 'pipe'],
    });
    results.push({ service: name, ok: true });
  } catch (e) {
    results.push({ service: name, ok: false, err: String(e.stderr || e.message).slice(0, 200) });
  }
}
console.log(JSON.stringify({ ok: results.every((r) => r.ok), results }, null, 2));
process.exit(results.every((r) => r.ok) ? 0 : 1);
