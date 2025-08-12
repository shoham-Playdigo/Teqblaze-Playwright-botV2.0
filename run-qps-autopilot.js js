/**
 * Teqblaze QPS Autopilot (DRY-RUN by default)
 * --------------------------------------------------
 * - Fetches last full hour (UTC) DSP stats via API
 * - Filters to allowed vendors / ids
 * - Computes QPS = bid_requests / 3600
 * - Logs plan + (optionally) changes QPS limit in UI via Playwright
 *
 * Auth:
 *  - In CI: set repo secrets TEQ_EMAIL, TEQ_PASSWORD
 *  - Locally: either env vars or a "login" file with two lines (email\npassword)
 */

const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright');

// --------- config from env ---------
const BASE = process.env.TEQ_BASE_URL || 'https://ssp.playdigo.com';
const DRY_RUN = (process.env.DRY_RUN || 'true').toLowerCase() === 'true';
const HEADLESS = (process.env.HEADLESS || 'true').toLowerCase() === 'true';
const DEBUG_API = (process.env.DEBUG_API || '0') === '1';

const ALLOW_DSPS = (process.env.ALLOW_DSPS || '')
  .split(',')
  .map(s => s.trim())
  .filter(Boolean);                          // e.g. "FreeWheel, Loop-Me, Sovrn"

const ALLOW_DSP_IDS = (process.env.ALLOW_DSP_IDS || '')
  .split(',')
  .map(s => s.trim())
  .filter(Boolean);                          // e.g. "15,400,1234"

// limits / rules
const MAX_LIMIT = 30000;
const MIN_LIMIT = 500;
const LOW_SRCPM_HARD = 0.005;                // set 100
const LOW_SRCPM_SOFT = 0.25;                 // decrease 15%
const HIGH_SRCPM = 0.55;                     // increase 15% if QPS >= 70% limit
const INCREASE_FACTOR = 1.15;
const DECREASE_FACTOR = 0.85;

// --------- utilities ---------
function utcNow() { return new Date(new Date().toISOString()); }
function pad(n) { return n.toString().padStart(2, '0'); }

function lastFullHourUTC() {
  const d = utcNow();
  d.setUTCMinutes(0, 0, 0);
  d.setUTCHours(d.getUTCHours() - 1);
  const Y = d.getUTCFullYear();
  const M = pad(d.getUTCMonth() + 1);
  const D = pad(d.getUTCDate());
  const H = pad(d.getUTCHours());
  return {
    date: `${Y}-${M}-${D}`,
    hourStamp: `${Y}-${M}-${D} ${H}:00:00`,
    hour: H,
    isoTag: `${Y}-${M}-${D}T${H}-00-00Z`
  };
}

function ensureDir(p) { fs.mkdirSync(p, { recursive: true }); }

function csv(rows) {
  if (!rows.length) return '';
  const keys = Object.keys(rows[0]);
  const esc = v => (v == null ? '' :
    String(v).replace(/"/g, '""'));
  const lines = [keys.join(',')];
  for (const r of rows) {
    lines.push(keys.map(k => `"${esc(r[k])}"`).join(','));
  }
  return lines.join('\n');
}

function readLogin() {
  const email = process.env.TEQ_EMAIL;
  const password = process.env.TEQ_PASSWORD;
  if (email && password) return { email, password };

  const f = path.resolve('login');
  if (fs.existsSync(f)) {
    const [e, p] = fs.readFileSync(f, 'utf8').split(/\r?\n/);
    if (e && p) return { email: e.trim(), password: p.trim() };
  }
  throw new Error('Missing credentials: set TEQ_EMAIL/TEQ_PASSWORD or create a "login" file.');
}

// --------- API helpers ---------
async function apiFetch(url, opts = {}) {
  if (DEBUG_API) console.log('API GET:', url);
  const res = await fetch(url, opts);
  const text = await res.text();
  if (DEBUG_API) console.log(' ->', res.status, text.slice(0, 200));
  let json = {};
  try { json = JSON.parse(text); } catch { /* not json */ }
  if (!res.ok) {
    throw new Error(`API ${res.status}: ${url}\n${text}`);
  }
  return json;
}

async function createToken(base, email, password) {
  const url = `${base}/api/create_token`;
  if (DEBUG_API) console.log('API POST:', url);
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'accept': 'application/json' },
    body: JSON.stringify({ email, password })
  });
  const json = await res.json();
  if (!res.ok || !json.token) throw new Error(`Token error: ${res.status} ${JSON.stringify(json)}`);
  return json.token;
}

async function fetchHourly(base, token, date) {
  // day_group=hour; date=YYYY-MM-DD; attributes + metrics; NO time_zone param (some tenants reject it)
  const params = new URLSearchParams();
  params.set('day_group', 'hour');
  params.set('date', date);
  params.append('attribute[]', 'company_dsp');
  params.append('attribute[]', 'dsp_id');
  params.append('metric[]', 'bid_requests');
  params.append('metric[]', 'dsp_srcpm');
  params.set('limit', '2000');
  const url = `${base}/api/${token}/adx-report?${params.toString()}`;
  const json = await apiFetch(url);
  return json.data || [];
}

// --------- Playwright helpers ---------
async function loginUI(page, email, password) {
  await page.goto(`${BASE}/login`, { waitUntil: 'networkidle' });
  await page.getByRole('textbox', { name: 'Email' }).fill(email);
  await page.getByRole('textbox', { name: 'Password' }).fill(password);
  await page.getByRole('button', { name: 'Sign In' }).click();
  await page.waitForLoadState('networkidle');
}

async function readCurrentLimit(page) {
  // Prefer the input id if present; fall back to visible number parsing.
  const field = page.locator('#max_qps_limit');
  if (await field.count()) {
    const val = (await field.inputValue() || '').replace(/,/g, '');
    const n = parseInt(val, 10);
    if (!Number.isNaN(n)) return n;
  }
  // fallback: look for any number near "QPS limit"
  const txt = await page.locator('body').innerText();
  const m = txt.match(/QPS\s*limit[^0-9]*([\d,]+)/i);
  if (m) {
    const n = parseInt(m[1].replace(/,/g, ''), 10);
    if (!Number.isNaN(n)) return n;
  }
  throw new Error('Could not read current QPS limit');
}

async function setLimitAndSave(page, newLimit) {
  await page.locator('#max_qps_limit').fill(String(newLimit));
  // Use the plain Save button (avoid "Save & Exit")
  await page.getByRole('button', { name: /^Save$/ }).click();
  await page.waitForLoadState('networkidle');
}

// --------- decision logic ---------
function decideAction({ srcpm, qps, currentLimit }) {
  // rule order: hard low -> soft low -> high
  if (srcpm < LOW_SRCPM_HARD) {
    return { action: 'set', newLimit: 100, rule: 'srcpm<0.005 -> set 100' };
  }
  if (srcpm < LOW_SRCPM_SOFT) {
    const dec = Math.max(MIN_LIMIT, Math.floor(currentLimit * DECREASE_FACTOR));
    return { action: 'decrease', newLimit: dec, rule: 'srcpm<0.25 -> -15%' };
  }
  if (srcpm > HIGH_SRCPM && qps >= 0.7 * currentLimit) {
    const inc = Math.min(MAX_LIMIT, Math.ceil(currentLimit * INCREASE_FACTOR));
    return { action: 'increase', newLimit: inc, rule: 'srcpm>0.55 & qps>=70% -> +15%' };
  }
  return { action: 'none', newLimit: currentLimit, rule: 'no-change' };
}

// --------- main ---------
(async () => {
  ensureDir('output');

  const creds = readLogin();
  const { date, hourStamp, isoTag } = lastFullHourUTC();
  console.log(`Fetching hourly report for last full hour UTC → date=${date}, hour=${hourStamp}`);

  // 1) token + report
  const token = await createToken(BASE, creds.email, creds.password);
  const all = await fetchHourly(BASE, token, date);
  const rowsAtHour = all.filter(r => r.date === hourStamp);

  // prepare snapshot
  const snapshot = rowsAtHour.map(r => ({
    hour_utc: hourStamp,
    company_dsp: r.company_dsp,
    dsp_id: r.dsp_id,
    bid_requests: r.bid_requests,
    dsp_srcpm: r.dsp_srcpm,
    qps: (r.bid_requests || 0) / 3600
  }));

  // filter by vendors / ids
  const allowSet = new Set(ALLOW_DSPS.map(s => s.toLowerCase()));
  const idSet = new Set(ALLOW_DSP_IDS);
  let targets = snapshot.filter(r =>
    (allowSet.size ? allowSet.has(String(r.company_dsp || '').toLowerCase()) : true) ||
    (idSet.size ? idSet.has(String(r.dsp_id)) : false)
  );

  // If neither list supplied, act on all (or comment to require one)
  // if (!ALLOW_DSPS.length && !ALLOW_DSP_IDS.length) targets = [];

  // write snapshot
  const snapFile = path.join('output', `last-hour-snapshot-${isoTag}.csv`);
  fs.writeFileSync(snapFile, csv(snapshot));
  console.log(`Snapshot → ${snapFile} (${snapshot.length} rows)`);

  if (!targets.length) {
    console.log('No rows matched filters. Exiting.');
    return;
  }

  // 2) login + iterate DSP items
  const browser = await chromium.launch({ headless: HEADLESS, args: ['--no-sandbox', '--disable-dev-shm-usage'] });
  const context = await browser.newContext();
  const page = await context.newPage();
  await loginUI(page, creds.email, creds.password);

  const actions = [];

  for (const r of targets) {
    const url = `${BASE}/ad-exchange/dsp/${r.dsp_id}/edit`;
    const qps = Number(r.qps) || 0;
    const srcpm = Number(r.dsp_srcpm) || 0;

    try {
      await page.goto(url, { waitUntil: 'networkidle' });
      await page.waitForSelector('#max_qps_limit', { timeout: 15000 });
      const currentLimit = await readCurrentLimit(page);

      const { action, newLimit, rule } = decideAction({ srcpm, qps, currentLimit });
      const changed = action !== 'none' && newLimit !== currentLimit;

      if (!DRY_RUN && changed) {
        await setLimitAndSave(page, newLimit);
      }

      actions.push({
        hour_utc: r.hour_utc,
        company_dsp: r.company_dsp,
        dsp_id: r.dsp_id,
        srcpm: srcpm.toFixed(3),
        bid_requests: r.bid_requests,
        qps: qps.toFixed(2),
        current_limit: currentLimit,
        proposed_limit: newLimit,
        action: changed ? action : 'none',
        rule,
        mode: DRY_RUN ? 'dry-run' : 'live',
        url
      });

      console.log(`→ ${r.company_dsp} [${r.dsp_id}] srcpm=${srcpm} qps=${qps.toFixed(2)} current=${currentLimit} ${changed ? `→ ${action} to ${newLimit}` : '→ no change'} (${rule})`);

    } catch (err) {
      console.error(`Failed on DSP ${r.dsp_id} (${r.company_dsp}):`, err.message);
      actions.push({
        hour_utc: r.hour_utc,
        company_dsp: r.company_dsp,
        dsp_id: r.dsp_id,
        srcpm: srcpm,
        bid_requests: r.bid_requests,
        qps: qps,
        current_limit: '',
        proposed_limit: '',
        action: 'error',
        rule: err.message,
        mode: DRY_RUN ? 'dry-run' : 'live',
        url
      });
    }
  }

  await browser.close();

  const actFile = path.join('output', `actions-${isoTag}.csv`);
  fs.writeFileSync(actFile, csv(actions));
  console.log(`${DRY_RUN ? '✅ Actions (DRY_RUN=true)' : '✅ Actions (LIVE)'} → ${actFile}`);
})().catch(err => {
  console.error('Run failed:', err);
  process.exit(1);
});
