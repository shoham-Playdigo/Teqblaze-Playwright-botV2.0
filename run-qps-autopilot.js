'use strict';
try { require('dotenv').config(); } catch {}

const fs = require('fs');
const path = require('path');
const https = require('https');
const { chromium } = require('playwright');

// ====== ENV / constants ======
const BASE_URL  = (process.env.TEQ_BASE_URL || 'https://ssp.playdigo.com').replace(/\/+$/, '');
const HEADLESS  = String(process.env.HEADLESS || 'false').toLowerCase() !== 'false'; // default visible
const DRY_RUN   = String(process.env.DRY_RUN  || 'true').toLowerCase() === 'true';
const DEBUG_API = String(process.env.DEBUG_API|| '0') === '1';

// ALLOW by names (or ALL), and EXCLUDE by names/IDs
const RAW_ALLOW = (process.env.ALLOW_DSPS || 'ALL').trim();
const ALLOW_MODE_ALL = RAW_ALLOW === '' || /^all$/i.test(RAW_ALLOW) || RAW_ALLOW === '*';
const ALLOW_DSPS = ALLOW_MODE_ALL ? [] : RAW_ALLOW.split(',').map(s => s.trim()).filter(Boolean);

const EXCLUDE_DSPS = (process.env.EXCLUDE_DSPS || 'Magnite')
  .split(',').map(s => s.trim()).filter(Boolean);
const EXCLUDE_DSP_IDS = (process.env.EXCLUDE_DSP_IDS || '')
  .split(',').map(s => s.trim()).filter(Boolean);

// Optional allow-list by IDs
const ALLOW_DSP_IDS = (process.env.ALLOW_DSP_IDS || '')
  .split(',').map(s => s.trim()).filter(Boolean);

// Business rules
const CAP_MAX         = 30000;
const FLOOR_MIN       = 500;
const CRITICAL_SET_TO = 100;
const SAT_QPS_PCT     = 0.70; // ≥70% of limit is “saturated”
const UP_PCT          = 1.15; // +15%
const DOWN_PCT        = 0.85; // −15%

// New: timeouts/retries & behavior toggles
const ENDPOINT_TIMEOUT_MS = Number(process.env.ENDPOINT_TIMEOUT_MS || 20000);
const FIELD_TIMEOUT_MS    = Number(process.env.FIELD_TIMEOUT_MS    || 15000);
const ENDPOINT_RETRIES    = Number(process.env.ENDPOINT_RETRIES    || 2);
const FAIL_ON_ITEM_ERROR  = String(process.env.FAIL_ON_ITEM_ERROR  || 'false') === 'true';

const OUT_DIR = path.join(process.cwd(), 'output');
function ensureOutDir(){ if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR,{recursive:true}); }

// CSV escape helper (define ONCE)
const csvEsc = (v) => `"${String(v ?? '').replace(/"/g, '""')}"`;

const normName = (s) => String(s||'').toLowerCase().replace(/[^a-z0-9]/g,''); // remove spaces/punct
const num = s => Number(String(s ?? '').replace(/[^\d.-]/g, ''));

// ====== tiny utils ======
function lastFullHourInfo() {
  const d = new Date(Date.now() - 60 * 60 * 1000); // N-1 hour UTC
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  const day = String(d.getUTCDate()).padStart(2, '0');
  const hh = String(d.getUTCHours()).padStart(2, '0');
  return { ymd: `${y}-${m}-${day}`, hourKey: `${y}-${m}-${day} ${hh}:00` };
}
function readLogin(){
  const raw = fs.readFileSync('login','utf8').replace(/\r/g,'').trim();
  const [email,password] = raw.split('\n').map(s=>s.trim());
  if(!email||!password) throw new Error('login file must have 2 lines: email then password');
  return {email,password};
}
function httpsRequest(urlStr, { method='GET', headers={}, body=null }={}){
  return new Promise((resolve,reject)=>{
    const u = new URL(urlStr);
    const req = https.request({
      protocol: u.protocol, hostname: u.hostname, port: u.port || 443,
      path: u.pathname + u.search, method,
      headers: { 'accept-encoding': 'identity', ...headers },
      insecureHTTPParser: true
    }, res => {
      const chunks=[]; res.on('data',d=>chunks.push(d));
      res.on('end',()=>resolve({ ok:res.statusCode>=200&&res.statusCode<300, status:res.statusCode, text:Buffer.concat(chunks).toString('utf8'), headers:res.headers }));
    });
    req.on('error', reject);
    if (body) req.write(body);
    req.end();
  });
}
async function createToken(email,password){
  const url = `${BASE_URL}/api/create_token`;
  // Try JSON
  let res = await httpsRequest(url,{ method:'POST', headers:{'content-type':'application/json',accept:'application/json'}, body:JSON.stringify({email,password}) });
  if(!res.ok){
    // Fallback to form-encoded
    res = await httpsRequest(url,{ method:'POST', headers:{'content-type':'application/x-www-form-urlencoded',accept:'application/json'}, body:new URLSearchParams({email,password}).toString() });
  }
  if(!res.ok) throw new Error(`create_token failed ${res.status}: ${res.text.slice(0,200)}`);
  try { const j = JSON.parse(res.text); const t = j.token || j.authenticator || j.key || j?.data?.token; if (t) return t; } catch {}
  return res.text.trim().replace(/["']/g,'');
}
function parseRows(text){
  try {
    const j=JSON.parse(text);
    if (Array.isArray(j)) return j;
    if (Array.isArray(j.data)) return j.data;
    if (Array.isArray(j.rows)) return j.rows;
  } catch {}
  return null;
}
function findKey(obj, candidates) {
  const norm = s => String(s).toLowerCase().replace(/[\s_-]+/g, '');
  const map = {}; for (const k of Object.keys(obj || {})) map[norm(k)] = k;
  for (const want of candidates) { const hit = map[norm(want)]; if (hit) return hit; }
  return null;
}
function extractHourUTC(row) {
  if (typeof row.date === 'string' && /^\d{4}-\d{2}-\d{2} \d{2}:/.test(row.date)) return row.date.slice(0, 13) + ':00';
  for (const v of Object.values(row)) {
    if (typeof v === 'string') {
      const m = v.match(/^(\d{4}-\d{2}-\d{2})[ T](\d{2})/);
      if (m) return `${m[1]} ${m[2]}:00`;
    }
  }
  const Y=row.Y??row.year, m=row.m??row.month, d=row.d??row.day, H=row.H??row.hour??row.hr??row.h;
  if (Y && m && d && H) return `${String(Y).padStart(4,'0')}-${String(m).padStart(2,'0')}-${String(d).padStart(2,'0')} ${String(H).padStart(2,'0')}:00`;
  if (row.date && (row.hour || row.hr || row.h)) {
    const day = String(row.date).slice(0,10); const hh = String(row.hour ?? row.hr ?? row.h).padStart(2,'0');
    if (/^\d{4}-\d{2}-\d{2}$/.test(day)) return `${day} ${hh}:00`;
  }
  return '';
}

// ====== API: fetch N-1 hour rows ======
async function fetchLastHourRows({email,password}){
  const { ymd, hourKey } = lastFullHourInfo();
  const token = await createToken(email,password);

  const qs = new URLSearchParams({ day_group:'hour', date: ymd, limit: '2000' });
  qs.append('attribute[]','company_dsp');
  qs.append('attribute[]','dsp_id');
  qs.append('metric[]','bid_requests');
  qs.append('metric[]','dsp_srcpm');

  const url = `${BASE_URL}/api/${encodeURIComponent(token)}/adx-report?${qs.toString()}`;
  if (DEBUG_API) console.log('API GET:', url);

  const res = await httpsRequest(url, { headers: { accept: 'application/json' } });
  if (!res.ok) throw new Error(`adx-report failed ${res.status}: ${res.text.slice(0,200)}`);
  const rows = parseRows(res.text) || [];
  if (!rows.length) return { hourKey, items: [] };

  const sample = rows[0];
  const kCompany = findKey(sample, ['company_dsp','company_ssp','dsp_company','company','dsp_name']);
  const kId      = findKey(sample, ['dsp_id','id','dspId']);
  const kReq     = findKey(sample, ['bid_requests','bids','requests']);
  const kSrcpm   = findKey(sample, ['dsp_srcpm','srcpm','eCPM','rpm']);

  const normalized = rows.map(r => ({
    hour_utc:   extractHourUTC(r),
    dsp_company: kCompany ? r[kCompany] : '',
    dsp_id:      kId ? r[kId] : '',
    bid_requests: kReq ? Number(r[kReq]) : NaN,
    dsp_srcpm:   kSrcpm ? Number(r[kSrcpm]) : NaN
  }));

  // Filter to last full hour and compute QPS
  const last = normalized.filter(r => r.hour_utc === hourKey)
                        .map(r => ({ ...r, qps: Number.isFinite(r.bid_requests) ? r.bid_requests/3600 : NaN }));
  return { hourKey, items: last };
}

// ====== Decision rules ======
function decideNewLimit({srcpm,qps,current}){
  if (srcpm < 0.005) return { action:'set',      newLimit:CRITICAL_SET_TO,                                  reason:'srcpm<0.005→set100' };
  if (srcpm < 0.25)  return { action:'decrease', newLimit:Math.max(FLOOR_MIN, Math.floor(current*DOWN_PCT)), reason:'srcpm<0.25→-15%'   };
  if (srcpm > 0.35 && qps >= SAT_QPS_PCT*current)
                     return { action:'increase', newLimit:Math.min(CAP_MAX, Math.round(current*UP_PCT)),     reason:'srcpm>0.35 & qps>=70%→+15%' };
  return { action:'hold', newLimit:current, reason:'no-change' };
}

// ====== Playwright helpers ======
async function uiLogin(page,email,password){
  await page.goto(`${BASE_URL}/login`,{waitUntil:'domcontentloaded', timeout: ENDPOINT_TIMEOUT_MS});
  await page.getByRole('textbox',{name:'Email'}).fill(email);
  await page.getByRole('textbox',{name:'Password'}).fill(password);
  await page.getByRole('button',{name:/sign in/i}).click();
  await page.waitForLoadState('domcontentloaded').catch(()=>{});
  await page.waitForTimeout(500);
}
async function qpsInput(page) {
  const input = page.locator('#max_qps_limit');
  await input.waitFor({ state:'visible', timeout: FIELD_TIMEOUT_MS });
  return input;
}
async function readCurrentLimit(page){
  return num(await (await qpsInput(page)).inputValue());
}
async function setQpsAndGetEffective(page, desired) {
  const input = await qpsInput(page);
  await input.click({ clickCount: 3 });
  await input.press('Backspace');
  await input.type(String(desired), { delay: 10 });
  const handle = await input.elementHandle();
  await handle.evaluate((el, val) => {
    el.value = String(val);
    el.dispatchEvent(new Event('input',  { bubbles: true }));
    el.dispatchEvent(new Event('change', { bubbles: true }));
    el.dispatchEvent(new Event('blur',   { bubbles: true }));
  }, desired);
  await input.press('Tab');
  await page.waitForTimeout(300);
  return num(await input.inputValue());
}
async function findExactSaveButton(page) {
  let btn = page.getByRole('button', { name: /^save$/i }).first();
  if (await btn.count() && await btn.isVisible()) return btn;
  const buttons = page.locator('button');
  const n = await buttons.count();
  for (let i = 0; i < n; i++) {
    const el = buttons.nth(i);
    if (!(await el.isVisible())) continue;
    const text = (await el.innerText()).trim();
    if (/^save$/i.test(text)) return el;
    if (/save/i.test(text) && !/exit/i.test(text)) return el;
  }
  const any = page.getByText(/^save$/i).first();
  if (await any.count() && await any.isVisible()) return any;
  throw new Error('Exact "SAVE" button not found.');
}
async function waitEnabledAndClick(page, btn) {
  await btn.scrollIntoViewIfNeeded().catch(()=>{});
  await btn.waitFor({ state:'visible', timeout: 4000 }).catch(()=>{});
  try {
    const handle = await btn.elementHandle();
    await page.waitForFunction(
      el => !!el && !el.disabled && el.getAttribute('aria-disabled') !== 'true',
      handle, { timeout: 4000 }
    );
  } catch {}
  try { await btn.click({ timeout: 8000 }); }
  catch { await page.waitForTimeout(250); await btn.click({ timeout: 8000 }); }
  await Promise.race([
    page.getByText(/saved|success/i).first().waitFor({ timeout: 5000 }).catch(() => {}),
    page.waitForResponse(r => r.url().includes(`/ad-exchange/dsp/`) &&
                               ['PUT','POST','PATCH'].includes(r.request().method()), { timeout: 8000 }).catch(()=>null),
    page.waitForLoadState('domcontentloaded')
  ]);
}
async function openDspEdit(page, dspId) {
  const url = `${BASE_URL}/ad-exchange/dsp/${encodeURIComponent(dspId)}/edit`;
  for (let attempt = 1; attempt <= ENDPOINT_RETRIES; attempt++) {
    try {
      await page.goto(url, { waitUntil: 'domcontentloaded', timeout: ENDPOINT_TIMEOUT_MS });
    } catch (err) {
      if (!String(err).includes('ERR_ABORTED')) throw err;
    }
    await page.waitForURL(/\/ad-exchange\/dsp\/\d+\/edit/i, { timeout: ENDPOINT_TIMEOUT_MS }).catch(()=>{});
    try {
      await qpsInput(page);
      return; // success
    } catch (e) {
      if (attempt === ENDPOINT_RETRIES) throw e;
      await page.waitForTimeout(1200);
    }
  }
}
async function saveAndVerify(page, dspId, expected) {
  const saveBtn = await findExactSaveButton(page);
  for (let attempt = 1; attempt <= ENDPOINT_RETRIES; attempt++) {
    const want = await setQpsAndGetEffective(page, expected);
    await waitEnabledAndClick(page, saveBtn);
    await openDspEdit(page, dspId);
    const after = await readCurrentLimit(page);
    if (after === want) return;
    console.log(`(attempt ${attempt}) UI showed ${want} but after reload saw ${after} — retrying...`);
  }
  const ts = new Date().toISOString().replace(/[:.]/g, '-');
  const shot = path.join(OUT_DIR, `after-save-failed-${ts}.png`);
  await page.screenshot({ path: shot, fullPage: true }).catch(() => {});
  throw new Error(`Save did not persist after ${ENDPOINT_RETRIES} attempts. Screenshot: ${shot}`);
}

// ====== Main ======
(async () => {
  ensureOutDir();

  const creds = readLogin();
  const { hourKey, items } = await fetchLastHourRows(creds);
  if (!items.length) {
    console.log(`No rows for last full hour ${hourKey}. Exiting.`);
    process.exit(0);
  }

  // Snapshot CSV
  const snapHeader = ['hour_utc','dsp_company','dsp_id','qps','bid_requests','dsp_srcpm'];
  const snapPath = path.join(OUT_DIR, `last-hour-snapshot-${hourKey.replace(/[: ]/g,'-')}.csv`);
  fs.writeFileSync(
    snapPath,
    '\uFEFF'+[snapHeader.join(','), ...items.map(r =>
      [r.hour_utc, r.dsp_company, r.dsp_id, (Number.isFinite(r.qps)? r.qps.toFixed(3):''), r.bid_requests, r.dsp_srcpm]
        .map(csvEsc).join(',')
    )].join('\n'),
    'utf8'
  );
  console.log(`Snapshot → ${snapPath}`);

  // ===== Filtering =====
  let rows = items.slice();

  // 1) ALLOW (optional)
  if (!ALLOW_MODE_ALL) {
    const allowedNorms = ALLOW_DSPS.map(normName);
    rows = rows.filter(r => {
      const n = normName(r.dsp_company);
      return allowedNorms.some(a => n.includes(a) || a.includes(n));
    });
  }
  if (ALLOW_DSP_IDS.length) {
    const idSet = new Set(ALLOW_DSP_IDS.map(String));
    rows = rows.filter(r => idSet.has(String(r.dsp_id)));
  }

  // 2) EXCLUDE (names + ids)
  if (EXCLUDE_DSPS.length) {
    const exclNorms = EXCLUDE_DSPS.map(normName);
    rows = rows.filter(r => {
      const n = normName(r.dsp_company);
      return !exclNorms.some(x => n.includes(x) || x.includes(n));
    });
  }
  if (EXCLUDE_DSP_IDS.length) {
    const badIds = new Set(EXCLUDE_DSP_IDS.map(String));
    rows = rows.filter(r => !badIds.has(String(r.dsp_id)));
  }

  if (!rows.length) {
    console.log('No rows matched filters after excludes. Exiting.');
    process.exit(0);
  }

  // Launch Playwright and apply rules (DRY_RUN will not click Save)
  const browser = await chromium.launch({ headless: HEADLESS });
  const context = await browser.newContext();
  const page = await context.newPage();
  const actions = [];
  const errors = [];

  try {
    await uiLogin(page, creds.email, creds.password);

    for (const r of rows) {
      const dspId = String(r.dsp_id || '').trim();
      if (!dspId) continue;

      console.log(`\n→ ${r.dsp_company} [${dspId}] srcpm=${r.dsp_srcpm} qps=${Number(r.qps).toFixed(2)}  URL: ${BASE_URL}/ad-exchange/dsp/${dspId}/edit`);

      // ===== per-item try/catch so we SKIP on timeouts/errors
      try {
        await openDspEdit(page, dspId);

        const current = await readCurrentLimit(page);
        const decision = decideNewLimit({ srcpm: Number(r.dsp_srcpm), qps: Number(r.qps), current });
        const saturation = current > 0 ? (Number(r.qps) / current) * 100 : 0;

        let wouldSaveValue = '';
        if (DRY_RUN && decision.action !== 'hold' && Number.isFinite(decision.newLimit)) {
          wouldSaveValue = await setQpsAndGetEffective(page, decision.newLimit);
          console.log(`   [DRY RUN] current=${current} → proposed=${decision.newLimit} → UI shows ${wouldSaveValue} (${decision.reason})`);
        } else if (!DRY_RUN && decision.action !== 'hold' && Number.isFinite(decision.newLimit)) {
          await saveAndVerify(page, dspId, decision.newLimit);
          const saved = await readCurrentLimit(page);
          console.log(`   savedLimit=${saved}`);
          wouldSaveValue = saved;
        } else {
          console.log('   No change.');
        }

        actions.push({
          dsp_id: dspId,
          dsp_company: r.dsp_company,
          srcpm: r.dsp_srcpm,
          qps: Number(r.qps?.toFixed?.(2)),
          current_limit: current,
          proposed_limit: decision.newLimit,
          would_save_value: wouldSaveValue,
          saturation_pct: Number.isFinite(saturation) ? Number(saturation.toFixed(1)) : '',
          action: decision.action,
          reason: decision.reason,
          error: ''
        });

      } catch (err) {
        const msg = (err && err.message) ? err.message : String(err);
        console.warn(`   ⚠ Skipping ${dspId} due to error: ${msg}`);

        errors.push({ dsp_id: dspId, err: msg });

        actions.push({
          dsp_id: dspId,
          dsp_company: r.dsp_company,
          srcpm: r.dsp_srcpm,
          qps: Number(r.qps?.toFixed?.(2)),
          current_limit: '',
          proposed_limit: '',
          would_save_value: '',
          saturation_pct: '',
          action: 'skipped_error',
          reason: 'endpoint timeout/error',
          error: msg
        });

        // continue to next item
        continue;
      }
    }
  } finally {
    await page.close(); await context.close(); await browser.close();
  }

  // Audit CSV (includes skipped_error rows)
  const actHeader = [
    'dsp_id','dsp_company','srcpm','qps',
    'current_limit','proposed_limit','would_save_value','saturation_pct',
    'action','reason','error'
  ];
  const actPath = path.join(OUT_DIR, `actions-${new Date().toISOString().replace(/[:.]/g,'-')}.csv`);
  fs.writeFileSync(actPath, '\uFEFF'+[
    actHeader.join(','),
    ...actions.map(a => actHeader.map(k => csvEsc(a[k])).join(','))
  ].join('\n'), 'utf8');
  console.log(`\n✅ Actions (DRY_RUN=${DRY_RUN}) → ${actPath}`);

  if (FAIL_ON_ITEM_ERROR && errors.length) {
    console.error(`Encountered ${errors.length} item error(s). Failing as requested (FAIL_ON_ITEM_ERROR=true).`);
    process.exit(1);
  }
  // otherwise finish successfully
  process.exit(0);
})().catch(err=>{
  console.error('Fatal error:', err.message||err);
  process.exit(1);
});
