#!/usr/bin/env node

/**
 * Local B&Q (diy.com) verified seller scraper (browser-backed).
 *
 * Why browser-backed?
 * - diy.com appears to block simple HTTP fetch() calls with anti-bot/WAF pages.
 * - Playwright loads the page like a real browser so parseSellerPage() can work.
 *
 * Usage:
 *   node scrape.mjs
 *   node scrape.mjs --from 3900 --to 4100
 *   node scrape.mjs --from 3958 --to 3959 --delay 1500
 *   node scrape.mjs --from 3958 --to 3959 --delay 1500 --headed
 *
 * Output:
 *   results/sellers.csv
 *   results/progress.json
 *   results/debug/*.html  (on blocked/empty pages for troubleshooting)
 */

import { writeFileSync, readFileSync, existsSync, mkdirSync, appendFileSync } from 'node:fs';
import { chromium } from 'playwright';
import { parseSellerPage } from './lib/parse.js';

// --- Config ---
const args = parseArgs(process.argv.slice(2));
const FROM_ID = args.from ?? 1;
const TO_ID = args.to ?? 10000;
const DELAY_MS = args.delay ?? 2000;
const HEADED = !!args.headed;

const RESULTS_DIR = 'results';
const DEBUG_DIR = `${RESULTS_DIR}/debug`;
const CSV_PATH = `${RESULTS_DIR}/sellers.csv`;
const PROGRESS_PATH = `${RESULTS_DIR}/progress.json`;
const MAX_RETRIES = 2;

const CSV_COLUMNS = [
  'sellerId',
  'businessName',
  'vatNumber',
  'registeredAddress',
  'shippedFrom',
  'sourceUrl',
];

let browser;
let context;
let page;

async function main() {
  mkdirSync(RESULTS_DIR, { recursive: true });
  mkdirSync(DEBUG_DIR, { recursive: true });

  const progress = loadProgress();
  initCsv();

  const total = TO_ID - FROM_ID + 1;
  let processedThisRun = 0;
  let found = 0;
  let errors = 0;

  console.log(`\nB&Q Verified Seller Scraper (Playwright)`);
  console.log(`Range: ${FROM_ID} – ${TO_ID} (${total} IDs)`);
  console.log(`Delay: ${DELAY_MS}ms`);
  console.log(`Output: ${CSV_PATH}`);

  const alreadyDone = Object.keys(progress)
    .map(Number)
    .filter((id) => id >= FROM_ID && id <= TO_ID).length;

  if (alreadyDone > 0) {
    console.log(`Resuming — ${alreadyDone} IDs already processed`);
  }

  await initBrowser();

  console.log('');

  try {
    for (let id = FROM_ID; id <= TO_ID; id++) {
      if (progress[id]) continue;

      processedThisRun++;
      const result = await scrapeSeller(id);

      if (result.error) {
        errors++;
        progress[id] = { status: 'error', error: result.error };
        logLine(id, `ERROR: ${result.error}`, total, processedThisRun + alreadyDone);
      } else if (!result.businessName && !result.vatNumber && !result.registeredAddress && !result.shippedFrom) {
        progress[id] = { status: 'empty' };
        logLine(id, 'no seller found', total, processedThisRun + alreadyDone);
      } else {
        found++;
        progress[id] = { status: 'ok' };
        appendCsvRow(result);
        logLine(id, `✓ ${result.businessName || '(seller found)'}`, total, processedThisRun + alreadyDone);
      }

      if (processedThisRun % 10 === 0) saveProgress(progress);
      if (id < TO_ID) await sleep(DELAY_MS);
    }
  } finally {
    saveProgress(progress);
    await closeBrowser();
  }

  console.log(`\n--- Done ---`);
  console.log(`Processed: ${processedThisRun + alreadyDone} / ${total}`);
  console.log(`Sellers found: ${found}`);
  console.log(`Errors: ${errors}`);
  console.log(`Results saved to: ${CSV_PATH}`);
}

async function initBrowser() {
  browser = await chromium.launch({
    headless: !HEADED,
    args: [
      '--disable-blink-features=AutomationControlled',
      '--no-sandbox',
    ],
  });

  context = await browser.newContext({
    locale: 'en-GB',
    timezoneId: 'Europe/London',
    userAgent:
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    viewport: { width: 1366, height: 900 },
    extraHTTPHeaders: {
      'Accept-Language': 'en-GB,en;q=0.9',
      'Upgrade-Insecure-Requests': '1',
    },
  });

  page = await context.newPage();

  // Light stealth-ish tweaks (not magic, but helps)
  await page.addInitScript(() => {
    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
  });
}

async function closeBrowser() {
  try { if (page) await page.close(); } catch {}
  try { if (context) await context.close(); } catch {}
  try { if (browser) await browser.close(); } catch {}
}

async function scrapeSeller(sellerId) {
  const url = `https://www.diy.com/verified-sellers/seller/${sellerId}`;

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      const response = await page.goto(url, {
        waitUntil: 'domcontentloaded',
        timeout: 45000,
      });

      const status = response?.status?.() ?? null;
      const finalUrl = page.url();

      // Hard not-found statuses
      if (status && [404, 410].includes(status)) {
        return emptyResult(sellerId, url);
      }

      // Wait for the React SPA to render seller content or a "not found" message.
      // Both are valid outcomes — we just need the page to finish rendering.
      try {
        await page.waitForFunction(
          () => {
            const t = document.body?.innerText || '';
            // Seller data rendered
            if (/VAT number|Registered address|This seller ships from|Shipped from/i.test(t)) return true;
            // "Not found" rendered (React component)
            if (document.querySelector('[data-test-id="seller-not-found"]')) return true;
            if (/seller details cannot be found|seller not found/i.test(t)) return true;
            return false;
          },
          { timeout: 8000 }
        );
      } catch {
        // continue anyway — we'll inspect HTML below
      }

      const html = await page.content();

      // Early detection: B&Q renders a specific element for missing sellers
      if (html.includes('data-test-id="seller-not-found"') ||
          html.includes('seller details cannot be found')) {
        return emptyResult(sellerId, url);
      }

      // Detect blocks/interstitials
      if (isBlockedHtml(html, status, finalUrl)) {
        saveDebugHtml(`${DEBUG_DIR}/blocked_${sellerId}.html`, html);
        // retry once with a refresh-like pause
        if (attempt < MAX_RETRIES) {
          await sleep(4000 * attempt);
          continue;
        }
        return { sellerId, error: `Blocked/challenge page (HTTP ${status ?? 'unknown'})` };
      }

      // B&Q sometimes returns generic pages; parse and decide
      const parsed = parseSellerPage(html, sellerId, url);

      // If parser got nothing, save sample for inspection
      if (!parsed.businessName && !parsed.vatNumber && !parsed.registeredAddress && !parsed.shippedFrom) {
        saveDebugHtml(`${DEBUG_DIR}/empty_${sellerId}.html`, html);

        // If content clearly says not found -> empty
        const text = html.toLowerCase();
        if (
          text.includes('page not found') ||
          text.includes('seller not found') ||
          text.includes("sorry, we can't find") ||
          text.includes('sorry, we can&apos;t find')
        ) {
          return emptyResult(sellerId, url);
        }

        // otherwise maybe transient/challenge page disguised as 200
        if (attempt < MAX_RETRIES) {
          await sleep(4000 * attempt);
          continue;
        }
      }

      return parsed;
    } catch (err) {
      if (attempt < MAX_RETRIES) {
        await sleep(4000 * attempt);
        continue;
      }
      return { sellerId, error: err?.message || String(err) };
    }
  }

  return { sellerId, error: 'Unknown scraping failure' };
}

function emptyResult(sellerId, url) {
  return {
    sellerId,
    businessName: '',
    vatNumber: '',
    registeredAddress: '',
    shippedFrom: '',
    sourceUrl: url,
  };
}

function isBlockedHtml(html, status, finalUrl = '') {
  // Strip <script> and <style> tags to avoid matching config JSON
  // (diy.com embeds "showCaptcha", "googleCaptchaSiteKey" etc. in every page)
  const visibleText = (html || '')
    .replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi, ' ')
    .replace(/<style\b[^>]*>[\s\S]*?<\/style>/gi, ' ')
    .toLowerCase();

  // Strong signals of WAF / bot challenge (checked against visible content only)
  const blockSignals = [
    'access denied',
    'request unsuccessful',
    'service unavailable',
    'temporarily unavailable',
    'pardon our interruption',
    'sorry, you have been blocked',
    'verify you are human',
    'cf-chl',
    'bot detection',
  ];

  // These signals are safe to check against the full HTML
  // (they won't appear in normal page config)
  const fullHtmlSignals = [
    'incapsula',
    'distil',
    '/_sec/',
  ];

  // status 503 often means challenge/interstitial here
  if (status === 503) return true;

  if (blockSignals.some((s) => visibleText.includes(s))) return true;

  const fullText = (html || '').toLowerCase();
  if (fullHtmlSignals.some((s) => fullText.includes(s))) return true;

  // Check for actual captcha challenge elements (not config keys)
  if (/solve.{0,20}captcha|complete.{0,20}captcha|captcha-container|captcha-box|g-recaptcha[^"]/i.test(visibleText)) {
    return true;
  }

  // Sometimes challenge pages redirect to generic routes
  if (finalUrl && /challenge|captcha|blocked/i.test(finalUrl)) return true;

  return false;
}

function saveDebugHtml(path, html) {
  try {
    writeFileSync(path, html, 'utf-8');
  } catch {
    // ignore debug write failures
  }
}

// --- CSV helpers ---
function initCsv() {
  if (!existsSync(CSV_PATH)) {
    writeFileSync(CSV_PATH, CSV_COLUMNS.join(',') + '\n', 'utf-8');
  }
}

function appendCsvRow(data) {
  const row = CSV_COLUMNS.map((col) => csvEscape(data[col]));
  appendFileSync(CSV_PATH, row.join(',') + '\n', 'utf-8');
}

function csvEscape(value) {
  let val = value ?? '';
  if (typeof val === 'object') val = JSON.stringify(val);
  val = String(val);
  if (val.includes(',') || val.includes('"') || val.includes('\n')) {
    return `"${val.replace(/"/g, '""')}"`;
  }
  return val;
}

// --- Progress helpers ---
function loadProgress() {
  if (existsSync(PROGRESS_PATH)) {
    return JSON.parse(readFileSync(PROGRESS_PATH, 'utf-8'));
  }
  return {};
}

function saveProgress(progress) {
  writeFileSync(PROGRESS_PATH, JSON.stringify(progress), 'utf-8');
}

// --- Utilities ---
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function logLine(id, message, total, done) {
  const pct = ((done / total) * 100).toFixed(1);
  process.stdout.write(`\r[${done}/${total} ${pct}%] ID ${id}: ${message}\n`);
}

function parseArgs(argv) {
  const result = {};
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === '--from' && argv[i + 1]) result.from = parseInt(argv[i + 1], 10);
    if (argv[i] === '--to' && argv[i + 1]) result.to = parseInt(argv[i + 1], 10);
    if (argv[i] === '--delay' && argv[i + 1]) result.delay = parseInt(argv[i + 1], 10);
    if (argv[i] === '--headed') result.headed = true;
  }
  return result;
}

main().catch((err) => {
  console.error('\nFatal error:', err);
  process.exit(1);
});