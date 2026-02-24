#!/usr/bin/env node

/**
 * B&Q (diy.com) verified seller scraper — API-based.
 *
 * Calls the Kingfisher marketplace seller API directly (no browser needed).
 * Much faster and more reliable than browser-based scraping.
 *
 * Usage:
 *   node scrape.mjs
 *   node scrape.mjs --from 3900 --to 4100
 *   node scrape.mjs --from 3958 --to 3959 --delay 1500
 *
 * Output:
 *   results/sellers.csv
 *   results/progress.json
 */

import { writeFileSync, readFileSync, existsSync, mkdirSync, appendFileSync } from 'node:fs';

// --- Config ---
const args = parseArgs(process.argv.slice(2));
const FROM_ID = args.from ?? 1;
const TO_ID = args.to ?? 10000;
const DELAY_MS = args.delay ?? 2000;

const RESULTS_DIR = 'results';
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

async function main() {
  mkdirSync(RESULTS_DIR, { recursive: true });

  const progress = loadProgress();
  initCsv();

  const total = TO_ID - FROM_ID + 1;
  let processedThisRun = 0;
  let found = 0;
  let errors = 0;

  console.log(`\nB&Q Verified Seller Scraper (API)`);
  console.log(`Range: ${FROM_ID} – ${TO_ID} (${total} IDs)`);
  console.log(`Delay: ${DELAY_MS}ms`);
  console.log(`Output: ${CSV_PATH}`);

  const alreadyDone = Object.keys(progress)
    .map(Number)
    .filter((id) => id >= FROM_ID && id <= TO_ID).length;

  if (alreadyDone > 0) {
    console.log(`Resuming — ${alreadyDone} IDs already processed`);
  }

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
  }

  console.log(`\n--- Done ---`);
  console.log(`Processed: ${processedThisRun + alreadyDone} / ${total}`);
  console.log(`Sellers found: ${found}`);
  console.log(`Errors: ${errors}`);
  console.log(`Results saved to: ${CSV_PATH}`);
}

// Kingfisher marketplace seller API key (publicly embedded in every diy.com page)
const SELLER_API_KEY = 'eyJvcmciOiI2MGFlMTA0ZGVjM2M1ZjAwMDFkMjYxYTkiLCJpZCI6IjE0NmFhMTQ5ZGIxYjQ4OGI4OWJlMTNkNTI0MmVhMmZmIiwiaCI6Im11cm11cjEyOCJ9';

async function scrapeSeller(sellerId) {
  const url = `https://www.diy.com/verified-sellers/seller/${sellerId}`;

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      // Call the Kingfisher marketplace seller API directly.
      // URL: /v1/sellers/BQUK/{sellerId}  (BQUK = B&Q UK tenant)
      // Auth: Authorization header (not x-api-key)
      // Accept: */* (application/json returns 406)
      const apiUrl = `https://api.kingfisher.com/v1/sellers/BQUK/${sellerId}`;
      const resp = await fetch(apiUrl, {
        headers: {
          'Authorization': SELLER_API_KEY,
          'Accept': '*/*',
        },
      });

      if (resp.status === 404 || resp.status === 410) {
        return emptyResult(sellerId, url);
      }

      if (!resp.ok) {
        const errMsg = `API HTTP ${resp.status}`;
        if (attempt < MAX_RETRIES) {
          await sleep(4000 * attempt);
          continue;
        }
        return { sellerId, error: errMsg };
      }

      const json = await resp.json();
      return parseSellerApiResponse(json, sellerId, url);
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

/**
 * Parse the Kingfisher seller API JSON response into our flat CSV format.
 *
 * API response shape (JSON:API):
 *   { data: { id, type, attributes: {
 *       corporateName, taxIdentificationNumber, shippingCountry,
 *       corporateContactInformation: { street1, city, state, country, postCode }
 *   }}}
 */
function parseSellerApiResponse(json, sellerId, sourceUrl) {
  const attrs = json?.data?.attributes;
  if (!attrs) {
    return emptyResult(sellerId, sourceUrl);
  }

  const businessName = (attrs.corporateName || attrs.sellerName || '').trim();
  const vatNumber = (attrs.taxIdentificationNumber || '').trim();
  const shippedFrom = (attrs.shippingCountry || '').trim();

  // Use corporateContactInformation for the registered address
  let registeredAddress = '';
  const addr = attrs.corporateContactInformation || attrs.contactInformation;
  if (addr && typeof addr === 'object') {
    registeredAddress = [
      addr.street1 || '',
      addr.street2 || '',
      addr.city || '',
      addr.state || '',
      addr.postCode || '',
      addr.country || '',
    ].filter(Boolean).map(s => s.trim()).join(', ');
  }

  return {
    sellerId,
    businessName,
    vatNumber,
    registeredAddress,
    shippedFrom,
    sourceUrl,
  };
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
  }
  return result;
}

main().catch((err) => {
  console.error('\nFatal error:', err);
  process.exit(1);
});
