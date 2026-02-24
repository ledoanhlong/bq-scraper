#!/usr/bin/env node

/**
 * B&Q (diy.com) verified seller scraper — API-based.
 *
 * Calls the Kingfisher marketplace seller API directly (no browser needed).
 *
 * Usage:
 *   node scrape.mjs                                   # IDs 1–25000, 5 concurrent
 *   node scrape.mjs --from 3900 --to 4100             # custom range
 *   node scrape.mjs --concurrency 10 --delay 300      # faster
 *   node scrape.mjs --from 1 --to 25000 --delay 500   # full run, conservative
 *
 * Output:
 *   results/sellers.csv      — one row per found seller
 *   results/progress.json    — tracks completed IDs (safe to resume)
 *
 * The scraper is fully resumable: re-run the same command and it skips
 * already-processed IDs. Ctrl+C is safe — progress is saved on exit.
 */

import { writeFileSync, readFileSync, existsSync, mkdirSync, appendFileSync } from 'node:fs';

// --- Config ---
const args = parseArgs(process.argv.slice(2));
const FROM_ID = args.from ?? 1;
const TO_ID = args.to ?? 35000;
const DELAY_MS = args.delay ?? 500;
const CONCURRENCY = args.concurrency ?? 5;

const RESULTS_DIR = 'results';
const CSV_PATH = `${RESULTS_DIR}/sellers.csv`;
const PROGRESS_PATH = `${RESULTS_DIR}/progress.json`;
const MAX_RETRIES = 3;

const CSV_COLUMNS = [
  'sellerId',
  'businessName',
  'vatNumber',
  'registeredAddress',
  'shippedFrom',
  'sourceUrl',
];

// Kingfisher marketplace seller API key (publicly embedded in every diy.com page)
const SELLER_API_KEY = 'eyJvcmciOiI2MGFlMTA0ZGVjM2M1ZjAwMDFkMjYxYTkiLCJpZCI6IjE0NmFhMTQ5ZGIxYjQ4OGI4OWJlMTNkNTI0MmVhMmZmIiwiaCI6Im11cm11cjEyOCJ9';

let shuttingDown = false;

async function main() {
  mkdirSync(RESULTS_DIR, { recursive: true });

  const progress = loadProgress();
  initCsv();

  // Graceful shutdown — save progress on Ctrl+C
  const shutdown = () => {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log('\n\nShutting down gracefully — saving progress...');
    saveProgress(progress);
    console.log('Progress saved. Re-run the same command to resume.');
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);

  const total = TO_ID - FROM_ID + 1;
  let processedThisRun = 0;
  let found = 0;
  let errors = 0;

  // Build list of IDs still to process
  const pendingIds = [];
  for (let id = FROM_ID; id <= TO_ID; id++) {
    if (!progress[id]) pendingIds.push(id);
  }

  const alreadyDone = total - pendingIds.length;

  console.log(`\nB&Q Verified Seller Scraper (API)`);
  console.log(`Range: ${FROM_ID} – ${TO_ID} (${total} IDs)`);
  console.log(`Concurrency: ${CONCURRENCY} | Delay: ${DELAY_MS}ms between batches`);
  console.log(`Output: ${CSV_PATH}`);
  if (alreadyDone > 0) {
    console.log(`Resuming — ${alreadyDone} already done, ${pendingIds.length} remaining`);
  }
  console.log('');

  try {
    // Process in batches of CONCURRENCY
    for (let i = 0; i < pendingIds.length; i += CONCURRENCY) {
      if (shuttingDown) break;

      const batch = pendingIds.slice(i, i + CONCURRENCY);
      const results = await Promise.all(batch.map((id) => scrapeSeller(id)));

      for (const result of results) {
        if (shuttingDown) break;

        processedThisRun++;
        const done = processedThisRun + alreadyDone;

        if (result.error) {
          errors++;
          progress[result.sellerId] = { status: 'error', error: result.error };
          logLine(result.sellerId, `ERROR: ${result.error}`, total, done);
        } else if (!result.businessName && !result.vatNumber && !result.registeredAddress && !result.shippedFrom) {
          progress[result.sellerId] = { status: 'empty' };
          logLine(result.sellerId, 'no seller found', total, done);
        } else {
          found++;
          progress[result.sellerId] = { status: 'ok' };
          appendCsvRow(result);
          logLine(result.sellerId, `OK ${result.businessName || '(seller found)'}`, total, done);
        }
      }

      // Save progress every 50 IDs processed
      if (processedThisRun % 50 < CONCURRENCY) saveProgress(progress);

      // Delay between batches (not between individual requests within a batch)
      if (i + CONCURRENCY < pendingIds.length && !shuttingDown) {
        await sleep(DELAY_MS);
      }
    }
  } finally {
    saveProgress(progress);
  }

  console.log(`\n--- Done ---`);
  console.log(`Processed this run: ${processedThisRun}`);
  console.log(`Total processed: ${processedThisRun + alreadyDone} / ${total}`);
  console.log(`Sellers found: ${found}`);
  console.log(`Errors: ${errors}`);
  console.log(`Results saved to: ${CSV_PATH}`);
}

async function scrapeSeller(sellerId) {
  const sourceUrl = `https://www.diy.com/verified-sellers/seller/${sellerId}`;

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      const apiUrl = `https://api.kingfisher.com/v1/sellers/BQUK/${sellerId}`;
      const resp = await fetch(apiUrl, {
        headers: {
          'Authorization': SELLER_API_KEY,
          'Accept': '*/*',
        },
      });

      // Not found — seller ID doesn't exist
      if (resp.status === 404 || resp.status === 410) {
        return emptyResult(sellerId, sourceUrl);
      }

      // Rate limited — back off and retry
      if (resp.status === 429) {
        const retryAfter = parseInt(resp.headers.get('retry-after') || '0', 10);
        const backoff = Math.max(retryAfter * 1000, 2000 * Math.pow(2, attempt));
        if (attempt < MAX_RETRIES) {
          await sleep(backoff);
          continue;
        }
        return { sellerId, error: 'Rate limited (429)' };
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
      return parseSellerApiResponse(json, sellerId, sourceUrl);
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

  // Prefer corporateContactInformation, but fall back to contactInformation
  // if the corporate fields are placeholder "TBC" values
  let registeredAddress = '';
  const corpAddr = attrs.corporateContactInformation;
  const contactAddr = attrs.contactInformation;
  const addr = (corpAddr && !isTbcAddress(corpAddr)) ? corpAddr : contactAddr;
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

/** Returns true if all address fields are "TBC" or empty placeholders. */
function isTbcAddress(addr) {
  const vals = [addr.street1, addr.street2, addr.city, addr.state, addr.postCode, addr.country]
    .map((v) => (v || '').trim().toUpperCase())
    .filter(Boolean);
  return vals.length === 0 || vals.every((v) => v === 'TBC');
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
    if (argv[i] === '--concurrency' && argv[i + 1]) result.concurrency = parseInt(argv[i + 1], 10);
  }
  return result;
}

main().catch((err) => {
  console.error('\nFatal error:', err);
  process.exit(1);
});
