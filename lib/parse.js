/**
 * Parser for B&Q diy.com verified seller pages.
 * Extracts:
 * - Business name
 * - VAT number
 * - Registered address
 * - Shipped from
 *
 * Robust against:
 * - label/value blocks without colons (common on modern UI pages)
 * - dt/dd or table markup (fallback)
 * - generic text noise (footer/contact forms)
 */

function decodeEntities(str = '') {
  return str
    .replace(/&nbsp;/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/\u00a0/g, ' ');
}

function cleanText(str = '') {
  return decodeEntities(str)
    .replace(/\r/g, '\n')
    .replace(/[ \t]+/g, ' ')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

function stripTagsKeepLines(html = '') {
  return html
    .replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi, '\n')
    .replace(/<style\b[^>]*>[\s\S]*?<\/style>/gi, '\n')
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<\/(p|div|li|tr|td|th|section|article|h\d|dd|dt)>/gi, '\n')
    .replace(/<[^>]+>/g, ' ')
    .replace(/\n[ \t]+/g, '\n')
    .replace(/[ \t]+\n/g, '\n');
}

function normalizeLabel(s = '') {
  return cleanText(s)
    .toLowerCase()
    .replace(/\s+/g, ' ')
    .replace(/[:\-–]+$/g, '')
    .trim();
}

function looksLikeVat(value = '') {
  const v = cleanText(value);
  // Supports common UK/EU VAT styles
  return /^(?:[A-Z]{2}\s*)?[A-Z0-9 -]{8,20}$/i.test(v) && /\d{8,}/.test(v);
}

function isBadBusinessName(value = '') {
  const v = normalizeLabel(value);
  const bad = new Set([
    '',
    'email',
    'e-mail',
    'phone',
    'telephone',
    'message',
    'subject',
    'name',
    'business name',
    'vat number',
    'registered address',
    'shipped from',
    'submit',
    'continue',
  ]);
  return bad.has(v);
}

function looksLikeBusinessNameCandidate(value = '') {
  const v = cleanText(value);
  if (!v || isBadBusinessName(v)) return false;

  const n = normalizeLabel(v);

  // reject common headings / UI text
  const bannedStarts = [
    'how to contact',
    'returns policy',
    'got a question',
    'this seller ships from',
    'contact seller',
    'down chevron',
  ];
  if (bannedStarts.some((x) => n.startsWith(x))) return false;

  // should not look like field labels
  const bannedExact = new Set([
    'vat number',
    'registered address',
    'business address',
    'shipped from',
    'ships from',
    'email',
    'phone',
    'telephone',
    'message',
    'subject',
  ]);
  if (bannedExact.has(n)) return false;

  // usually a company name is not too long
  if (v.length > 120) return false;

  return true;
}

function extractDtDdPairs(html) {
  const result = {};
  const re = /<dt[^>]*>([\s\S]*?)<\/dt>\s*<dd[^>]*>([\s\S]*?)<\/dd>/gi;
  let m;
  while ((m = re.exec(html)) !== null) {
    const k = cleanText(stripTagsKeepLines(m[1])).replace(/\n+/g, ' ');
    const v = cleanText(stripTagsKeepLines(m[2])).replace(/\n+/g, ' ');
    if (k && v) result[k] = v;
  }
  return result;
}

function extractTablePairs(html) {
  const result = {};
  const re = /<tr[^>]*>\s*<t[hd][^>]*>([\s\S]*?)<\/t[hd]>\s*<t[hd][^>]*>([\s\S]*?)<\/t[hd]>\s*<\/tr>/gi;
  let m;
  while ((m = re.exec(html)) !== null) {
    const k = cleanText(stripTagsKeepLines(m[1])).replace(/\n+/g, ' ');
    const v = cleanText(stripTagsKeepLines(m[2])).replace(/\n+/g, ' ');
    if (k && v) result[k] = v;
  }
  return result;
}

function extractColonPairsFromText(text) {
  const result = {};
  const lines = text.split('\n').map((x) => x.trim()).filter(Boolean);

  for (const line of lines) {
    const m = line.match(/^([A-Za-z][A-Za-z\s()\/&.-]{2,60})\s*:\s*(.+)$/);
    if (!m) continue;
    const key = cleanText(m[1]);
    const val = cleanText(m[2]);
    if (key && val && !result[key]) result[key] = val;
  }

  return result;
}

/**
 * Extract values from pages that render as:
 *   Business name
 *   ACME LTD
 *   VAT number
 *   GB123...
 *   Registered address
 *   ...
 */
function extractLabelValueBlocks(text) {
  const lines = text
    .split('\n')
    .map((x) => cleanText(x))
    .filter(Boolean);

  // Section boundaries / headings that should stop multi-line capture
  const stopHeadings = [
    'this seller ships from',
    'how to contact a b&q verified seller',
    'returns policy',
    'got a question about your order',
  ];

  const allLabels = [
    'business name',
    'vat number',
    'vat no',
    'registered address',
    'business address',
    'shipped from',
    'ships from',
    ...stopHeadings,
    // common UI/contact noise
    'email',
    'phone',
    'telephone',
    'message',
    'subject',
    'contact seller',
  ];

  const isKnownLabel = (line) => {
    const n = normalizeLabel(line);
    return allLabels.some((l) => n === l || n.startsWith(l));
  };

  const isStopHeading = (line) => {
    const n = normalizeLabel(line);
    return stopHeadings.some((h) => n === h || n.startsWith(h));
  };

  // Supports "Label VALUE" on the same line
  function extractSameLineValue(line, label) {
    const nLine = normalizeLabel(line);
    const nLabel = normalizeLabel(label);

    if (nLine === nLabel) return '';

    if (nLine.startsWith(nLabel + ' ')) {
      const idx = line.toLowerCase().indexOf(label.toLowerCase());
      if (idx >= 0) {
        return cleanText(line.slice(idx + label.length)).replace(/^[:\-–\s]+/, '');
      }
      // Fallback if casing/spacing differ
      return cleanText(line).replace(new RegExp(`^${label}\\s*[:\\-–]?\\s*`, 'i'), '');
    }

    return '';
  }

  function collectAfterLabel(targetLabels, { maxLines = 6, keepMultiline = false } = {}) {
    const candidates = [];

    for (let i = 0; i < lines.length; i++) {
      const line = lines[i];
      const lineNorm = normalizeLabel(line);

      const isTarget = targetLabels.some((l) => {
        const nl = normalizeLabel(l);
        return lineNorm === nl || lineNorm.startsWith(nl);
      });

      if (!isTarget) continue;

      // 1) Try same-line value first
      let pushedSameLine = false;
      for (const l of targetLabels) {
        const same = extractSameLineValue(line, l);
        if (same) {
          candidates.push(same);
          pushedSameLine = true;
          break;
        }
      }
      if (pushedSameLine) continue;

      // 2) Collect subsequent lines until next label/heading
      const collected = [];
      for (let j = i + 1; j < lines.length && collected.length < maxLines; j++) {
        const next = lines[j];
        const nextNorm = normalizeLabel(next);

        if (isStopHeading(next)) break; // hard stop on known section headings
        if (isKnownLabel(next)) break;  // stop on next field label

        // skip tiny UI noise
        if (!nextNorm || ['copy', 'open', 'close', 'down chevron'].includes(nextNorm)) continue;

        collected.push(next);
      }

      if (collected.length > 0) {
        candidates.push(keepMultiline ? collected.join(', ') : collected[0]);
      }
    }

    return candidates;
  }

  const businessCandidates = collectAfterLabel(['Business name'], { maxLines: 3, keepMultiline: false })
    .filter((v) => !isBadBusinessName(v));

  const vatCandidates = collectAfterLabel(['VAT number', 'VAT no', 'VAT No.'], { maxLines: 3, keepMultiline: false })
    .filter(looksLikeVat);

  const addressCandidates = collectAfterLabel(['Registered address', 'Business address'], { maxLines: 8, keepMultiline: true });

  // B&Q often shows "This seller ships from <country>"
  const shippedFromCandidates = [
    ...collectAfterLabel(['Shipped from', 'Ships from'], { maxLines: 2, keepMultiline: false }),
    ...collectAfterLabel(['This seller ships from'], { maxLines: 2, keepMultiline: false }),
  ];

  return {
    businessName: businessCandidates[0] || '',
    vatNumber: vatCandidates[0] || '',
    registeredAddress: addressCandidates[0] || '',
    shippedFrom: shippedFromCandidates[0] || '',
  };
}

function findValueByLabels(map, labels) {
  const entries = Object.entries(map);

  // exact match
  for (const label of labels) {
    const n = normalizeLabel(label);
    const hit = entries.find(([k]) => normalizeLabel(k) === n);
    if (hit?.[1]) return hit[1];
  }

  // contains match
  for (const label of labels) {
    const n = normalizeLabel(label);
    const hit = entries.find(([k]) => normalizeLabel(k).includes(n));
    if (hit?.[1]) return hit[1];
  }

  return '';
}

function extractRegexFallback(text) {
  const t = cleanText(text);
  const lines = t.split('\n').map((x) => cleanText(x)).filter(Boolean);

  // 1) Standard newline layout
  let businessName =
    (t.match(/Business\s+name\s*\n+([^\n]+)/i)?.[1] || '').trim();

  let vatNumber =
    (t.match(/VAT\s+(?:number|no\.?)\s*\n+([A-Z0-9 -]{8,25})/i)?.[1] || '').trim();

  let registeredAddress =
    (t.match(/Registered\s+address\s*\n+([\s\S]{0,250}?)(?:\n\s*(?:This seller ships from|Shipped\s+from|How to contact|Returns policy|Got a question)\b|\n{2,}|$)/i)?.[1] || '')
      .replace(/\n+/g, ', ')
      .trim();

  let shippedFrom =
    (t.match(/This seller ships from\s+([^\n]+)/i)?.[1] || '').trim();

  if (!shippedFrom) {
    shippedFrom = (t.match(/(?:Shipped\s+from|Ships\s+from)\s*\n+([^\n]+)/i)?.[1] || '').trim();
  }

  // 2) Derive business name from line(s) above VAT number if missing
  if (!businessName) {
    for (let i = 0; i < lines.length; i++) {
      const n = normalizeLabel(lines[i]);
      if (n === 'vat number' || n.startsWith('vat number') || n === 'vat no' || n.startsWith('vat no')) {
        for (let k = i - 1; k >= Math.max(0, i - 3); k--) {
          const candidate = lines[k];
          if (looksLikeBusinessNameCandidate(candidate)) {
            businessName = candidate;
            break;
          }
        }
        if (businessName) break;
      }
    }
  }

  // 3) Derive business name from line(s) above Registered address if still missing
  if (!businessName) {
    for (let i = 0; i < lines.length; i++) {
      const n = normalizeLabel(lines[i]);
      if (n === 'registered address' || n.startsWith('registered address')) {
        for (let k = i - 1; k >= Math.max(0, i - 4); k--) {
          const candidate = lines[k];
          if (looksLikeBusinessNameCandidate(candidate)) {
            businessName = candidate;
            break;
          }
        }
        if (businessName) break;
      }
    }
  }

  // 4) Extra regex heuristic: uppercase company-like line before VAT block
  if (!businessName) {
    const m = t.match(/([A-Z][A-Z0-9&'().,\- ]{3,})\s*\n+\s*VAT\s+(?:number|no\.?)/);
    if (m?.[1] && looksLikeBusinessNameCandidate(m[1])) {
      businessName = cleanText(m[1]);
    }
  }

  if (isBadBusinessName(businessName)) businessName = '';
  if (vatNumber && !looksLikeVat(vatNumber)) vatNumber = '';

  return { businessName, vatNumber, registeredAddress, shippedFrom };
}

export function parseSellerPage(html, sellerId, sourceUrl = '') {
  const visibleText = cleanText(stripTagsKeepLines(html));

  // 1) Best for this page type: visible-text label blocks
  const blockVals = extractLabelValueBlocks(visibleText);

  // 2) Structured fallback (dt/dd, table, colon)
  const dtdd = extractDtDdPairs(html);
  const table = extractTablePairs(html);
  const colon = extractColonPairsFromText(visibleText);
  const merged = { ...colon, ...table, ...dtdd };

  const labelVals = {
    businessName: findValueByLabels(merged, ['Business name', 'Seller name', 'Company name']),
    vatNumber: findValueByLabels(merged, ['VAT number', 'VAT No', 'VAT no.']),
    registeredAddress: findValueByLabels(merged, ['Registered address', 'Business address', 'Company address']),
    shippedFrom: findValueByLabels(merged, ['Shipped from', 'Ships from', 'This seller ships from']),
  };

  // 3) Regex fallback (includes "line before VAT number" heuristic)
  const rxVals = extractRegexFallback(visibleText);

  let businessName = blockVals.businessName || labelVals.businessName || rxVals.businessName;
  let vatNumber = blockVals.vatNumber || labelVals.vatNumber || rxVals.vatNumber;
  let registeredAddress = blockVals.registeredAddress || labelVals.registeredAddress || rxVals.registeredAddress;
  let shippedFrom = blockVals.shippedFrom || labelVals.shippedFrom || rxVals.shippedFrom;

  // Guard against false positives like "Email"
  if (isBadBusinessName(businessName)) {
    businessName = '';
  }

  if (vatNumber && !looksLikeVat(vatNumber)) {
    vatNumber = '';
  }

  registeredAddress = cleanText(registeredAddress).replace(/\n+/g, ', ');
  shippedFrom = cleanText(shippedFrom).replace(/\n+/g, ', ');
  businessName = cleanText(businessName);
  vatNumber = cleanText(vatNumber);

  // Safety trim if extra sections leak into the address
  const cutPhrases = [
    'this seller ships from',
    'how to contact a b&q verified seller',
    'got a question about your order',
    'returns policy',
  ];
  for (const p of cutPhrases) {
    const idx = registeredAddress.toLowerCase().indexOf(p);
    if (idx >= 0) {
      registeredAddress = registeredAddress.slice(0, idx).trim().replace(/[,\s]+$/g, '');
    }
  }

  // If shippedFrom still empty, derive from visible text phrase
  if (!shippedFrom) {
    const m = visibleText.match(/This seller ships from\s+([^\n]+)/i);
    if (m?.[1]) shippedFrom = cleanText(m[1]);
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