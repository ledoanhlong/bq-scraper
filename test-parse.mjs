import { readFileSync } from 'node:fs';
import { parseSellerPage } from './lib/parse.js';

const html = readFileSync('./html template', 'utf-8');
const parsed = parseSellerPage(html, 3958, 'https://www.diy.com/verified-sellers/seller/3958');

console.log(parsed);