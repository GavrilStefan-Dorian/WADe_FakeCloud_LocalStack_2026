'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { parseCSV, analyzeCSV } = require('../processor');
const samples = filename => fs.readFileSync(path.join(__dirname, '../samples', filename), 'utf8');

const rows = parseCSV('a,b\r\n"a,b","quoted ""word"""\r\n');
assert.deepEqual(rows[1].fields, ['a,b', 'quoted "word"']);
assert.throws(() => parseCSV('a,b\n"unclosed'), /Unclosed quoted field/);

const good = analyzeCSV(samples('graduates-valid.csv'));
assert.equal(good.status, 'ok');
assert.equal(good.totalRows, 12);
assert.equal(good.accepted, 12);
assert.equal(good.rejected, 0);
assert.equal(good.byStatus.employed, 7);
assert.equal(good.byStatus.seeking, 3);
assert.equal(good.byStatus.studying, 2);
assert.equal(good.employmentRatePct, 58.3);
assert.equal(good.byIndustry['IT, Consulting'], 1);
assert.equal(good.cohortEmployment['2022'].employmentRatePct, 100);

const bad = analyzeCSV(samples('graduates-problems.csv'));
assert.equal(bad.status, 'partial');
assert.equal(bad.totalRows, 8);
assert.equal(bad.accepted, 4);
assert.equal(bad.rejected, 4);
assert.ok(bad.errors.some(e => e.reasons.includes('Duplicate graduate ID')));
assert.ok(bad.errors.some(e => e.reasons.includes('Employed graduate needs an industry')));
assert.ok(bad.errors.some(e => e.reasons.includes('Industry must be blank when not employed')));

assert.equal(analyzeCSV('wrong,header\na,b').status, 'invalid');
console.log('PASS: CSV parser, valid dataset, 4 rejected records, cohort statistics, error cases');
console.log(`Valid: ${good.accepted} accepted, ${good.employmentRatePct}% employed`);
console.log(`Problem sample: ${bad.accepted} accepted, ${bad.rejected} rejected`);
