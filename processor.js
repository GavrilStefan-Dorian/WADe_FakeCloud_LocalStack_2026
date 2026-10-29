'use strict';

// Pure data-processing logic: no AWS dependencies and no Docker required.
// CSV records have no names or real student information.
const COLUMNS = [
  'graduate_id', 'graduation_year', 'program',
  'industry', 'city', 'status'
];
const STATUSES = new Set(['employed', 'seeking', 'studying']);

function parseCSV(text) {
  const source = text.replace(/^\uFEFF/, '');
  const records = [];
  let row = [], value = '', mode = 'start', line = 1, rowLine = 1;
  function addField() { row.push(value); value = ''; mode = 'start'; }
  function addRow() {
    addField();
    if (row.some(field => field.trim() !== '')) {
      records.push({ line: rowLine, fields: row });
    }
    row = [];
    rowLine = line + 1;
  }
  for (let i = 0; i < source.length; i++) {
    const char = source[i];
    if (mode === 'quoted') {
      if (char === '"') {
        if (source[i + 1] === '"') { value += '"'; i++; }
        else mode = 'afterQuote';
      } else if (char === '\r') {
        if (source[i + 1] === '\n') i++;
        value += '\n'; line++;
      } else { value += char; if (char === '\n') line++; }
      continue;
    }
    if (char === '"') {
      if (mode !== 'start') throw new Error(`Unexpected quote on line ${line}`);
      mode = 'quoted';
    } else if (char === ',') {
      addField();
    } else if (char === '\n' || char === '\r') {
      if (char === '\r' && source[i + 1] === '\n') i++;
      addRow(); line++;
      rowLine = line;
    } else if (mode === 'afterQuote') {
      if (char !== ' ' && char !== '\t') {
        throw new Error(`Unexpected character after quote on line ${line}`);
      }
    } else {
      value += char;
      mode = 'unquoted';
    }
  }
  if (mode === 'quoted') throw new Error(`Unclosed quoted field on line ${line}`);
  if (value.length || row.length || mode === 'afterQuote') addRow();
  return records;
}

function countBy(rows, field) {
  const counts = {};
  for (const row of rows) counts[row[field]] = (counts[row[field]] || 0) + 1;
  return Object.fromEntries(Object.entries(counts).sort(([a], [b]) => a.localeCompare(b)));
}

function analyzeCSV(csvText) {
  const report = {
    status: 'invalid', totalRows: 0, accepted: 0, rejected: 0,
    employmentRatePct: 0,
    byStatus: {}, byProgram: {}, byIndustry: {}, byCity: {},
    byGraduationYear: {},
    cohortEmployment: {},
    acceptedRecords: [], errors: []
  };

  let csvRows;
  try { csvRows = parseCSV(csvText); }
  catch (err) { report.errors.push({ line: null, reasons: [err.message] }); return report; }

  if (!csvRows.length || csvRows[0].fields.map(x => x.trim().toLowerCase()).join(',') !== COLUMNS.join(',')) {
    report.errors.push({ line: 1, reasons: [`Expected columns: ${COLUMNS.join(',')}`] });
    return report;
  }
  const seenIds = new Set();
  const accepted = [];
  for (const {line, fields} of csvRows.slice(1)) {
    report.totalRows++;
    const [id, year, program, industry, city, status] = fields.map(x => x.trim());
    const reasons = [];
    if (fields.length !== COLUMNS.length) reasons.push('Expected 6 columns');
    if (!/^[A-Za-z0-9_-]{2,20}$/.test(id || '')) reasons.push('Invalid graduate ID');
    // A duplicate ID is rejected even if the earlier row had other errors.
    if (id && seenIds.has(id)) reasons.push('Duplicate graduate ID');
    if (id) seenIds.add(id);
    if (!/^\d{4}$/.test(year || '') || Number(year) < 2015 || Number(year) > 2026) {
      reasons.push('Graduation year must be between 2015 and 2026');
    }
    if (!program) reasons.push('Missing study program');
    if (!city) reasons.push('Missing city');
    if (!STATUSES.has(status)) reasons.push('Status must be employed, seeking or studying');
    if (status === 'employed' && !industry) reasons.push('Employed graduate needs an industry');
    if (status !== 'employed' && industry) reasons.push('Industry must be blank when not employed');

    if (reasons.length) {
      report.errors.push({ line, graduateId: id || '(missing)', reasons });
      continue;
    }
    accepted.push({graduateId: id, graduationYear: year, program, industry, city, status});
  }

  report.accepted = accepted.length;
  report.rejected = report.totalRows - report.accepted;
  report.status = report.accepted === 0 ? 'invalid' : report.rejected ? 'partial' : 'ok';
  report.employmentRatePct = accepted.length ?
    Math.round(accepted.filter(r => r.status === 'employed').length / accepted.length * 1000) / 10 : 0;
  report.byStatus = countBy(accepted, 'status');
  report.byProgram = countBy(accepted, 'program');
  report.byIndustry = countBy(accepted.filter(r => r.status === 'employed'), 'industry');
  report.byCity = countBy(accepted, 'city');
  report.byGraduationYear = countBy(accepted, 'graduationYear');
  const years = [...new Set(accepted.map(r => r.graduationYear))].sort();
  for (const year of years) {
    const cohort = accepted.filter(r => r.graduationYear === year);
    const employed = cohort.filter(r => r.status === 'employed').length;
    report.cohortEmployment[year] = {
      total: cohort.length, employed,
      employmentRatePct: Math.round(employed / cohort.length * 1000) / 10
    };
  }
  report.acceptedRecords = accepted;
  return report;
}

module.exports = {parseCSV, analyzeCSV};
