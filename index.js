'use strict';
// Separate Lambda: deliberately does not replace the working "wade-hello" function.
const { S3Client, GetObjectCommand, PutObjectCommand } = require('@aws-sdk/client-s3');
const { analyzeCSV } = require('./processor');

exports.handler = async (event) => {
  const endpoint = process.env.AWS_ENDPOINT_URL;
  if (!endpoint) throw new Error('Missing local AWS_ENDPOINT_URL');
  const s3 = new S3Client({
    endpoint,
    region: process.env.AWS_REGION || 'us-east-1',
    forcePathStyle: true,
    credentials: { accessKeyId: 'test', secretAccessKey: 'test' }
  });
  const processed = [];
  for (const record of event.Records || []) {
    if (record.eventSource !== 'aws:s3' || !record.s3) continue;
    const key = decodeURIComponent(record.s3.object.key.replace(/\+/g, ' '));
    if (!key.startsWith('incoming/') || !key.toLowerCase().endsWith('.csv')) continue;
    const filename = key.slice('incoming/'.length);
    const outputKey = 'reports/' + filename.replace(/\.csv$/i, '.json');
    const result = await s3.send(new GetObjectCommand({
      Bucket: record.s3.bucket.name, Key: key
    }));
    const csv = await result.Body.transformToString();
    const report = analyzeCSV(csv);
    await s3.send(new PutObjectCommand({
      Bucket: 'wade-graduates-out', Key: outputKey,
      Body: JSON.stringify({ source: key, ...report }, null, 2),
      ContentType: 'application/json'
    }));
    console.log('WADE_GRADUATES_REPORT', key, report.status, report.accepted, report.rejected);
    processed.push({ input: key, output: outputKey, status: report.status });
  }
  return { processed };
};
