#!/usr/bin/env node

'use strict';

const { loadConfig } = require('./config');
const carrot = require('../carrot/js/carrot');

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

let client;

async function suggestionsHandler(jobInfo) {
  // eslint-disable-next-line no-console
  console.log(` [x] Got job info: ${JSON.stringify(jobInfo)}`);
  await sleep(200);
  client.reportProgress(jobInfo, 0.25);
  await sleep(200);
  client.reportProgress(jobInfo, 0.5);
  await sleep(200);
  const output = { calculation: 'here is a result' };
  const ack = client.saveResult({
    jobInfoJson: jobInfo,
    success: true,
    output,
  });
  return ack; // finished (ack so the job isn't repeated)
}

(async() => {
  const config = await loadConfig();
  client = await carrot.createWorker(config);
  const jobName = 'suggestions';
  await client.register(jobName, suggestionsHandler);
  await sleep(100000);
  await client.close();
  process.exit(0);
})();
