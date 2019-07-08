#!/usr/bin/env node

/* eslint-disable no-console */

'use strict';

const { loadConfig } = require('./config');
const carrot = require('../carrot/js/carrot');

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

(async() => {
  const config = await loadConfig();
  const client = await carrot.createProducer(config);
  // eslint-disable-next-line no-await-in-loop
  while (!client.ready) await sleep(100);
  const jobType = 'suggestions';
  const jobInfo = {
    jobType,
    args: { individual: false },
  };
  const jobId = await client.run(jobInfo);
  console.log(` [x] Sent job ${jobId}`);

  client.onJobProgressEvent(jobType, info => {
    console.log(` [x] progress: ${JSON.stringify(info)}`);
  });

  client.onJobDoneEvent(jobType, async info => {
    console.log(` [x] done: ${JSON.stringify(info)}`);
  });

  await sleep(5000);

  const result = await client.getResult(jobId);
  console.log(` [x] result = ${JSON.stringify(result)}`);
  process.exit(0);
})();
