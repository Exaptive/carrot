'use strict';

const fs = require('fs');


async function loadConfig(filename = 'config.json') {
  const data = fs.readFileSync(filename);
  return JSON.parse(data);
}


module.exports = { loadConfig };
