'use strict';

const expect = require('chai').expect;
const sinon = require('sinon');
const carrot = require('../../../carrot/js/carrot');
const config = require('../../config');

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

describe('carrot', function() {
  const jobType1 = 'suggestions';
  const jobInfo1 = {
    jobType: jobType1,
    args: { individual: false },
  };

  let producerClient1;
  let workerClient1;

  async function suggestionsHandler(jobInfo) {
    await sleep(2);
    workerClient1.reportProgress(jobInfo, 0.25);
    await sleep(2);
    workerClient1.reportProgress(jobInfo, 0.5);
    await sleep(2);
    const result = { success: true, result: 'here is a result' };
    const ack = workerClient1.saveResult({
      jobInfoJson: jobInfo,
      success: true,
      result,
    });
    return ack; // finished (ack so the job isn't repeated)
  }

  this.beforeEach(async function() {
    producerClient1 = await carrot.createProducer(config);
    workerClient1 = carrot.createWorker(config);
    // eslint-disable-next-line no-await-in-loop
    while (!producerClient1.ready || !workerClient1.ready) await sleep(2);
  });
  this.afterEach(async function() {
    await producerClient1.close();
    await workerClient1.close();
  });

  it('should pass jobs from producer to worker', async function() {
    const workCb = sinon.spy();
    await workerClient1.register(jobType1, workCb);
    await producerClient1.run(jobInfo1);
    await sleep(200);
    expect(workCb.callCount).to.equal(1);
  });

  it('should result in a stored job result', async function() {
    await workerClient1.register(jobType1, suggestionsHandler);
    const jobId = await producerClient1.run(jobInfo1);
    await sleep(200);
    const result = await producerClient1.getResult(jobId);
    expect(result.success).to.equal(true);
    expect(result.result).to.equal('here is a result');
  });

  it('should send progress and done events', async function() {
    await workerClient1.register(jobType1, suggestionsHandler);
    await producerClient1.run(jobInfo1);

    const progressCb = sinon.spy();
    const doneCb = sinon.spy();
    producerClient1.onJobProgressEvent(jobType1, progressCb);
    producerClient1.onJobDoneEvent(jobType1, doneCb);

    await sleep(200);

    expect(progressCb.callCount).to.equal(2);
    expect(doneCb.callCount).to.equal(1);
  });
});
