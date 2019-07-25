/* eslint-disable no-console */

'use strict';

const EventEmitter = require('events');
const amqp = require('amqplib');
const Redis = require('ioredis');

const protocol = require('./protocol');
const mongo = require('./mongo');


class Worker extends EventEmitter {
  constructor(config) {
    super();
    this.config = config;
  }

  async init() {
    this.broker = await amqp.connect(this.config.rabbitmq.url);
    this.broker.on('error', err => this.onBrokerError(err));
    this.broker.on('end', this.onBrokerEnd);
    this.channel = await this.broker.createChannel();
    this.redis = new Redis(this.config.redis.url);
  }
  onBrokerError(err) {
    console.log(' [x] broker error');
    this.emit('error', err);
  }

  onBrokerEnd() {
    console.log(' [x] broker end');
    this.emit('end');
  }

  /** Register a worker callback for a particular job type to do the work
   * the worker will typically call saveResult() to save results.
   *
   * The worker should return true  if the job should be acknowledged as completed.
   * Otherwise, the job may be repeated.
   *
   * @param {String} jobType
   * @param {CallableFunction} workerCallback
   */
  async register(jobType, workerCallback) {
    const jobQueueName = _jobQueueName(this.config, jobType);
    await _initQueueListen(this.channel, jobQueueName, async jobInfo => {
      const jobInfoJson = _messageToJson(jobInfo);
      await this.channel.assertQueue(jobInfoJson.eventReturnQueue, { durable: true });
      const ack = await workerCallback(jobInfoJson);
      if (ack) await this.channel.ack(jobInfo);
      await this.reportDone(jobInfoJson);
    });
  }

  /** Event: Report partial progress for a job and optional extra data
   *
   * @param {Object} jobInfo
   * @param {Number} progress
   * @param {Object} data
   */
  async reportProgress(jobInfo, progress, data = {}) {
    console.log(` [x] Reporting progress`);
    const eventInfo = {
      eventJobType: jobInfo.jobType,
      eventJobId: jobInfo.jobId,
      eventType: protocol.PROGRESS_EVENT_NAME,
      progress,
      data,
    };
    await this.channel.sendToQueue(jobInfo.eventReturnQueue,
      _jsonToBuffer(eventInfo), { persistent: true });
  }

  /** Event: Report a job as complete (which is different from sending results)
   * see saveResult()
   *
   * @param {*} jobInfo
   * @param {*} data
   */
  async reportDone(jobInfo, data = {}) {
    console.log(` [x] Reporting done`);
    const eventInfo = {
      eventJobType: jobInfo.jobType,
      eventJobId: jobInfo.jobId,
      eventType: protocol.DONE_EVENT_NAME,
      data,
    };
    await this.channel.sendToQueue(jobInfo.eventReturnQueue,
      _jsonToBuffer(eventInfo), { persistent: true });
  }

  /** Record the job results to redis and that it is finished in mongo
   *
   */
  async saveResult({ jobInfoJson, success, output }) {
    // record result in redis
    const resultKey = _resultKey(this.config, jobInfoJson.jobId);
    const result = { success, output };
    await this.redis.set(resultKey, JSON.stringify(result));

    // record job success/failure in mongo
    const outcomeCollection = await mongo.getCollection({
      config: this.config.mongo,
      dbName: this.config.mongo.jobsDbName,
      collectionName: this.config.mongo.jobOutcomeCollectionName });

    await outcomeCollection.updateOne({ _id: mongo.ObjectId(jobInfoJson.jobId) },
      {
        $set: {
          success,
          updatedAt: _now(),
        },
        $setOnInsert: {
          createdAt: _now(),
        },
      }, { upsert: true });

    // all bookkeeping is done, the worker can acknowledge the job as done now
    return true;
  }

  /** close the client
   *
   */
  async close() {
    console.log(' [x] cleanup...');
    if (this.redis) await this.redis.disconnect();
    if (this.channel) await this.channel.close();
    if (this.broker) await this.broker.close();
  }
}

class Producer extends EventEmitter {
  constructor(config) {
    super();
    this.config = config;
  }

  async init() {
    this.broker = await amqp.connect(this.config.rabbitmq.url);
    this.broker.on('error', err => this.onBrokerError(err));
    this.broker.on('end', this.onBrokerEnd);
    this.channel = await this.broker.createChannel();
    this.redis = new Redis(this.config.redis.url);
    this.eventReturnQueue = this.config.eventReturnQueue;
    await this.initEventListener();
  }

  onBrokerError(err) {
    console.log(' [x] broker error');
    this.emit('error', err);
  }

  onBrokerEnd() {
    console.log(' [x] broker end');
    this.emit('end');
  }

  /** Get the result of a job by ID
   *
   * @param {String} jobId
   */
  async getResult(jobId) {
    const resultKey = _resultKey(this.config, jobId);
    try {
      return JSON.parse(await this.redis.get(resultKey));
    } catch (error) {
      console.error(error);
      throw new Error(error);
    }
  }

  /** Submit a job to the worker pool
   *
   * @param {String} jobType
   * @param {Object} args
   */
  async run({ jobType, args } = {}) {
    if (!jobType) throw new Error('jobType must be defined');

    // record job in mongo
    const jobCollection = await mongo.getCollection({
      config: this.config.mongo,
      dbName: this.config.mongo.jobsDbName,
      collectionName: this.config.mongo.jobsCollectionName });

    const eventReturnQueue = this.eventReturnQueue;
    const jobInfo = { jobType, eventReturnQueue, args };
    const res = await jobCollection.insertOne(jobInfo);

    // update job in mongo based on job id
    const jobId = res._id || res.insertedId;
    const resultKey = _resultKey(this.config, jobId);
    jobInfo.jobId = jobId;
    jobInfo.resultKey = resultKey;
    await jobCollection.updateOne({ _id: jobId },
      {
        $set: {
          resultKey,
          createdAt: _now(),
          updatedAt: _now(),
        },
      }, { upsert: false });

    // send to amqp/worker
    const jobQueue = _jobQueueName(this.config, jobType);
    await this.channel.assertQueue(jobQueue, { durable: true });
    console.log(` [x] Sending: job ${jobId} of type ${jobType} to ${jobQueue}`);
    await this.channel.sendToQueue(jobQueue, _jsonToBuffer(jobInfo), { persistent: true });
    console.log(` [x] Sent: job ${jobId}`);
    return jobId;
  }

  onJobEvent(jobType, eventType, callback) {
    const key = _callbackKey(jobType, eventType);
    console.log(` [x] registering callback for ${key}`);
    this.on(_callbackKey(jobType, eventType), info => callback(_messageToJson(info)));
  }

  onJobProgressEvent(jobType, callback) {
    return this.onJobEvent(jobType, protocol.PROGRESS_EVENT_NAME, callback);
  }

  onJobDoneEvent(jobType, callback) {
    return this.onJobEvent(jobType, protocol.DONE_EVENT_NAME, callback);
  }

  /** begin listening for job events
   *
   */
  async initEventListener() {
    await _initQueueListen(this.channel, this.eventReturnQueue, async eventInfo => {
      const eventInfoJSON = _messageToJson(eventInfo);
      console.log(` [x] event: ${JSON.stringify(eventInfoJSON)}`);
      const eventType = eventInfoJSON.eventType;
      const eventJobType = eventInfoJSON.eventJobType;
      const eventKey = _callbackKey(eventJobType, eventType);
      console.log(` [x] emit: ${eventKey}`);
      await this.emit(eventKey, eventInfo);
      await this.channel.ack(eventInfo);
    });
  }

  /** close the client
   *
   */
  async close() {
    console.log(' [x] cleanup...');
    await (await mongo.closeConnection());
    if (this.channel) await this.channel.close();
    if (this.broker) await this.broker.close();
    if (this.redis) await this.redis.disconnect();
  }
}

function _now() {
  return new Date().getTime();
}

function _callbackKey(jobType, eventType) {
  return `${jobType}-${eventType}`;
}

function _resultKey(config, jobId) {
  return `${config.resourcePrefix}_${config.redis.jobResultsSuffix}_${jobId}`;
}

function _messageToJson(message) {
  return JSON.parse(message.content.toString());
}

function _jsonToBuffer(json) {
  return Buffer.from(JSON.stringify(json));
}

async function createWorker(config) {
  const worker = new Worker(config);
  await worker.init();
  return worker;
}

async function createProducer(config) {
  const producer = new Producer(config);
  await producer.init();
  return producer;
}

function _jobQueueName(config, jobType) {
  return `${config.resourcePrefix}_${jobType}_${config.rabbitmq.jobQueueSuffix}`;
}

async function _initQueueListen(channel, queueName, callback) {
  await channel.assertQueue(queueName, { durable: true });
  await channel.prefetch(1); // evenly distribute amongst workers
  // manual acknowledgment mode,
  // see https://www.rabbitmq.com/confirms.html for details
  await channel.consume(queueName, async msg => callback(msg), { noAck: false });
}

module.exports = {
  createWorker,
  createProducer,
};
