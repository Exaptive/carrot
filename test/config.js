'use strict';

const config = {
  mongo: {
    url: process.env.CARROT_MONGO_URL || 'mongodb://localhost',
    use_ssl: false,
    options: {
      useNewUrlParser: true,
    },
    jobsCollectionName: 'jobs',
    jobOutcomeCollectionName: 'jobOutcome',
    jobsDbName: 'cognet2_test',
  },
  rabbitmq: {
    url: process.env.CARROT_RABBITMQ_URL || 'amqp://rabbitmq:rabbitmq@localhost',
    jobQueueSuffix: 'jobs',
  },
  redis: {
    url: process.env.CARROT_REDIS_URL || 'redis://127.0.0.1/0',
    jobResultsSuffix: 'results',
  },
  resourcePrefix: 'test',
};

module.exports = config;

