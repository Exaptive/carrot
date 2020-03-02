import mongo
import protocol

import time
import math

import pika
import redis

from functools import partial
import copy
import json

from bson.objectid import ObjectId
import bson
import threading
import Queue

class Worker:
    def __init__(self, config):
        self.config = config
        self.ready = False
        self.broker = pika.BlockingConnection(pika.URLParameters(config['rabbitmq']['url']))
        self.channel = self.broker.channel()
        self.redis = redis.Redis.from_url(config['redis']['url'])
        self.mongo = mongo.connect(config['mongo'])
        self.exchangeName = ''
        self.ready = True

    def register(self, jobType, workerCallback):
        jobQueueName = self._jobQueueName(jobType)
        self._initQueueListen(jobQueueName, workerCallback)

    def reportDone(self, jobInfo, data = {}):
        print(' [x] Reporting done')
        eventInfo = {
            'eventJobType': jobInfo['jobType'],
            'eventJobId': jobInfo['jobId'],
            'eventType': protocol.DONE_EVENT_NAME,
            'data': data,
        }
        self.channel.basic_publish(exchange=self.exchangeName,
            routing_key=jobInfo['eventReturnQueue'],
            body=json.dumps(eventInfo),
            properties=pika.BasicProperties(delivery_mode=2)) # persistent message

    def reportProgress(self, jobInfo, progress, data = {}):
        p = partial(self._reportProgress, jobInfo, progress, data)
        self.broker.add_callback_threadsafe(p)

    def _reportProgress(self, jobInfo, progress, data = {}):
        print(' [x] Reporting progress')
        eventInfo = {
            'eventJobType': jobInfo['jobType'],
            'eventJobId': jobInfo['jobId'],
            'eventType': protocol.PROGRESS_EVENT_NAME,
            'progress': progress,
            'data': data,
        }
        self.channel.basic_publish(exchange=self.exchangeName,
            routing_key=jobInfo['eventReturnQueue'],
            body=json.dumps(eventInfo),
            properties=pika.BasicProperties(delivery_mode=2)) # persistent message

    def saveResult(self, jobInfo, success, output, output_ttl = None, artifacts = {}):
        p = partial(self._saveResult, jobInfo, success, output, output_ttl, artifacts)
        self.broker.add_callback_threadsafe(p)

    def _saveResult(self, jobInfo, success, output, output_ttl = None, artifacts = {}):
        # record result in redis
        _id = ObjectId(jobInfo['jobId'])
        resultKey = self._resultKey(_id)
        result = {
            'success': success,
            'output': output,
        }
        if output_ttl:
            self.redis.set(resultKey, json.dumps(result), ex=output_ttl)
        else:
            self.redis.set(resultKey, json.dumps(result))

        # record job success/failure in mongo
        mongo.update_record(
            client=self.mongo,
            db_name=self.config['mongo']['jobsDbName'],
            collection_name=self.config['mongo']['jobOutcomeCollectionName'],
            query={ '_id': _id },
            operation={
                '$set': {
                    'success': success,
                    'artifacts': artifacts,
                    'updatedAt': self._now(),
                },
                '$setOnInsert': {
                    'createdAt': self._now(),
                },
            },
            upsert=True)

        # all bookkeeping is done, the worker can acknowledge the job as done now
        return True

    def close(self):
        print(' [x] cleanup...')
        self.mongo.close()
        self.broker.close()
        # the redis.Redis connection pool (self.redis) handles disconnect

    def _threaded_callback(self, workerCallback, jobInfo, queue):
        ack = workerCallback(jobInfo)
        queue.put(ack)

    def _callback(self, workerCallback, ch, method, properties, body):
        jobInfo = json.loads(body)
        # make sure event queue exists for the worker
        # it is more efficient to do this once per job than in reportProgress/reportDone
        eventReturnQueue = jobInfo['eventReturnQueue']
        self.channel.queue_declare(eventReturnQueue, durable=True)

        # Run the callback using a thread so the main thread can handle heartbeats
        queue = Queue.Queue()
        t = threading.Thread(
            target=self._threaded_callback,
            args=(workerCallback, jobInfo, queue)
        )
        t.start()

        while t.is_alive():
            # Give pika time to process other events
            self.broker.process_data_events()

            # Pause the loop, but in a safer way for pika and broker communication
            self.broker.sleep(15)
        ack = queue.get()

        if (ack):
            self.channel.basic_ack(delivery_tag=method.delivery_tag)
        self.reportDone(jobInfo)

    def _callbackKey(self, jobType, eventType):
        return "{}-{}".format(jobType, eventType)

    def _initQueueListen(self, jobQueueName, workerCallback):
        self.channel.basic_qos(prefetch_count=1) # must be be before queue_declare and basic_consume
        self.channel.queue_declare(queue=jobQueueName, durable=True)
        boundCallback = partial(self._callback, workerCallback)
        self.channel.basic_consume(queue=jobQueueName,
            auto_ack=False,
            on_message_callback=boundCallback)
        print('consume...')
        self.channel.start_consuming()

    def _jobQueueName(self, jobType):
        return '{}_{}_{}'.format(self.config['resourcePrefix'], jobType, self.config['rabbitmq']['jobQueueSuffix'])

    def _now(self):
        # compatibility with js Date.getTime()
        return float(math.trunc(time.time()*1000.0))

    def _resultKey(self, jobId):
        return "{}_{}_{}".format(self.config['resourcePrefix'], self.config['redis']['jobResultsSuffix'], jobId)
