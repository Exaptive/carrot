import carrot

import time
import json

with open('config.json') as config_file:
    config = json.load(config_file)

print('starting')
client = carrot.Worker(config)

def suggestionsHandler(jobInfo):
  print(" [x] Got job info: {}".format(jobInfo))
  time.sleep(0.200)
  client.reportProgress(jobInfo, 0.25)
  time.sleep(0.200)
  client.reportProgress(jobInfo, 0.5)
  time.sleep(0.200)
  output = { 'calculation': 'here is a result' }
  ack = client.saveResult(
    jobInfo=jobInfo,
    success=True,
    output=output)
  return ack # finished (ack so the job isn't repeated)

print('register...')
client.register('suggestions', suggestionsHandler)