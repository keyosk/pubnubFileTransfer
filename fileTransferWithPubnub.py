from gevent import monkey
monkey.patch_all()

import sys
import copy
import urllib
import urllib2
import json
import random
import signal
import gevent
import gevent.pool
import gevent.event
import base64
import logging
import magic
import time
import os.path
import uuid

if len(sys.argv) == 1:
  sys.exit('ERROR : You must provide a file to publish.')

mime = magic.Magic(mime=True)

global_stop = gevent.event.Event()

file_path = sys.argv[1]

#################################################################################

num_workers = 20

sleep_time = 0.1

chunk_length = 25000

number_channels = 20

channel_prefix = 'pubnubFileTransferV1.0-'

pub_key = 'pub-c-8dbb9b60-466c-43dd-b4dd-f7b5692f3fc2'

sub_key = 'sub-c-374a0d66-3f63-11e4-8637-02ee2ddab7fe'

rounds_complete = 0

#################################################################################

encoded_file = 'data:image/png;base64,' + base64.encodestring(open(file_path,'rb').read()).replace('\n', '')

encoded_file_list = [encoded_file[i:i+chunk_length] for i in range(0, len(encoded_file), chunk_length)]

encoded_file_mime = mime.from_file(file_path)

total_packets = len(encoded_file_list)

encoded_file_modified = time.ctime(os.path.getmtime(file_path))

encoded_file_unique = str(int(time.time())) + '' + str(uuid.uuid4())

encoded_file_name = os.path.basename(file_path)

channels = [channel_prefix + str(i) for i in range(0, number_channels)]

#################################################################################

def pubnub_publish(channel=None,pub_key='demo',sub_key='demo',origin='pubsub.pubnub.com',data=None):
    if channel == None or data == None:
      return None
    payload = urllib.quote(json.dumps(data))
    url = 'http://%s/publish/%s/%s/0/%s/0/' % (origin, pub_key, sub_key, channel)
    try:
        return urllib2.urlopen(url + payload, timeout=1).read()
    except Exception as e:
        logging.exception(e)
        print('FAILED TO PUBLISH %s' % (channel,))
        sys.exit()

def stop():
    print('Stopping Worker...')
    global_stop.set()

class Reserver(gevent.Greenlet):

    def __init__(self, exit_signal):
        super(Reserver, self).__init__()
        self._exit_signal = exit_signal

    def _run(self):

        global rounds_complete

        while not self._exit_signal.is_set():

          len_encoded_file_list = len(encoded_file_list)

          if rounds_complete == 500:
            stop()

          if len_encoded_file_list != 0:

            data = encoded_file_list.pop()

            index = len_encoded_file_list

            data = {
              'name' : encoded_file_name,
              'unique' : encoded_file_unique,
              'event' : 'packet',
              'totalPackets' : total_packets,
              'packetNum' : index,
              'packet' : data,
              'modified' : encoded_file_modified,
              'type' : encoded_file_mime
            }
            channel = random.choice(channels)
            result = pubnub_publish(channel=channel,data=data,pub_key=pub_key,sub_key=sub_key)
            print "publish packet %s of %s to %s : %s" % (index, total_packets, channel, result)
          else:
            rounds_complete = rounds_complete + 1

          gevent.sleep(sleep_time)

gevent.signal(signal.SIGINT, stop)
gevent.signal(signal.SIGTERM, stop)

reservers = gevent.pool.Pool(num_workers, Reserver)

while not global_stop.is_set():

    while not reservers.full():
        reservers.spawn(exit_signal=global_stop)

    gevent.sleep(0.1)

print('Stopping Reservers')
reservers.join()
print('Reservers Shutdown')