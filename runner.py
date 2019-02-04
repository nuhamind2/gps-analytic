import sys
import importlib
import asyncio
import logging
import json
from streamz import Stream
from nats.aio.client import Client as NATS

config = {
    "host": "127.0.0.1",
    "port": 4222,
    "subject": "tenant1.clientPublish.*.gps",
    "publish_prefix": "analytic"
}

def decode(msg):
    try:
        logging.debug("decode")
        return json.loads(msg)
    except:
        logging.debug("decode exception")
        return None

def _filter(data):
    if data:
        logging.debug("filter true")
        return True
    else:
        logging.debug("filter false")
        return False

def encode(data):
    try:
        logging.debug("encode")
        return dict(data=json.dumps(data["data"]),subject=data["subject"])
    except:
        logging.debug("encode exception")
        return None

async def run(loop, args):
    ncon = NATS()
    await ncon.connect(f'nats://{config["host"]}:{config["port"]}', loop=loop)
    logging.info("Connected to NATS")

    async def publish(msg):
        logging.debug("publish")
        await ncon.publish(f'{config["publish_prefix"]}.{args[1]}.{msg["subject"]}',bytes(msg["data"],'utf-8'))

    source = Stream(asynchronous=True)
    user_source = source.map(decode).filter(_filter)

    async def emitter(msg):
        logging.debug('New message')
        await source.emit(msg.data)
    if(len(args) < 2):
        logging.error('Argument required')
        return
    mod = importlib.import_module(args[1])
    user_sink = mod.make_pipeline(user_source)
    user_sink.map(encode).filter(_filter).sink(publish)

    await ncon.subscribe(config["subject"], cb=emitter)
    logging.info("Subscribed to NATS")
    while True:
        await asyncio.sleep(1)

if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',level=logging.INFO)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(loop, sys.argv))
