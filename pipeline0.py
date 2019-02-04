import re
from common import strip

config = {
    "subject" : "all"
}


def make_pipeline(source):    
    def encode(data):
        return dict(subject=config["subject"], data = data)
    pipeline = source.map(strip).map(encode)
    return pipeline
