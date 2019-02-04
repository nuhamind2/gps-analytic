from geopy import distance
from common import strip

config = {
    "center": {"lat": -7.80108, "lon": 110.36454},
    "radius": 2000,
    "subject": "titik0"
}


def make_pipeline(source):

    def inside_circle(data):
        p = (data["payload"]["lat"], data["payload"]["lon"])
        c = (config["center"]["lat"], config["center"]["lon"])
        d = distance.distance(p, c).meters
        if d > config["radius"]:
            return False
        else:
            return True

    def encode(data):
        return dict(subject=config["subject"], data=data)

    pipeline = source.filter(inside_circle).map(strip).map(encode)
    return pipeline
