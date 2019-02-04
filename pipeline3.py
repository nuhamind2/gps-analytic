from geopy import distance
from time import time
import decimal

config = {
    "window": 4,
    "subject": "collectivedistance"
}


def _filter(data):
    if data:
        return True
    else:
        return False


def make_pipeline(source):

    def compute_distance(state, data):
        clientid = data["clientId"]
        if clientid in state:
            prev = (state[clientid]["lat"], data["payload"]["lon"])
            current = (data["payload"]["lat"], data["payload"]["lon"])
            state[clientid]["lat"] = data["payload"]["lat"]
            state[clientid]["lon"] = data["payload"]["lon"]
            d = distance.distance(prev, current).meters
            return state, dict(distance=d, time=time())
        else:
            state[clientid] = dict()
            state[clientid]["lat"] = data["payload"]["lat"]
            state[clientid]["lon"] = data["payload"]["lon"]
            return state, None

    def acc_distance(state, data):
        if(data["time"] - state["begin"] > config["window"]):
            new_state = dict(begin=start_window(data["time"]), sum=0)
            return new_state, state
        else:
            state["sum"] += data["distance"]
            return state, None

    def encode(data):
        return dict(subject=config["subject"], data=dict(value=data["sum"], axis=data["begin"]))

    def start_window(epoch):
        now = int(epoch)
        return now - now % config["window"]

    pipeline = source.\
        accumulate(compute_distance, returns_state=True, start=dict()).\
        filter(_filter).\
        accumulate(acc_distance, returns_state=True, start=dict(sum=0, begin=start_window(time()))).\
        filter(_filter).\
        map(encode)

    return pipeline
