import re
from common import strip

config = {
    "pattern" : '^asset_a[A-Z]$',
    "subject" : "groupa"
}


def make_pipeline(source):
    pat = re.compile(config["pattern"])

    def match_subgroup(data):
        if pat.findall(data["clientId"]):
            return True
        else:
            return False

    def encode(data):
        return dict(subject=config["subject"], data = data)
    pipeline = source.filter(match_subgroup).map(strip).map(encode)
    return pipeline
