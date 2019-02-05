def strip(data):
        return dict(assetId = data["clientId"],time = data["payload"]["time"],
        lon = data["payload"]["lon"],lat = data["payload"]["lat"])