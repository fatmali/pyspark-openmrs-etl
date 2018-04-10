import json

def getConfig():

    with open('config/config.json', 'r') as f:
        return json.load(f)

