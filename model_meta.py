import json

def readModelMetas():
    modelsMeta = None
    with open("./conf/models_info.json", "r", encoding='utf-8') as file:
        modelsMeta = json.load(file)
    return modelsMeta

def readModelMeta(model_code: str):
    for modelMeta in readModelMetas():
        if modelMeta['model_code'] == model_code:
            return modelMeta