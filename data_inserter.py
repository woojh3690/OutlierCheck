import sys
import json
from datetime import datetime

import pandas as pd
from kafka import KafkaProducer

from module.model_meta import readModelMetas
from module.repeated_timer import RepeatedTimer

# Kafka 생성자 초기화
producer = KafkaProducer(
            acks=0, 
            bootstrap_servers = ["{}:{}".format(sys.argv[1], sys.argv[2])],
            value_serializer=lambda x: x.encode('utf-8')
        )

dfs = []
cursors = []
modelMetas = readModelMetas()

# 데이터 변형
select_model = -1
select_col = -1
change_value = -1

# SAMPLE_DATA = {"model_code": "", "timestamp":"2022-08-08 15:38:39","features": [219, 56319, 21848, 12741, 21.0, 4.0, 1]}
def sendTrainData(idx):
    # 데이터 메시지 생성
    msg = {}
    msg["model_code"] =  modelMetas[idx]['model_code']
    msg["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    temp = dfs[idx].iloc[cursors[idx], :].tolist()

    if (idx == select_model):
        temp[select_col] = change_value

    msg["features"] = temp

    # 데이터 전송
    strMsg = json.dumps(msg, ensure_ascii=False)
    producer.send("outlier_check", value=strMsg)
    #print("전송한 메시지 : ", strMsg)
    cursors[idx] += 1

if __name__ == '__main__':
    timers = []
    
    for idx, modelMeta in enumerate(modelMetas):
        # 데이터 프레임 초기화
        train_csv = './tf_model/{}/{}'.format(modelMeta['model_code'], modelMeta['train_csv'])
        dfs.append(pd.read_csv(train_csv, header=0, encoding='utf-8'))

        # 데이터 프레임 커서 초기화
        cursors.append(0)

        timer = RepeatedTimer(1, sendTrainData, idx)
        timers.append(timer)

    while True:
        print("자전거 : 0, 교통 : 1, 자전거 + 교통 : 2")
        select_model_temp = int(input("변형할 트윈 선택 : "))
        modelMeta = modelMetas[select_model]

        print("변형할 칼럼 선택", modelMeta["input_col_infos"])
        select_col_temp = int(input("변형할 칼럼 선택 : "))
        change_value = int(input("변형할 값 선택 : "))
        select_model = select_model_temp
        select_col = select_col_temp

        print("잘못된 데이터로 변형중입니다.")
        input("변형을 중시하시려면 아무키나 입력 : ")
        select_model = -1
        select_col = -1
        change_value = -1
