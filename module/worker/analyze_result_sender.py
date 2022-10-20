from multiprocessing import Queue, Value
from threading import Thread
from queue import Empty
from copy import deepcopy
import json


class AnalyzeResultSender(Thread):

    def __init__(self, kafkaProducer, modelMeta, queue: Queue):
        super().__init__()
        self.kafkaProducer = kafkaProducer
        self.modelMeta = modelMeta
        self.queue = queue
        self.is_end = Value('i', 0)
    
    # Override
    def run(self):
        while (not bool(self.is_end.value)):
            # 작업 큐에서 작업한개를 가져온다
            try:
                data = self.queue.get(timeout=5)
            except Empty as e:
                continue
            self.send_to_web(data["datetime"], data['mse'], data['cur_row'])
        print("Analyze result close...")

    # web 에 전송할 메시지를 만들고 web topic 으로 데이터를 전송한다.
    def send_to_web(self, datetime: str, mse: float, cur_row: list):
        send_msg = self.create_send_msg(datetime, mse, cur_row)
        self.kafkaProducer.send(self.modelMeta["model_code"], value=send_msg)

    # web 에 전송할 메시지 생성
    def create_send_msg(self, datetime: str, mse: float, cur_row: list):
        send_msg = self.get_default_json()

        # datetime 설정
        send_msg["datetime"] = datetime

        # input_data 설정
        input_data = []
        for idx, field in enumerate(self.modelMeta["input_col_infos"]):
            input_data.append({"name": field, "value": cur_row[idx]})
        send_msg["input_data"] = input_data

        # mse 설정
        send_msg["analysis"]["mse"] = mse

        value = None
        name = None
        for threshold in self.modelMeta["threshold"]:
            cur_value = threshold["value"]
            if (mse > cur_value and (value is None or cur_value > value)):
                value = cur_value
                name = threshold["name"]
        
        # error_data 설정
        error_data = deepcopy(input_data)
        if (name is not None):
            error_data.append({"name": "mse", "value": mse})
            error_data.append({"name": "판단 결과", "value": name})
            send_msg["analysis"]["error_data"] = error_data

        json_data = json.dumps(send_msg, ensure_ascii=False)
        return json_data

    # 기본 json 메시지 포맷 가져오기
    def get_default_json(self):
        return {
            "datetime": None,
            "input_data": [],
            "analysis": {
                "mse": None,
                "threshold": self.modelMeta["threshold"]
            }
        }
            
    # 안전하게 스레드를 종료한다.
    def close(self):
        self.is_end.acquire()
        self.is_end.value = 1
        self.is_end.release()
