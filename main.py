from kafka_manager import KafkaManager
from lstm_worker import LstmWorker
from analyze_result_sender import AnalyzeResultSender

import numpy as np

from multiprocessing import Queue
from threading import Thread
import json
import sys

# 메인 클래스
class main(Thread, KafkaManager):

    def __init__(self, args):
        if (len(args) != 5): print('유효한 인자가 없습니다.')

        Thread.__init__(self)
        KafkaManager.__init__(self, args[1], args[2])

        # 모델 메타데이터 초기화
        for modelMeta in self.readModelMeta():
            if modelMeta['model_code'] == args[4]:
                self.modelMeta = modelMeta

        self.WINDOW_SIZE = self.modelMeta['window_size']    # 윈도우 사이즈
        self.buffer = []                                    # 데이터 버퍼
        self.work_queue = Queue()                           # Thread-safe 작업 명령 큐
        self.result_queue = Queue()                         # Thread-safe 작업 결과 큐
        self.workers = []                                   # 모델 실행 스레드 리스트
        self.is_end = False                                 # 종료 이벤트

        # AnalyzeResultSender 초기화
        result_sender = AnalyzeResultSender(self.producer, self.modelMeta, self.result_queue)
        result_sender.start()
        self.workers.append(result_sender)

        # TF 모델 초기화
        for id in range(int(args[3])):
            worker = LstmWorker(id, self.work_queue, self.result_queue, self.modelMeta)
            worker.start()
            self.workers.append(worker)

    def readModelMeta(self):
        modelsMeta = None
        with open("./conf/models_info.json", "r", encoding='utf-8') as file:
            modelsMeta = json.load(file)
        return modelsMeta

    # Override Thread
    def run(self):
        print("Listening kafka message...")
        while (not self.is_end):
            for message in self.consumer:
                try:
                    jsonObj = json.loads(message.value)
                    print("받은 메시지 : ", jsonObj)
                    self.push(jsonObj['features'])
                    self.check_outlier(jsonObj['timestamp'])
                except Exception as e:
                    print(e)
        print("End main...")
    
    # 버퍼에 데이터를 저장한다. 데이터 개수는 WINDOW_SIZE를 넘지 않는다.
    def push(self, value):
        self.buffer.append(value)
        if (len(self.buffer) > self.WINDOW_SIZE):
            self.buffer.pop(0)

    # 버퍼가 가득찼으면 버퍼에 데이터를 이상감지 모델로 검사한다.
    def check_outlier(self, timestamp):
        if (len(self.buffer) >= self.WINDOW_SIZE):
            np_data = np.array([self.buffer])
            self.work_queue.put({"timestamp": timestamp, "cur_row": self.buffer[-1], "data": np_data})

    # 종료
    def close(self):
        self.is_end = True
        for worker in self.workers: worker.close()
        for worker in self.workers: worker.join()
        super().kafka_close()

if __name__ == '__main__':
    main = main(sys.argv)
    main.start()
    main.join()