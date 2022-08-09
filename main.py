from kafka_manager import KafkaManager
from lstm_worker import LstmWorker

import numpy as np

from json import loads, dumps
from multiprocessing import Queue
from threading import Thread
import sys

WINDOW_SIZE = 24        # lstm 윈도우 크기
FEATURE_SIZE = 6        # lstm 윈도우 크기
THRESHOLD_MSE = 0.4     # 기준 mse

SAVE_DIR = './tf_model/{}'
MODEL_VER = 'v2'
SCALER_NAME = 'scaler.pkl'

# 메인 클래스
class main(Thread, KafkaManager):

    def __init__(self, args):
        if (len(args) != 4): print('유효한 인자가 없습니다.')

        KafkaManager.__init__(self, args[1], args[2])
        Thread.__init__(self)
        self.buffer = []
        self.work_queue = Queue()
        self.workers = []
        self.is_end = False

        for id in range(int(args[3])):
            worker = LstmWorker(
                id,
                self.work_queue,
                SAVE_DIR.format(MODEL_VER), 
                SAVE_DIR.format(SCALER_NAME), 
                THRESHOLD_MSE
            )
            worker.start()
            self.workers.append(worker)

    def run(self):
        print("Listening kafka message...")
        while (not self.is_end):
            for message in self.consumer:
                try:
                    jsonObj = loads(message.value)
                    print("받은 메시지 : ", jsonObj)
                    self.push(jsonObj['features'])
                    self.check_outlier()
                except Exception as e:
                    print(e)
        self.kafka_close()
        print("End kafka listening...")
    
    # 버퍼에 데이터를 저장한다. 데이터 개수는 WINDOW_SIZE를 넘지 않는다.
    def push(self, value):
        self.buffer.append(value)
        if (len(self.buffer) > WINDOW_SIZE):
            self.buffer.pop(0)

    # 버퍼가 가득찼으면 버퍼에 데이터를 이상감지 모델로 검사한다.
    def check_outlier(self):
        if (len(self.buffer) >= WINDOW_SIZE):
            np_data = np.array([self.buffer])
            self.work_queue.put(np_data)

    # 종료
    def close(self):
        self.is_end = True
        for worker in self.workers: worker.close()
        for worker in self.workers: worker.join()

if __name__ == '__main__':
    main = main(sys.argv)
    main.start()
    main.join()