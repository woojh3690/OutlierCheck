from analyze_result_sender import AnalyzeResultSender

import tensorflow as tf
import numpy as np

from multiprocessing import Process, Queue, Value
from queue import Empty
import joblib

SAVE_DIR = './tf_model/{}/{}'

class LstmWorker(Process):

    def __init__(self, process_id, queue: Queue, result_queue: Queue, modelMeta):
        super().__init__(name='worker-'+ str(process_id))
        self.id = process_id
        self.queue = queue
        self.result_queue = result_queue
        self.modelMeta = modelMeta
        self.model_path = SAVE_DIR.format(modelMeta['model_code'], modelMeta['model_ver'])
        self.scaler = joblib.load(SAVE_DIR.format(modelMeta['model_code'], 'scaler.pkl'))
        self.is_end = Value('i', 0)
        
    def run(self):
        lstm = tf.keras.models.load_model(self.model_path)
        while (not bool(self.is_end.value)):
            # 작업 큐에서 작업한개를 가져온다
            try:
                work = self.queue.get(timeout=5)
            except Empty as e:
                continue

            # TF 모델을 이용한 mse 계산
            np_data = work["data"]
            np_data_scale = self.scale(np_data)
            predict = lstm.predict(np_data_scale, verbose = 0)
            diff_data = self.flatten(np_data_scale) - self.flatten(predict)
            mse = np.mean(np.power(diff_data, 2), axis=1)[0]
            self.result_queue.put({"datetime": work["timestamp"], "mse": mse, "cur_row": work["cur_row"]})
        print("{}-id worker closed.".format(self.id))
    
    # 3차원 -> 2차원 변환
    def flatten(self, X):
        flattened_X = np.empty((X.shape[0], X.shape[2]))
        for i in range(X.shape[0]):
            flattened_X[i] = X[i, (X.shape[1]-1), :]
        return(flattened_X)

    # 스케일링
    def scale(self, X):
        for i in range(X.shape[0]):
            X[i, :, :] = self.scaler.transform(X[i, :, :])
        return X

    # 안전하게 스레드를 종료한다.
    def close(self):
        self.is_end.acquire()
        self.is_end.value = 1
        self.is_end.release()
