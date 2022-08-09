import tensorflow as tf
from multiprocessing import Process, Queue, Value
from queue import Empty
import numpy as np

class LstmWorker(Process):

    def __init__(self, process_id, queue: Queue, model_path, threshold):
        super().__init__(name='worker-'+ str(process_id))
        self.id = process_id
        self.queue = queue
        self.model_path = model_path
        self.THRESHOLD_MSE = threshold
        self.is_end = Value('i', 0)
        
    def run(self):
        lstm = tf.keras.models.load_model(self.model_path)
        while (not bool(self.is_end.value)):
            try:
                np_data = self.queue.get(timeout=5)
            except Empty as e:
                continue

            predict = lstm.predict(np_data, verbose = 0)
            diff_data = self.flatten(np_data) - self.flatten(predict)
            mse = np.mean(np.power(diff_data, 2), axis=1)[0]

            # 문제 없는 데이터면 다음 메시지 대기
            if (mse < self.THRESHOLD_MSE):
                print(self.id, "!문제 없음")
            else:
                print(self.id, "문제 있음")
        print("{} worker closed.".format(self.id))
    
    # 3차원 -> 2차원 변환
    def flatten(self, X):
        flattened_X = np.empty((X.shape[0], X.shape[2]))
        for i in range(X.shape[0]):
            flattened_X[i] = X[i, (X.shape[1]-1), :]
        return(flattened_X)

    def close(self):
        self.is_end.acquire()
        self.is_end.value = 1
        self.is_end.release()
