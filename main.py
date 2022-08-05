from kafka_manager import KafkaManager

import tensorflow as tf
import numpy as np

from json import loads, dumps
import sys

# LSTM 윈도우 크기
WINDOW_SIZE = 24

# 메인 클래스
class main(KafkaManager):

    def __init__(self, args):
        super().__init__(args[1], args[2])
        self.buffer = []
        self.lstm = tf.keras.models.load_model('tf_model/v2')
        self.lstm.summary()    

    def start(self):
        print("main_start")
        for message in self.consumer:
            try:
                jsonObj = loads(message.value)
            except: pass

            print("받은 메시지 : ", jsonObj)
            print("받은 시간 : ", jsonObj['msg_data']['timestamp'])
            print("json : ", jsonObj['msg_data']['features'])

    # 버퍼에 데이터를 저장한다. 
    # 버퍼에 저장되는 데이터 개수는 WINDOW_SIZE를 넘지 않는다.
    def push(self, value):
        self.buffer.append(value)
        if (self.buffer.count() > WINDOW_SIZE):
            self.buffer.remove(1) 

    # 버퍼가 가득찼으면 버퍼에 데이터를 이상감지 모델로 검사한다.
    def check_outlier(self):
        if (self.buffer.count() < WINDOW_SIZE):
            return
        
        np_data = np.array(self.buffer)
        predict = self.lstm.predict(np_data)
        diff_data = self.flatten(np_data) - self.flatten(predict)
        mse = np.mean(np.power(diff_data, 2), axis=1)

    # 3차원 -> 2차원 변환    
    def flatten(self, X):
        flattened_X = np.empty((X.shape[0], X.shape[2]))  # sample x features array.
        for i in range(X.shape[0]):
            flattened_X[i] = X[i, (X.shape[1]-1), :]
        return(flattened_X)

if __name__ == '__main__':
    main = main(sys.argv)
    main.start()