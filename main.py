from kafka_manager import KafkaManager
from json import loads, dumps

import sys

# 메인 클래스
class main(KafkaManager):

    def __init__(self, args):
        super().__init__(args[1], args[2])
        self.buffer = []

    def start(self):
        print("main_start")
        for message in self.consumer:
            try:
                jsonObj = loads(message.value)
            except: pass

            print("받은 메시지 : ", jsonObj)
            print("받은 시간 : ", jsonObj['msg_data']['timestamp'])
            print("json : ", jsonObj['msg_data']['features'])

if __name__ == '__main__':
    main = main(sys.argv)
    main.start()