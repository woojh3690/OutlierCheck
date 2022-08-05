from kafka_manager import KafkaManager
from json import loads, dumps


# 메인 클래스
class main(KafkaManager):

    def __init__(self):
        super().__init__("192.168.0.217", "9092")
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
    main = main()
    main.start()