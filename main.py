from kafka_manager import KafkaManager

# 메인 클래스
class main(KafkaManager):
    def __init__(self):
        super().__init__("192.168.0.217", "9092")

    def start(self):
        print("main_start")
        for message in self.consumer:
            print("Topic: {}, Partition: {}, Offset: {}, Key: {}, Value: {}"
                .format(message.topic, message.partition, message.offset, message.key, message.value)
            )

if __name__ == '__main__':
    main = main()
    main.start()