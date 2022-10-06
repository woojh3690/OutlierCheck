from kafka import KafkaConsumer, KafkaProducer
from json import loads, dumps

class KafkaManager:

    def __init__(self, ip, port):
        server_url = "{}:{}".format(ip, port)

        # Kafka 소비자 초기화
        self.consumer = KafkaConsumer(
            'outlier_check',
            bootstrap_servers=[server_url],
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='outlier_checker',
            value_deserializer=lambda x: x.decode('utf-8'),
            consumer_timeout_ms=5000
        )
        
        # Kafka 생성자 초기화
        self.producer = KafkaProducer(
            acks=0, 
            bootstrap_servers = [server_url],
            value_serializer=lambda x: x.encode('utf-8')
        )
    
    def kafka_close(self):
        self.consumer.close()
        self.producer.close()
        print("Destroyed KafkaManager...")

