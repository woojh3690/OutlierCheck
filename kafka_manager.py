from kafka import KafkaConsumer, KafkaProducer
from json import loads, dumps
import time

class KafkaManager:

    def __init__(self, ip, port):
        server_url = "{}:{}".format(ip, port)

        self.consumer = KafkaConsumer(
            'outlier_check',
            bootstrap_servers=[server_url],
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='outlier_checker'
        )
        
        self.producer = KafkaProducer(
            acks=0, 
            bootstrap_servers = [server_url], 
            value_serializer = lambda x: dumps(x).encode('utf-8')
        )

