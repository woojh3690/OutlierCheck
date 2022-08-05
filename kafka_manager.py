from kafka import KafkaConsumer, KafkaProducer

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
            bootstrap_servers = [server_url]
        )

