from main import main
from datetime import datetime, timedelta
import time
import sys
import json

SAMPLE_DATA = {"timestamp":"2022-08-08 15:38:39","features": [219, 56319, 21848, 12741, 21.0, 4.0, 1]}

if __name__ == '__main__':
    main = main(sys.argv)
    main.start()

    time.sleep(20)

    now = datetime.now()

    # 모델 초기화
    for i in range(20):
        now += timedelta(seconds=1)
        SAMPLE_DATA["timestamp"] = now.strftime("%Y-%m-%d %H:%M:%S")
        send_msg = json.dumps(SAMPLE_DATA)
        main.producer.send("outlier_check", value=send_msg)

    time.sleep(10)

    # 모델 성능 측정
    for i in range(10000):
        now += timedelta(seconds=1)
        SAMPLE_DATA["timestamp"] = now.strftime("%Y-%m-%d %H:%M:%S")
        send_msg = json.dumps(SAMPLE_DATA)
        main.producer.send("outlier_check", value=send_msg)
    
    time.sleep(120)

    print("Send close event")
    main.close()
    main.join()
