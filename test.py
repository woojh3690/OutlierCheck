from main import main
from datetime import datetime, timedelta
import time
import sys
import json

SAMPLE_DATA = {"timestamp":"2022-08-08 15:38:39","features": [219, 56319, 21848, 12741, 21.0, 4.0]}

if __name__ == '__main__':
    main = main(sys.argv)
    main.start()

    time.sleep(30)

    now = datetime.now()
    for i in range(100):
        now += timedelta(seconds=1)
        SAMPLE_DATA["timestamp"] = now.strftime("%Y-%m-%d %H:%M:%S")
        send_msg = json.dumps(SAMPLE_DATA)
        main.producer.send("outlier_check", value=send_msg)
    
    time.sleep(30)

    print("Send close event")
    main.close()
