from main import main
import time
import sys

SAMPLE_DATA = '{{"timestamp":"date","idx":{},"feature":{}}}'

if __name__ == '__main__':
    main = main(sys.argv)
    main.start()

    for i in range(30):
        for j in range(6):
            send_msg = SAMPLE_DATA.format(j, i * 6 + j)
            print("Send Msg : " + send_msg)
            main.producer.send("outlier_check", value=send_msg)
    
    time.sleep(30)
    main.close()
