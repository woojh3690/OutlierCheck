from main import main
import time
import sys

SAMPLE_DATA = '{"timestamp":"2022-08-08 15:38:39","features": [219, 56319, 21848, 12741, 21.0, 4.0]}'

if __name__ == '__main__':
    main = main(sys.argv)
    main.start()

    time.sleep(20)

    for i in range(100):
        print("Send Msg : " + SAMPLE_DATA)
        main.producer.send("outlier_check", value=SAMPLE_DATA)
    
    time.sleep(30)

    print("Send close event")
    main.close()
