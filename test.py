from table_queue import TableQueue
import numpy as np

if __name__ == '__main__':
    tableQueue = TableQueue(24, 6)
    tableQueue.push(2, 3)

    np_data = np.char.isnumeric(tableQueue.queue)
    print(np_data)
    print(np_data.sum())
