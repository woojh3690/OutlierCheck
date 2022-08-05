class TableQueue:
    def __init__(self, row_size, col_size):
        self.queue = []
        for _ in range(col_size):
            self.queue.append(['None'] * row_size)
        self.row_size = row_size
        self.col_size = col_size
    
    # 버퍼에 데이터를 저장한다. 
    # 버퍼에 저장되는 데이터 개수는 row_size 넘지 않는다.
    def push(self, idx, value):
        sel_queue = self.queue[idx]
        sel_queue.append(value)
        sel_queue.pop(0)