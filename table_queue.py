class TableQueue:
    def __init__(self, row_size, col_size):
        self.table = [['None'] * col_size] * row_size
    
    def push(self, idx, value):
        print("table : ", self.table)
        # TODO