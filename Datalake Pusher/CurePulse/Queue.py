import multiprocessing

class Queue:
    def __init__(self):
        self.CreateQueues()

    def CreateQueues(self):
        
        ### Create Queues
        queue_1_conn = multiprocessing.Pipe()
        queue_2_conn = multiprocessing.Pipe()
        queue_3_conn = multiprocessing.Pipe()
        queue_4_conn = multiprocessing.Pipe()
        queue_5_conn = multiprocessing.Pipe()
        queue_6_conn = multiprocessing.Pipe()
        queue_7_conn = multiprocessing.Pipe()
        queue_8_conn = multiprocessing.Pipe()
        queue_9_conn = multiprocessing.Pipe()
        queue_10_conn = multiprocessing.Pipe()
        queue_11_conn = multiprocessing.Pipe()

        ### Create Queue Connections
        self.p1_queue_1_conn, self.p2_queue_1_conn = queue_1_conn   # Queue 1
        self.p2_queue_2_conn, self.p3_queue_2_conn = queue_2_conn   # Queue 2
        self.p3_queue_3_conn, self.p4_queue_3_conn = queue_3_conn   # Queue 3
        self.p4_queue_4_conn, self.p5_queue_4_conn = queue_4_conn   # Queue 4
        self.p5_queue_5_conn, self.p6_queue_5_conn = queue_5_conn   # Queue 5
        self.p6_queue_6_conn, self.p7_queue_6_conn = queue_6_conn   # Queue 6
        self.p7_queue_7_conn, self.p8_queue_7_conn = queue_7_conn   # Queue 7
        self.p8_queue_8_conn, self.p9_queue_8_conn = queue_8_conn   # Queue 8
        self.p9_queue_9_conn, self.p10_queue_9_conn = queue_9_conn  # Queue 9
        self.p10_queue_10_conn, self.p11_queue_10_conn = queue_10_conn  # Queue 10
        self.p11_queue_11_conn, self.p12_queue_11_conn = queue_11_conn  # Queue 10