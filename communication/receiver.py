import socket
from multiprocessing import Queue
from utils.constants import RECV_BYTES


class Receiver:
    def __init__(self, ip, port):
        print(ip, port)
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.bind(('0.0.0.0', port))
        self.server.listen(5)

    def listening(self, queue: Queue):
        while True:
            conn, addr = self.server.accept()
            msg = ""
            while True:
                data = conn.recv(RECV_BYTES)
                if len(data) == 0:
                    break
                msg += data.decode()
            queue.put(msg)
            conn.close()
