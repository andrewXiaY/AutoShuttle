import socket


class Sender:
    def __init__(self):
        self.client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def send_msg(self, destination, port, msg):
        self.client.connect((destination, port))
        self.client.send(msg.encode("utf8"))

