from random import random, choice
from communication.message import Signal
import socket
import requests


SIGNALS = [-1, 0, 1]


def generate_signal():
    """
    This is the function to produce signals which will be sent to
    the machine operating account
    :return: -1, 0 or 1
    """

    return choice(SIGNALS)


def create_msg_template():
    sig = Signal(random(), random(), random(), random(), random(), random(), random(), random())
    return sig._asdict()


def get_ip(flag):

    if flag == 0:  # localhost
        return "localhost"
    elif flag == 1:  # LAN
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(('8.8.8.8', 80))
            ip = s.getsockname()[0]
        finally:
            s.close()
        return ip
    elif flag == 2:  # WAN
        return requests.get('http://ifconfig.me/ip', timeout=1).text.strip()
    else:
        raise ValueError("Flag should be 0, 1 or 2")

