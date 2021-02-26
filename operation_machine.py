from communication.receiver import Receiver
from communication.sender import Sender
from datetime import datetime
from utils.helper import get_ip
import socket
import sys
from multiprocessing import Process, Queue


def send_registration(acc, destination, d_port, port, ip_flag):
    """
    将账户信息发送给computation machine
    :param acc: 账号
    :param destination: computation machine 地址
    :param d_port: computation machine 端口
    :param port: operation machine 端口
    :param ip_flag: 获取ip地址种类
    :return: None
    """
    hostname = socket.gethostname()
    ip_address = get_ip(ip_flag)

    print(f"Initializing operator {acc} in {ip_address}:{port}")

    msg = {
        "account": acc,
        "ip": ip_address,
        "port": port
    }

    print(f"Sending registration to computation machine")
    Sender().send_msg(destination, d_port, str(msg))
    return ip_address


def listen_signal(ip, port, queue: Queue = None):
    """
    监听computation machine发送过来的信号
    :param ip: 绑定的地址
    :param port: 监听端口
    :param queue: 用来存储信号的队列
    :return:
    """
    receiver = Receiver(ip, port)
    receiver.listening(queue)


def main():
    acc = sys.argv[1]
    comp_machine_addr = sys.argv[2]
    comp_machine_port = int(sys.argv[3])
    port = int(sys.argv[4]) if len(sys.argv) == 6 else 7070

    print(f"The target computation machine is {comp_machine_addr}:{comp_machine_port}")

    # 将注册信息发送给computation machine
    ip_address = send_registration(acc, comp_machine_addr, comp_machine_port, port, int(sys.argv[-1]))

    # 存储信号的队列
    shared_queue = Queue()

    # 新建监听信号的进程
    listen_signal_proc = Process(target=listen_signal, args=(ip_address, port, shared_queue,))
    listen_signal_proc.daemon = True
    listen_signal_proc.start()

    while True:
        while not shared_queue.empty():
            sig = shared_queue.get()
            # 这里进行交易操作，现在暂时不知道交易逻辑
            print(sig)
            """
            if sig['type'] == 'trade':
                trader.trade(sig['signal'][0], sig['signal][1], )
            """
        if datetime.now().hour == '15':
            break

    listen_signal_proc.join()


if __name__ == "__main__":
    main()
