from utils.helper import generate_signal, create_msg_template, get_ip
from communication.receiver import Receiver
from communication.sender import Sender
from datetime import datetime
import time
import sys
from multiprocessing import Queue, Process, Manager


def consume_registration(reg_pool, queue: Queue):
    """
    将收到的operation machine的账户和地址信息存储到dictionary中
    :param reg_pool: 用来存储账户信息的dictionary
    :param queue: 用来待处理账户信息的队列
    :return: None
    """
    while True:
        while not queue.empty():
            reg = eval(queue.get())
            acc_ = reg["account"]
            if acc_ in reg_pool:
                print(f"Update account {acc_} to {reg['ip']}:{reg['port']}")
            else:
                print(f"Register account {acc_} ({reg['ip']}:{reg['port']})")
            reg_pool[reg["account"]] = (reg["ip"], reg["port"])


def listen_registration(ip, port, queue=None):
    receiver = Receiver(ip, port)
    receiver.listening(queue)


def main():

    ip_address = get_ip(int(sys.argv[-1]))
    port = 6060 if len(sys.argv) == 2 else int(sys.argv[1])

    print(f"Computation is hosted by {ip_address}:{port}\n"
          f"Please start the operator machine by passing '{ip_address}' and '{port}' as the second and third parameter")

    shared_queue = Queue()

    # registration_pool records ip addresses of all operation machine
    registration_pool = Manager().dict()

    # listening registration from operation machine
    listening_proc = Process(target=listen_registration, args=(ip_address, port, shared_queue,))
    listening_proc.daemon = True
    listening_proc.start()

    # process registration sent from operation machine
    process_registration_proc = Process(target=consume_registration, args=(registration_pool, shared_queue,))
    process_registration_proc.daemon = True
    process_registration_proc.start()

    """
    ptf_manager = PTF_Manager()
    """

    # This is logic of computation of signal
    while True:
        time.sleep(10)
        # here to process msgs in the shared_queue

        """
        signal = ptf_manager.generate_signal()
        {
            'type': [trade, roll contract],
            'signal': [(ticker, latest_signal), True or False]
        }
        """
        _ = generate_signal()  # 这里没有处理产生的信号
        for acc_, (addr, port_) in registration_pool.items():
            msg = create_msg_template()
            """
            在这里添加一个方法用来将所有需要传输的信息转换成一个字典形式的字符串，
            当前用一个hard coded的字符串代替
            """
            try:
                Sender().send_msg(addr, port_, str(msg))
                print(f"Signal sent to {acc_}({addr}:{port_})")
            except Exception as e:
                print(e)

        if datetime.now().hour == "15":
            break

    listening_proc.join()
    process_registration_proc.join()


if __name__ == "__main__":
    main()
