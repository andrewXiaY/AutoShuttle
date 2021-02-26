# AutoShuttle

###### 量化信息交流工具

---------------------------------

`Computation Machine` 是用来部署计算信号的程序的机器

`Operation Machine` 是用来部署操作账户程序的机器

- Note:在这两个程序中使用了multiprocessing来并行处理多个任务，多进程之间使用`multiprocessing.Queue`和`Manager.dict`
进行信息交流

----------------------------------

### __computation_machine.py__

> 程序中包含三部分: `consume_registration`, `listen_registration` 以及 `主逻辑`
>> `consume_registration(dict, queue)` 是一个在后台一直处理新注册的operation machine的进程，dict是一个 "accout" -> ("ip"，"port") 的字典，
> 此进程不断从`queue`中提取operation machine的信息并存储在dict中

>> `listen_registration(port, queue)` 是一个在后后台监听operation machine注册信息的进程，注册信息指"账号"，"IP地址"和"端口号"。每当一个新的operation machine
> 启动，computation machine会收到这样一个注册信息，此进程将这个注册信息放入到`queue`中等待`consume_registration`处理
 
>> `主逻辑`：每隔10秒钟给dict（registration_pool）中的operation machine发送信号

__How to run computation_machine.py?__
1. `python computation_machine.py` to use default port number 6060
2. `python computation_machine.py {port_number}` to use given port number

------------------------------------------------

### __operation_machine.py__

> 程序中包含两部分: `listen_signal` 以及 `主逻辑`
>> `listen_signal(port, queue)` 是一个在后台监听信号的进程，此进程不断的将新信号放入到queue中
 
>> `主逻辑`：当`queue`不为空时处理信号

__How to run operation_machine.py?__
(`cm` represents `computation machine`)
1. `python computation_machine.py {account_number} {cm_address} {cm_port} {port_number}` to use given port number
2. `python computation_machine.py {account_number} {cm_address} {cm_port}` to use default port number

-------------------------------------------------------

### 特别注意
当前程序只在Mac上单机进行过测试，测试方式是单独跑一个computation machine，然后在多个terminal中模拟多个operation machines，

```bash
Terminal_1 >> python computaion_machine.py 6060
-------------------------------------------------------------
Terminal_2 >> python operation_machine.py 1 127.0.0.1 6060 7070
Terminal_3 >> python operation_machine.py 2 127.0.0.1 6060 7071 (这里端口不一样是因为一个机器上模拟不能使用同一个端口)
Terminal_4 >> python operation_machine.py 3 127.0.0.1 6060 7072
......
--------------------------------------------------------------
```

### TODO
1. 实现局域网功能
2. 实现广域网功能