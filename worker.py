import json
import socket
import time
import sys
import random
import numpy
import threading

args = sys.argv
port = int(args[1])
worker_id = int(args[2])


exec_pool = []
def listen_to_master():
    print("Listening to master for jobs")
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('localhost', port))
        s.listen(1)
        while(1):
            try:
                host, _ = s.accept()
                with host:
                    data = host.recv(1024)
                    d = data.decode()
                    print(d)
                    l = d.split(',')
                    print(l)
                    exec_pool.append(l)
                print(exec_pool)
            except KeyboardInterrupt:
                break


def execute_tasks():
    print("Executing the tasks assigned")
    while(1):
        if(len(exec_pool) > 0):
           threading.Timer(1.0,execute_tasks).start()
           for i in exec_pool:
              print(i[1]) 
              x = int(i[1])
              x -= 1
              i[1] = str(x)
              if(int(x) == 0):
                exec_pool.remove(i)
                send_update(i[0])
        #waiting       


def send_update(data):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('localhost', 5001))
    data = data + "," + str(worker_id)
    s.send(data.encode())


if __name__ == '__main__':
    if(len(sys.argv) != 3):
        print("Usage: python worker.py <port> <worker_id>")
        exit()

    master_listener = threading.Thread(target=listen_to_master)
    task_executor = threading.Thread(target=execute_tasks)
    task_executor.start()
    master_listener.start()

    print("Continue to execute in worker %s" % (worker_id))
    master_listener.join()
    task_executor.join()
