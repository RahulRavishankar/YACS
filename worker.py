import json
import socket
import time
import sys
import random
import numpy
import threading
import logging
import datetime

args = sys.argv
port = int(args[1])
worker_id = int(args[2])

#logging_lock = threading.Lock()




def send_update(data):
    date1 = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    time.sleep(data[1])
    date2 = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # print("Data: ",data)
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('localhost', 5001))
    msg = date1 + "," + date2 + "," + data[0] + "," + str(worker_id)
    s.send(msg.encode())
    print("Execution of %s completed" % (data[0]))
    s.close()


def listen_to_master():
    print("Listening to master for jobs")
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('localhost', port))
    s.listen(1)
    while(1):
        host, _ = s.accept()
        data = host.recv(1024)
        d = data.decode()
        l = d.split(',')
        l[1] = int(l[1])
        # print("Creating a new thread for:", l)
        t = threading.Thread(target=send_update, args=(l,))
        t.start()
        # host.close()

    print("NOT LISTENING TO MASTER ANYMORE")
    s.close()


if __name__ == '__main__':
    if(len(sys.argv) != 3):
        print("Usage: python worker.py <port> <worker_id>")
        exit()

    master_listener = threading.Thread(target=listen_to_master)
    master_listener.start()

    print("Continue to execute in worker %s" % (worker_id))
    master_listener.join()
    print("Alll Done")
