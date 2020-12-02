import json
import socket
import time
import sys
import random
import numpy
import threading
import logging

args = sys.argv
port = int(args[1])
worker_id = int(args[2])

logging_lock = threading.Lock()

logging.basicConfig(filename='master.log', filemode='w',
                    format='%(asctime)s  %(message)s', datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)

def send_update(data):
    time.sleep(data[1])
    # print("Data: ",data)
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('localhost', 5001))
    msg = data[0] + "," + str(worker_id)
    
    
    time.sleep(3)
    s.send(msg.encode())
    print("Execution of %s completed"%(data[0]))
    s.close()
    logging_lock.acquire()
    job_id = ""
    i = 0
    while (data[0][i] != '_'):
        job_id += data[0][i]
        i += 1
    logging.info("ending task" + " " + job_id +
                 " " + data[0] + " " + str(worker_id))

    logging_lock.release()

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
        l[1]=int(l[1])
        # print("Creating a new thread for:", l)
        
        t = threading.Thread(target=send_update,args=(l,))
        t.start()
        logging_lock.acquire()
        job_id = ""
        i = 0
        while (l[0][i] != '_'):
            job_id += l[0][i]
            i += 1
        logging.info("starting task" + " " + job_id + " "
                     + l[0] + " " + str(worker_id))  # job_id left
        # host.close()
        logging_lock.release()
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
