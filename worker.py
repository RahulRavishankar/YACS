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





#function which simulates running of a task and then sends message to worker upon task completion
def send_update(data):
    date1 = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") #time at which task starts
    time.sleep(data[1])
    date2 = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") #time at which task ends
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('localhost', 5001))
    msg = date1 + "," + date2 + "," + data[0] + "," + str(worker_id) #send start time,end time,task_id and worker_id to master
    s.send(msg.encode())
    print("Execution of %s completed" % (data[0]))
    s.close()


#function which continuously listens to master for receiving tasks
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
        l[1] = int(l[1]) #duration of the task
        t = threading.Thread(target=send_update, args=(l,)) #creates a separate thread for each task 
        t.start()
        # host.close()

    print("NOT LISTENING TO MASTER ANYMORE")
    s.close()


if __name__ == '__main__':
    if(len(sys.argv) != 3): #error handling for incorrect sys arguments
        print("Usage: python worker.py <port> <worker_id>")
        exit()

    master_listener = threading.Thread(target=listen_to_master)
    master_listener.start()

    print("Continue to execute in worker %s" % (worker_id))
    master_listener.join()
    print("Alll Done")
