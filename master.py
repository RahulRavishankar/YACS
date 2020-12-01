import json
import socket
import time
import sys
import random
import numpy
import threading

tasks_queue = []


def listen_to_requests():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('localhost', 5000))
    s.listen(1)

    while(1):
        try:
            host, port = s.accept()

            with host:
                data = host.recv(1024)

                if not data:
                    break
                x = json.loads(data)
                d = dict()
                p = []
                l = []
                for i in x['map_tasks']:
                    l.append((i['task_id'], i['duration']))
                p.append(l)
                r = []
                for i in x['reduce_tasks']:
                    r.append((i['task_id'], i['duration']))

                p.append(r)
                d[x['job_id']] = p
                tasks_queue.append(d)
            print(tasks_queue)
        except KeyboardInterrupt:
            break

def handle_roundrobin():
    pass


def handle_random():
    pass


def handle_LL():
    pass

def listen_to_workers():
    print("Listening to workers")
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('localhost', 5001))
    s.listen(1)
    
    while(1):
        try:
            host, _ = s.accept()    
            with host:
                data = host.recv(1024)
                print(data.decode())
        except KeyboardInterrupt:
            break




if __name__ == '__main__':
    if(len(sys.argv) != 3):
        print("Usage: python master.py <path to config file> <name of algorithm: RR,LL,RANDOM>")
        exit()
    args = sys.argv
    path = args[1]
    algo = args[2]

    if (algo != "RR") and (algo != "RANDOM") and (algo != "LL"):
        print("INVALID ALGORITHM! ENTER RR,LL or RANDOM")
        exit()

    f = open(path, 'r')
    data = json.load(f)

    print("Connect with Workers..............")
    sockets = {}
    for worker in data["workers"]:
        print(worker)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(('localhost', worker["port"]))
        sockets[worker["worker_id"]] = s

        msg = "Hello from master"
        sockets[worker["worker_id"]].send(msg.encode())
    
    print("Connection with Workers successful!!\n")

    workers=[x for x in data["workers"]] #list of dictionaries, each dictionary contains the worker details.
    for i in workers:
        i["free_slots"]=i["slots"]
        #print(i)

    
    requests_listener = threading.Thread(target=listen_to_requests)
    worker_listener = threading.Thread(target=listen_to_workers)
    requests_listener.start()
    worker_listener.start()

    print("Continue processing on the master thread to assign jobs")
    if algo == "RR":
        handle_roundrobin()
    elif algo == "RANDOM":
        handle_random()
    elif algo == "LL":
        handle_LL()

    worker_listener.join()
    requests_listener.join()
