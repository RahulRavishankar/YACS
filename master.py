import json
import socket
import time
import sys
import random
import numpy
import threading
from Priority_Queue import PriorityQueue
import logging
import datetime

logging.basicConfig(filename='YACS_logs.log', filemode='w',
                    format='%(message)s', level=logging.INFO)


jobs_pq = PriorityQueue()  # format: (prority, job)
pq_lock = threading.Lock()
workers_lock = {}
sockets = {}
lock = threading.Lock()


def send_tasks(data, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('localhost', port))
    s.send(data.encode())
    s.close()


def listen_to_requests():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('localhost', 5000))
    s.listen(1)

    while(1):
        try:
            host, _ = s.accept()
            with host:
                data = host.recv(1024)
                x = json.loads(data)
                d = dict()
                p = []
                l = []
                for i in x['map_tasks']:
                    l.append((i['task_id'], i['duration']))
                    lock.acquire()
                    date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    logging.info(date + " " + "task arrival" + " " +
                                 x["job_id"] + " " + i["task_id"])
                    lock.release()
                p.append(l)
                r = []
                for i in x['reduce_tasks']:
                    r.append((i['task_id'], i['duration']))
                    lock.acquire()
                    date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    logging.info(date + " " + "task arrival" +
                                 " " + x["job_id"] + " " + i["task_id"])
                    lock.release()

                p.append(r)
                d[x['job_id']] = p

                pq_lock.acquire()
                jobs_pq.insert(d)
                pq_lock.release()
            print("Received Request")       
        except KeyboardInterrupt:
            break


def handle_roundrobin(workers):
    # jobs is structured like: [{'0': [[('0_M0', 2)], [('0_R0', 4), ('0_R1', 1)]]}, {'1': [[('1_M0', 1)], [('1_R0', 2), ('1_R1', 4)]]}]
    prev_worker_id = -1

    while(1):
        while(not jobs_pq.isEmpty()):
            pq_lock.acquire()
            task = jobs_pq.getTask()
            pq_lock.release()
            if(task == None):
                continue

            found = False
            while(not found):
                worker_id = (prev_worker_id + 1) % len(worker_ids)
                count = 0
                while(count < len(worker_ids)):
                    time.sleep(0.1)
                    count += 1
                    workers_lock[worker_id+1].acquire()
                    freeSlotisAvailable = workers[worker_id+1]["free_slots"]
                    workers_lock[worker_id+1].release()
                    if(freeSlotisAvailable):
                        prev_worker_id = worker_id
                        workers_lock[worker_id+1].acquire()
                        workers[worker_id+1]["free_slots"] -= 1
                        workers_lock[worker_id+1].release()

                        print("Task %s assigned to worker %d" %
                              (str(task), worker_id+1))
                        #####################
                        # Send task to worker
                        send_tasks(str(task[0])+","+str(task[1]),
                                   workers[worker_id+1]["port"])
                        #####################
                        found = True

                        break
                    worker_id = (worker_id + 1) % len(worker_ids)


def handle_random(workers, worker_ids):

    while(1):
        while(not jobs_pq.isEmpty()):
            pq_lock.acquire()
            task = jobs_pq.getTask()
            pq_lock.release()
            if(task == None):
                continue
            worker_found = False
            while(not worker_found):
                worker_id = random.choice(worker_ids)
                workers_lock[worker_id].acquire()
                freeSlotisAvailable = workers[worker_id]["free_slots"]
                workers_lock[worker_id].release()
                if(freeSlotisAvailable):
                    worker_found = True
                    workers_lock[worker_id].acquire()
                    workers[worker_id]["free_slots"] -= 1
                    workers_lock[worker_id].release()

                    print("Task %s assigned to worker %d" %
                          (str(task), worker_id))
                    #####################
                    # Send task to worker
                    send_tasks(str(task[0])+","+str(task[1]),
                               workers[worker_id]["port"])
                    #####################

    print("Exitting random")


def handle_LL(workers):
    while(1):
        while(not jobs_pq.isEmpty()):
            pq_lock.acquire()
            task = jobs_pq.getTask()
            pq_lock.release()
            if(task == None):
                continue
            worker_found = False
            max_slots = 0
            max_id = 0
            while(not worker_found):
                for worker_id in workers:
                    workers_lock[worker_id].acquire()
                    if(workers[worker_id]["free_slots"] > max_slots):
                        max_slots = workers[worker_id]["free_slots"]
                        max_id = worker_id
                    workers_lock[worker_id].release()

                if(max_slots):
                    worker_found = True
                    workers_lock[max_id].acquire()
                    workers[max_id]["free_slots"] -= 1
                    workers_lock[max_id].release()
                    print("Task %s assigned to worker %d" %
                          (str(task), max_id))
                    #####################
                    # Send task to worker
                    send_tasks(str(task[0])+","+str(task[1]),
                               workers[max_id]["port"])
                    #####################
                    break


def listen_to_workers():
    print("Listening to workers")
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('localhost', 5001))
    s.listen(1)

    while(1):
        try:
            host, _ = s.accept()
            with host:
                msg = host.recv(1024)
                msg = msg.decode()
                start_time, end_time, task_id, worker_id = msg.split(',')
                worker_id = int(worker_id)
                print("Message %s received from worker %d " %
                      (task_id, worker_id))
                lock.acquire()
                job_id = task_id.split("_")[0]
                logging.info(start_time + " " + "starting task" +
                             " " + job_id + " " + task_id + " " + str(worker_id))
                logging.info(end_time + " " + "ending task" + " " +
                             job_id + " " + task_id + " " + str(worker_id))
                lock.release()
                workers_lock[worker_id].acquire()
                workers[worker_id]["free_slots"] += 1
                workers_lock[worker_id].release()

                pq_lock.acquire()
                jobs_pq.popTask(task_id)
                pq_lock.release()
        except KeyboardInterrupt:
            break

    print("NOT LISTENING TO UPDATES FROM WORKERS ANYMORE")
    s.close()


if __name__ == '__main__':
    if(len(sys.argv) != 3):
        print("Usage: python master.py <path to config file> <name of algorithm: RR,LL,RANDOM>")
        exit()
    args = sys.argv
    path = args[1]
    algo = args[2]
    logging.info(args[2])

    if (algo != "RR") and (algo != "RANDOM") and (algo != "LL"):
        print("INVALID ALGORITHM! ENTER RR,LL or RANDOM")
        exit()

    f = open(path, 'r')
    data = json.load(f)

    print("Connect with Workers..............")
    workers = {}
    worker_ids = []
    for worker in data["workers"]:
        print(worker)
        workers[worker["worker_id"]] = {
            "slots": worker["slots"],
            "port": worker["port"],
            "free_slots": worker["slots"]
        }
        worker_ids.append(worker["worker_id"])
        workers_lock[worker["worker_id"]] = threading.Lock()

    print("Connection with Workers successful!!\n")

    requests_listener = threading.Thread(target=listen_to_requests)
    worker_listener = threading.Thread(target=listen_to_workers)
    requests_listener.start()
    worker_listener.start()

    print("Continue processing on the master thread to assign jobs")
    time.sleep(5)
    if algo == "RR":
        handle_roundrobin(workers)
    elif algo == "RANDOM":
        handle_random(workers, worker_ids)
    elif algo == "LL":
        handle_LL(workers)

    worker_listener.join()
    requests_listener.join()
