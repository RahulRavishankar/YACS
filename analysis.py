import sys
import math
import re

from datetime import datetime,date,time

''''task completion time:  
(end time of task inWorker process) – (arrival time of task at Worker process)
job completion time:
(end time of the last reduce task) – (arrival time of job at Master)'''

def median(list):
    list.sort()
    l = len(list)
    
    mid = (l-1)//2
    
    if(l%2==0):
        return (list[mid] + list[mid+1])/2
    else:
        return list[mid]

filepath = sys.argv[1]
f = open(filepath)

tasks_completion_time = []
job_arrival = dict()
starting_task = dict()
ending_task = dict()

pats = ['job arrival','starting task','ending task']
pat = "(" + "|".join(pats) + ")" + "\s+(\w+)"

for line in f:
    #line = line.strip()
    m = re.search(pat,line)
    if m:
        date = line.split()[0]
        time = line.split()[1]
        job_id = line.split()[2]
        task = line.split()[3]
        if(m.group(1) == 'job arrival'):
            job_arrival[job_id] = time
            

        if(m.group(1) == 'starting task'):
            starting_task[task] = time

        if(m.group(1) == 'ending task'):
            ending_task[task] = time

           
for i in starting_task.keys():

    RA_1 = datetime.strptime(starting_task[i], '%H:%M:%S.%f').time()
    
    RA_2 = datetime.strptime(ending_task[i], '%H:%M:%S.%f').time()
    diff = RA_2 - RA_1
    task_completion_time.append(diff)

mean_tasks = sum(task_completion_time)/len(task_completion_time)
median_tasks = median(task_completion_time)
    
            
        
    
