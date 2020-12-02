import sys
import math
import re
import matplotlib.pyplot as plt

from datetime import datetime,date,time

''''task completion time:  
(end time of task inWorker process) – (arrival time of task at Worker process)
job completion time:
(end time of the last reduce task) – (arrival time of job at Master)'''

worker = sys.argv[1]
master = sys.argv[2]
f = open(worker)
p = open(master)

def median(list):
    list.sort()
    l = len(list)
    
    mid = (l-1)//2
    
    if(l%2==0):
        return (list[mid] + list[mid+1])/2
    else:
        return list[mid]

arr = dict() #stores first task arrival of a job
for line in p:
    job_id  = line.split()[4]
    if job_id not in arr:
        arr[job_id] = []
        arr[job_id].append(datetime.strptime(line.split()[1], '%H:%M:%S').time())
    else:
        arr[job_id].append(datetime.strptime(line.split()[1], '%H:%M:%S').time())

p.close()

end = dict()





task_completion_time = []
job_arrival = dict()
starting_task = dict()
ending_task = dict()

pats = ['starting task','ending task']
pat = "(" + "|".join(pats) + ")" + "\s+(\w+)"
hm = dict()


for line in f:
    #line = line.strip()
    #print(line.split())
    m = re.search(pat,line)
    if m:
        
        time = line.split()[1]
        worker_id = line.split()[6]
        task = line.split()[5]
        job = line.split()[4]
            

        if(m.group(1) == 'starting task'):
            starting_task[task] = time
            if(worker_id + " " + time) not in hm:
                hm[worker_id + " " + time] = 1
            else:
                hm[worker_id + " " + time] += 1
        if(m.group(1) == 'ending task'):
            ending_task[task] = time
            if job not in end:
                end[job] = []
                end[job].append(datetime.strptime(time, '%H:%M:%S').time())
            else:
                end[job].append(datetime.strptime(time, '%H:%M:%S').time())
                
            

f.close()            
a = date.today()
#print(today)
job_completion = []

for i in end.keys():
    sorted(end[i], reverse=True)
    print(i,end[i])
    sorted(arr[i])
    #print(i,arr[i])
    A_1 = end[i][0]
    
    A_2 = arr[i][0]
    dif = datetime.combine(a, A_1) - datetime.combine(a, A_2)
    
    job_completion.append(dif.total_seconds())

mean_jobs = sum(job_completion)/len(job_completion)
median_jobs = median(job_completion)
fig = plt.figure()
ab = fig.add_axes([0,0,1,1])
xax = ['mean', 'median']
yax = [mean_jobs,median_jobs]
ab.bar(xax,yax)
plt.show()
print(mean_jobs)
print(median_jobs)

#print(starting_task)        
for i in starting_task.keys():
    #print(i)
    RA_1 = datetime.strptime(starting_task[i], '%H:%M:%S').time()
    
    RA_2 = datetime.strptime(ending_task[i], '%H:%M:%S').time()
    
    diff = datetime.combine(a, RA_2) - datetime.combine(a, RA_1)
    #print(diff.total_seconds())
    task_completion_time.append(diff.total_seconds())

mean_tasks = sum(task_completion_time)/len(task_completion_time)
median_tasks = median(task_completion_time)
#print(hm)
#print(task_completion_time)   
#print(mean_tasks,median_tasks)
fig = plt.figure()
ax = fig.add_axes([0,0,1,1])
xaxis = ['mean', 'median']
yaxis = [mean_tasks,median_tasks]
ax.bar(xaxis,yaxis)
plt.show()
        
    
