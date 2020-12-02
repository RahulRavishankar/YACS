import sys
import math
import re
import matplotlib.pyplot as plt
from operator import itemgetter


from datetime import datetime,date,time

''''task completion time:  
(end time of task inWorker process) – (arrival time of task at Worker process)
job completion time:
(end time of the last reduce task) – (arrival time of job at Master)'''

filepath = sys.argv[1]

f = open(filepath)



    
def median(list):
    list.sort()
    l = len(list)
    
    mid = (l-1)//2
    
    if(l%2==0):
        return (list[mid] + list[mid+1])/2
    else:
        return list[mid]

arr = dict() #stores first task arrival of a job
'''for line in p:
    job_id  = line.split()[4]
    time = line.split()[1]
    if job_id not in arr:
        arr[job_id] = []
        arr[job_id].append(list(map(int,time.split(":"))))
    else:
        arr[job_id].append(list(map(int,time.split(":"))))

p.close()'''

end = dict()


task_completion_time = []
job_arrival = dict()
starting_task = dict()
ending_task = dict()

pats = ['job arrival','starting task','ending task']
pat = "(" + "|".join(pats) + ")" + "\s+(\w+)"
hm = dict()


for line in f:
    #line = line.strip()
    #print(line.split())
    m = re.search(pat,line)
    if m:
        
        time = line.split()[1]
        
        task = line.split()[5]
        job = line.split()[4]

        if(m.group(1) == 'job arrival'):
            if job not in arr:
                arr[job] = []
                arr[job].append(list(map(int,time.split(":"))))
            else:
                arr[job].append(list(map(int,time.split(":"))))

        if(m.group(1) == 'starting task'):
            worker_id = line.split()[6]
            starting_task[task] = time
            if(worker_id + " " + time) not in hm:
                hm[worker_id + " " + time] = 1
            else:
                hm[worker_id + " " + time] += 1
        if(m.group(1) == 'ending task'):
            worker_id = line.split()[6]
            ending_task[task] = time
            if job not in end:
                end[job] = []
                end[job].append(time.split(":"))
            else:
                end[job].append(time.split(":"))
                
            

f.close()            
a = date.today()
#print(today)
job_completion = []

for i in end.keys():
    
    end[i] = sorted(end[i], key=itemgetter(1,2),reverse=True)
    print(i,end[i])
    arr[i] = sorted(arr[i], key=itemgetter(1,2))
    #print(i,arr[i])
    A_1 = end[i][0]
    t1 = str(end[i][0][0]) + ":" + str(end[i][0][1]) + ":" + str(end[i][0][2])
    t1_ = datetime.strptime(t1, '%H:%M:%S').time()
    A_2 = arr[i][0]
    t2 = str(arr[i][0][0]) + ":" + str(arr[i][0][1]) + ":" + str(arr[i][0][2])
    t2_ = datetime.strptime(t2, '%H:%M:%S').time()
    dif = datetime.combine(a, t1_) - datetime.combine(a, t2_)
    
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
        
    
