import sys
import math
import re
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from operator import itemgetter
from datetime import datetime,date,time
from tabulate import tabulate

def getTimeSec(timestamp):
    time=datetime.strptime(timestamp,"%H:%M:%S")
    a_timedelta=time-datetime(1900,1,1)
    seconds=a_timedelta.total_seconds()
    return seconds  

def median(list):
    list.sort()
    l = len(list)
    
    mid = (l-1)//2
    
    if(l%2==0):
        return (list[mid] + list[mid+1])/2
    else:
        return list[mid]

def plot(logs,algo):
	arr = dict() #stores first task arrival of a job
	end = dict()
	task_completion_time = []
	job_arrival = dict()
	starting_task = dict()
	ending_task = dict()
	tstamp=0
	w1=0
	w2=0
	w3=0
	heatframe=[]
	base_time_set = False
	base_time = -1

	pats = ['job arrival','starting task','ending task']
	pat = "(" + "|".join(pats) + ")" + "\s+(\w+)"
	hm = dict()

	for line in logs:
		cur_time=getTimeSec(line[10:18])
		if(not base_time_set):
			base_time_set = True
			base_time = cur_time
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
				worker=line[-2]
				if(worker=='1'):
					w1+=1
				elif(worker=='2'):
					w2+=1
				else:
					w3+=1
				if tstamp==0:
					tstamp=cur_time
					heatframe.append((cur_time,"W1",w1))
					heatframe.append((cur_time,"W2",w2))
					heatframe.append((cur_time,"W3",w3))

				worker_id = line.split()[6]
				starting_task[task] = time
				if(worker_id + " " + time) not in hm:
					hm[worker_id + " " + time] = 1
				else:
					hm[worker_id + " " + time] += 1
			if(m.group(1) == 'ending task'):
				worker=line[-2]
				if(worker=='1'):
					w1-=1
				elif(worker=='2'):
					w2-=1
				else:
					w3-=1
				if tstamp==0:
					tstamp=cur_time
					heatframe.append((cur_time,"W1",w1))
					heatframe.append((cur_time,"W2",w2))
					heatframe.append((cur_time,"W3",w3))
					worker_id = line.split()[6]
					ending_task[task] = time

				if job not in end:
					end[job] = []
					end[job].append(time.split(":"))
				else:
					end[job].append(time.split(":"))

		if(cur_time>tstamp):
			heatframe.append((tstamp,"W1",w1))
			heatframe.append((tstamp,"W2",w2))
			heatframe.append((tstamp,"W3",w3))
			tstamp=cur_time

	workers=[]
	times=[]
	tasks=[]
	for x in range(3,len(heatframe)):
		times.append(heatframe[x][0]-base_time)
		workers.append(heatframe[x][1])
		tasks.append(heatframe[x][2])
		
	heat=pd.DataFrame({"Time":times,"Workers":workers,"Tasks Running":tasks})
	print(tabulate(heat, headers = 'keys', tablefmt = 'psql')) 	
	heated=heat.pivot(index="Workers",columns="Time",values="Tasks Running")
	sns.heatmap(heated, annot=True, fmt="g", cmap='viridis')
	plt.title("Worker job allocation:(%s)"%algo)
	plt.show()

	a = date.today()
	job_completion = []

	for i in end.keys():
	    end[i] = sorted(end[i], key=itemgetter(1,2),reverse=True)
	    arr[i] = sorted(arr[i], key=itemgetter(1,2))
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
	print("Mean: ",mean_jobs)
	print("Median: ",median_jobs)

	print("Number of tasks started: ",len(starting_task))
	print("Number of tasks ended: ",len(ending_task))
	#print(starting_task)        
	for i in starting_task.keys():
	    RA_1 = datetime.strptime(starting_task[i], '%H:%M:%S').time()
	    RA_2 = datetime.strptime(ending_task[i], '%H:%M:%S').time()
	    diff = datetime.combine(a, RA_2) - datetime.combine(a, RA_1)
	    task_completion_time.append(diff.total_seconds())

	mean_tasks = sum(task_completion_time)/len(task_completion_time)
	median_tasks = median(task_completion_time)
	fig = plt.figure()
	ax = fig.add_axes([0,0,1,1])
	xaxis = ['mean', 'median']
	yaxis = [mean_tasks,median_tasks]
	ax.bar(xaxis,yaxis)
	plt.show()


''''task completion time:  
(end time of task inWorker process) – (arrival time of task at Worker process)
job completion time:
(end time of the last reduce task) – (arrival time of job at Master)'''

filepath = sys.argv[1]
algo = sys.argv[2]
f = open(filepath)
logs=f.readlines()
plot(logs,algo)
f.close()
