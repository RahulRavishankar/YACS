import sys
import math
import re
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from operator import itemgetter
from datetime import datetime,date,time
#from tabulate import tabulate
import numpy as np

def getTimeSec(timestamp):
	timestamp=timestamp[11:19]
	time=datetime.strptime(timestamp,"%H:%M:%S")
	a_timedelta=time-datetime(1900,1,1)
	seconds=a_timedelta.total_seconds()
	return seconds  

#function to calculate median of a list
def median(list):
    list.sort()
    l = len(list)
    
    mid = (l-1)//2
    
    if(l%2==0):
        return (list[mid] + list[mid+1])/2
    else:
        return list[mid]

def plot(logs,algo):
	arr = dict() #stores arrival time of all tasks for a particular job_id
	end = dict() #stores ending time of all tasks for a particular job_id
	task_completion_time = [] #stores the time taken to complete each task
	job_arrival = dict() #stores arrival of each task at master
	starting_task = dict() #stores starting time of each task at worker
	ending_task = dict() #stores ending time of each task at worker
	
	#variables to store for data collection for heatmap
	tstamp=0
	w1=0
	w2=0
	w3=0
	
	#values to be plotted for heatmap, stored in a list of tuples, heatframe
	heatframe=[]
	#tracking first logged timestamp, to set base time
	base_time_set = False
	base_time = -1

	pats = ['task arrival','starting task','ending task']
	pat = "(" + "|".join(pats) + ")" + "\s+(\w+)"
	hm = dict()
	
	for line in logs:
		cur_time=getTimeSec(line)
		#setting base time of execution
		if(not base_time_set):
			base_time_set = True
			base_time = cur_time
		#adding an entry to the heatmap dataset, every time there is an increase in time
		#each entry in the dataset contains the timestamp, worker, 
		#and number of tasks running on said worker
		if(cur_time>tstamp):
			heatframe.append((tstamp,"W1",w1))
			heatframe.append((tstamp,"W2",w2))
			heatframe.append((tstamp,"W3",w3))
			tstamp=cur_time
			
		#detect the type of event: task arrival, task starting, or task ending
		m = re.search(pat,line)
		if m:
			time = line.split()[1]
			task = line.split()[5]
			job = line.split()[4]
			if(m.group(1) == 'task arrival'):
			    if job not in arr:
			        arr[job] = []
			        arr[job].append(list(map(int,time.split(":"))))
			    else:
			        arr[job].append(list(map(int,time.split(":"))))

			elif(m.group(1) == 'starting task'):
				worker=line[-2]
				if(worker=='1'):
					w1=w1+1
				elif(worker=='2'):
					w2=w2+1
				else:
					w3=w3+1

				worker_id = line.split()[6]
				starting_task[task] = time
				if(worker_id + " " + time) not in hm:
					hm[worker_id + " " + time] = 1
				else:
					hm[worker_id + " " + time] += 1
			elif(m.group(1) == 'ending task'):
				worker=line[-2]
				if(worker=='1'):
					w1-=1
				elif(worker=='2'):
					w2-=1
				else:
					w3-=1

				worker_id = line.split()[6]
				ending_task[task] = time
				if job not in end:
					end[job] = []
					end[job].append(time.split(":"))
				else:
					end[job].append(time.split(":"))

	#exctracting the values required for heatmap plotting
	workers=[]
	times=[]
	tasks=[]
	for x in range(3,len(heatframe)):
		#subtracting basetime to ensure heatmap's x axis (time) runs from 0 to runtime
		times.append(heatframe[x][0]-base_time)
		workers.append(heatframe[x][1])
		tasks.append(heatframe[x][2])
	
	#formatting the dataset to plot the heatmap,
	#x-axis: time ; y-axis: workers ; color: no. of tasks running on the worker
	heat=pd.DataFrame({"Time":times,"Workers":workers,"Tasks Running":tasks})
	heated=heat.pivot(index="Workers",columns="Time",values="Tasks Running")
	sns.heatmap(heated, annot=True, fmt="g", cmap='viridis',cbar_kws={'label': 'Tasks Running'})
	plt.title("Worker job allocation:(%s)"%algo)
	plt.show()

	a = date.today()
	job_completion = []

	for i in end.keys():
	    end[i] = sorted(end[i], key=itemgetter(1,2),reverse=True) #sort to get the ending time for last reducer task
	    arr[i] = sorted(arr[i], key=itemgetter(1,2)) #sort to get the arrival time for the first task of each job
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
	#plot bar graph for mean and median of job completion
	objects = ('mean', 'median')
	y_pos = np.arange(len(objects))
	performance = [mean_jobs,median_jobs]
	plt.bar(y_pos,performance,align='center',alpha=0.5)
	plt.xticks(y_pos,objects)
	plt.ylabel('Time')
	plt.title('Mean and median time of job completion: (%s)'%algo)
	plt.show()
	print("Number of tasks started: ",len(starting_task))
	print("Number of tasks ended: ",len(ending_task))
	print("Mean of jobs: ",mean_jobs)
	print("Median of jobs: ",median_jobs)

	
	#print(starting_task)        
	for i in starting_task.keys():
	    RA_1 = datetime.strptime(starting_task[i], '%H:%M:%S').time()
	    RA_2 = datetime.strptime(ending_task[i], '%H:%M:%S').time()
	    diff = datetime.combine(a, RA_2) - datetime.combine(a, RA_1)
	    task_completion_time.append(diff.total_seconds())
	mean_tasks = sum(task_completion_time)/len(task_completion_time)
	median_tasks = median(task_completion_time)
	print("Mean of tasks: ",mean_tasks)
	print("Median of tasks: ",median_tasks)
	#plot bar graph for mean and median of task completion
	objects = ('mean', 'median')
	y_pos = np.arange(len(objects))
	performance = [mean_tasks,median_tasks]
	plt.bar(y_pos,performance,align='center',alpha=0.5)
	plt.xticks(y_pos,objects)
	plt.ylabel('Time')
	plt.title('Mean and median time of task completion: (%s)'%algo)
	plt.show()

        
	

''''task completion time:  
(end time of task inWorker process) – (arrival time of task at Worker process)
job completion time:
(end time of the last reduce task) – (arrival time of job at Master)'''

filepath = sys.argv[1]
f = open(filepath)
logs=f.readlines()
algo=logs.pop(0)[:-1]
logs.sort(key=getTimeSec)
plot(logs,algo)
f.close() 


