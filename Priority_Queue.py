class PriorityQueue:
    def __init__(self):
        self.queue=[]

    # Function to insert a new job into the priority queue
    def insert(self, job):
        # [{'0': [[('0_M0', 2)], [('0_R0', 4), ('0_R1', 1)]]}, {'1': [[('1_M0', 1)], [('1_R0', 2), ('1_R1', 4)]]}]
        job_id=list(job.keys())[0]
        d = {
            "job_id": job_id,
            "map_tasks": [],
            "reduce_tasks": [],
            "sent_tasks":[]
        }
        
        tasks = job[str(job_id)]
        d["map_tasks"] = tasks[0]
        d["reduce_tasks"] = tasks[1]
        self.queue.append([1,d])

    # Function to return the task from a job with the highest priority
    def getTask(self):
        task = None
        # If the queue is empty, return None
        if(len(self.queue)==0):
            return None

        # Find the job which has the highest priority
        max = 0
        for i in range(len(self.queue)): 
            if self.queue[i][0] > self.queue[max][0]: 
                max = i
            
        # If map tasks exist, return a map task
        if(len(self.queue[max][1]["map_tasks"])>0):
            task = self.queue[max][1]["map_tasks"].pop()
            self.queue[max][1]["sent_tasks"].append(task)
            # If all map tasks are completed, increment the priority
            if(len(self.queue[max][1]["map_tasks"])==0):
                self.queue[max][0] = 2
        # If all map tasks have been executed and reduce tasks exist, return a reduce task
        elif(len(self.queue[max][1]["sent_tasks"])==0 and len(self.queue[max][1]["reduce_tasks"])>0):
            task = self.queue[max][1]["reduce_tasks"].pop()
            # If all reduce tasks are complete, remove the job from the queue
            if(len(self.queue[max][1]["reduce_tasks"])==0):
                del self.queue[max]
        # If the reduce tasks cannot be executed because map tasks are still getting executed,
        # lower the priority and try to find another job
        elif( self.queue[max][0] == 2):
            priority = self.queue[max][0] - 1
            found = False
            # Find a job with a lower priority
            for i in range(len(self.queue)): 
                if(not found and self.queue[i][0] == priority):
                    # If map tasks exists, return a map task
                    if(len(self.queue[i][1]["map_tasks"])>0):
                        found = True
                        task = self.queue[i][1]["map_tasks"].pop()
                        self.queue[i][1]["sent_tasks"].append(task)
                        # If all map tasks are done, increment the priority
                        if(len(self.queue[i][1]["map_tasks"])==0):
                            self.queue[i][0] = 2
                        break
                    # If all map tasks have been executed and reduce tasks exist, return a reduce task
                    elif(len(self.queue[i][1]["sent_tasks"])==0 and len(self.queue[i][1]["reduce_tasks"])>0):
                        found = True
                        task = self.queue[i][1]["reduce_tasks"].pop()

                        # If all reduce tasks have been executed, remove the job from the queue
                        if(len(self.queue[i][1]["reduce_tasks"])==0):
                            del self.queue[i]
                        break

        return task 

    # Function to remove a task from the queue after execution
    def popTask(self, taskId):
        job_Id, taskType = taskId.split("_")
        # If the task is a reducer task, exit from the function
        if(taskType[0]=="R"):
            return None

        # If the task is a map task, find the corresponding job of the task in the priority queue
        # Remove the map task and append it to sent_tasks
        for i in range(len(self.queue)):
            if(self.queue[i][1]["job_id"]==job_Id):
                for j in range(len(self.queue[i][1]["sent_tasks"])):
                    if(self.queue[i][1]["sent_tasks"][j][0]==taskId):
                        del self.queue[i][1]["sent_tasks"][j]
                        return

    # Function to check if the queue is empty
    def isEmpty(self):
        return (len(self.queue)==0)

    # Function to display the queue
    def display(self):
        for job in self.queue:
            print(job)


