class PriorityQueue:
    def __init__(self):
        self.queue=[]

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

    def getTask(self):
        task = None
        if(len(self.queue)==0):
            return None
        max = 0
        for i in range(len(self.queue)): 
            if self.queue[i][0] > self.queue[max][0]: 
                max = i
            
        if(len(self.queue[max][1]["map_tasks"])>0):
            task = self.queue[max][1]["map_tasks"].pop()
            self.queue[max][1]["sent_tasks"].append(task)
            # If all map tasks are done, increment the priority
            if(len(self.queue[max][1]["map_tasks"])==0):
                self.queue[max][0] = 2
        elif(len(self.queue[max][1]["sent_tasks"])==0 and len(self.queue[max][1]["reduce_tasks"])>0):
            task = self.queue[max][1]["reduce_tasks"].pop()

            if(len(self.queue[max][1]["reduce_tasks"])==0):
                del self.queue[max]
        elif( self.queue[max][0] == 2):
            #If task not found, lower the priority and try again
            priority = self.queue[max][0] - 1
            found = False
            for i in range(len(self.queue)): 
                if(not found and self.queue[i][0] == priority):
                    if(len(self.queue[i][1]["map_tasks"])>0):
                        found = True
                        task = self.queue[i][1]["map_tasks"].pop()
                        self.queue[i][1]["sent_tasks"].append(task)
                        # If all map tasks are done, increment the priority
                        if(len(self.queue[i][1]["map_tasks"])==0):
                            self.queue[i][0] = 2
                        break
                    elif(len(self.queue[i][1]["sent_tasks"])==0 and len(self.queue[i][1]["reduce_tasks"])>0):
                        found = True
                        task = self.queue[i][1]["reduce_tasks"].pop()

                        if(len(self.queue[i][1]["reduce_tasks"])==0):
                            del self.queue[i]
                        break

        return task 

    
    def popTask(self, taskId):
        job_Id, taskType = taskId.split("_")
        if(taskType[0]=="R"):
            return None

        for i in range(len(self.queue)):
            if(self.queue[i][1]["job_id"]==job_Id):
                for j in range(len(self.queue[i][1]["sent_tasks"])):
                    if(self.queue[i][1]["sent_tasks"][j][0]==taskId):
                        del self.queue[i][1]["sent_tasks"][j]
                        return

    def isEmpty(self):
        return (len(self.queue)==0)

    
    def display(self):
        for job in self.queue:
            print(job)


