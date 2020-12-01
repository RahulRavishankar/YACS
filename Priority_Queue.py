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

    # def getTask(self):
    #     priorities=[x[0] for x in self.queue]
    #     priorindex=priorities.index(max(priorities))
    #     job=self.queue[priorindex][1]
    #     key=list(job.keys())[0]
    #     tasks=list(job.values())[0]
    #     mappers=tasks[0]
    #     reducers=tasks[1]
    #     if(mappers):
    #         task=mappers.pop()  
    #         if(len(mappers)==0):
    #             self.queue[priorindex][0]=2
    #     else:
    #         task=reducers.pop()
    #         if(len(reducers)==0):
    #             del self.queue[priorindex]

    #     return task
    
    def popTask(self, taskId):
        job_Id, taskType = taskId.split("_")
        if(taskType[0]=="R"):
            return None

        for i in range(len(self.queue)):
            if(self.queue[i][1]["job_id"]==job_Id):
                for j in  range(len(self.queue[i][1]["sent_tasks"])):
                    print(self.queue[i][1]["sent_tasks"][j])
                    if(self.queue[i][1]["sent_tasks"][j][0]==taskId):
                        del self.queue[i][1]["sent_tasks"][j]

    def isEmpty(self):
        return (len(self.queue)==0)

    
    def display(self):
        for job in self.queue:
            print(job)


p = PriorityQueue()
p.insert({'0': [[('0_M0', 2)], [('0_R0', 4), ('0_R1', 1)]]})
p.insert({'1': [[('1_M0', 1),("1_M1",4),("1_M2",3)], [('1_R0', 2), ('1_R1', 4)]]})
print("Yoooooo",p.getTask())
print(p.popTask("0_M0"))
print("Yoooooo",p.getTask())
print("Yoooooo",p.getTask())
print("Yoooooo",p.getTask())
print("Yoooooo",p.getTask())
print("Yoooooo",p.getTask())
print("Yoooooo",p.getTask())
print(p.popTask("1_M0"))
print(p.popTask("1_M1"))
print("Yoooooo",p.getTask())
print(p.popTask("1_M2"))
print("Yoooooo",p.getTask())
print(p.popTask("1_R1"))
print(p.popTask("1_R3"))
print("Yoooooo",p.getTask())
print(p.popTask("1_M2"))
print("Yoooooo",p.getTask())
print(p.popTask("1_R1"))
print(p.popTask("1_R3"))


