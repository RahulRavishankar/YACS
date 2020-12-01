class PriorityQueue:
    def __init__(self):
        self.queue=[]

    def insert(self, job):
        self.queue.append([1,job])

    def getTask(self):
        priorities=[x[0] for x in self.queue]
        priorindex=priorities.index(max(priorities))
        job=self.queue[priorindex][1]
        key=list(job.keys())[0]
        tasks=list(job.values())[0]
        mappers=tasks[0]
        reducers=tasks[1]
        if(mappers):
            task=mappers.pop()  
            if(len(mappers)==0):
                self.queue[priorindex][0]=2
        else:
            task=reducers.pop()
            if(len(reducers)==0):
                del self.queue[priorindex]

        return task
    
    def isEmpty(self):
        return (len(self.queue)==0)

    
    def display(self):
        for job in self.queue:
            print(job)


