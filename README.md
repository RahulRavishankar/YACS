

<h1 align="center">Yet Another Centralized Scheduler</h1>

<div align="center">


</div>


## 📝 Table of Contents

- [About](#about)
- [Built Using](#built_using)
- [Authors](#authors)


## 🧐 About <a name = "about"></a>

> Big data workloads consist of multiple jobs from different applications. These workloads are too large to run on a single machine. Therefore, they are run on clusters of interconnected machines. A scheduling framework is used to manage and allocate the resources of the cluster (CPUs, memory, disk, network bandwidth, etc.) to the different jobs in the workload. 

> Our project YACS is a acentralized scheduling framework. The framework consists of one Master, which runs on a dedicated machine and manages the resources of the rest of the machines in the cluster.

> The other machines in the cluster have one Worker process running on each of them . The Master process makes scheduling decisions while the Worker processes execute the tasks and inform the Master when a task completes its execution.

> The Master listens for job requests and dispatches the tasks in the jobs to machines based on a scheduling algorithm. The scheduling algorithms we have aimplemented are Random,Round Robin and Least-Loaded.

> Each machine is partitioned int equal-sized resource encapsulations  called slots. The Master is informed of the number of machines and the number of slots in each machine with the help of a config file.

> The Worker processes listen for Task Launch messages from the Master. On receiving a Launch message, the Worker adds the task to the execution pool of the machine it runs on. The execution pool consists of all currently running tasks in the machine. 

> Once the remaining_duration of a task reaches 0, the Worker removes the task from its execution pool and reports to the Master that the task has completed its execution. 

> The framework respects the map-reduce dependency in the jobs. Therefore, when a map task completes execution, the Master will have to check if it satisfies the dependencies of any reduce tasks and whether the reduce tasks can now be launched. A job is said to have completed execution only when all the tasks in the job have finished executing.


## ⛏️ Built Using <a name = "built_using"></a>
Python
> concepts of multithreading, socket programming and logging were implemented


## ✍️ Authors <a name = "authors"></a>

- [@gaurikapoplai21](https://github.com/gaurikapoplai21) 
- [@rahul_5409](https://github.com/RahulRavishankar)
- [@nikijraj](https://github.com/nikijraj)
- [@keerthanamahadev](https://github.com/keerthanamahadev) 

