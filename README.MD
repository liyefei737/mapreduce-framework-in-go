6.824 - Spring 2020
6.824 Lab 1: MapReduce

### A mapreduce framework implemented according to Google's original paper

Project Description:

A distributed MapReduce, consisting of two programs, the master and the worker. There will be just one master process, and one or more worker processes executing in parallel. In a real system the workers would run on a bunch of different machines, but for this lab you'll run them all on a single machine. The workers will talk to the master via RPC. Each worker process will ask the master for a task, read the task's input from one or more files, execute the task, and write the task's output to one or more files. The master should notice if a worker hasn't completed its task in a reasonable amount of time (for this lab, use ten seconds), and give the same task to a different worker.

We have given you a little code to start you off. The "main" routines for the master and worker are in main/mrmaster.go and main/mrworker.go; don't change these files. You should put your implementation in mr/master.go, mr/worker.go, and mr/rpc.go. 


Project highlights:

- native go RPC is used as the means for workers and the master to communicate.
- the master process has 2 ways to detect timed-out worker jobs: 1. use a thread runing in the backgorund to check periodically 2. when a worker finishes its job and notifies the master, the master can check wether the job exceeds the time limit.
- the master process is thread safe, and it creates a thread whenever it sees a request coming from a worker process asking for a task.
- temporary files(later removed) are used as a mechanism for us to work with timed-out jobs.
