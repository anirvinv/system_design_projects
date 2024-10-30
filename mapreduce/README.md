# Notes

1. mrsequential.go is the sequential coordinator $\rightarrow$ use this for reference
2. My code needs to be written in **mr/coordinator.go, mr/worker.go, and mr/rpc.go**
   - what is rpc?
     - remote procedure call, we can use stubs to execute code on remote machines pretending like we did it on our own machine

## Coordinator

- This is the RPC server
- The workers _(clients)_ will ask it for tasks

## Map phase

- Each worker produces $nReduce$ intermediate files --> each key is mapped to a one of these files with a hash

Intermediate filenames take the following format: `mr-X-Y`, where X is the map task number and Y is the reduce task number

## Reduce phase

- Workers are assigned a reduce# and they read all files that end in this reduce#

Output files are named as follows: `mr-out-reducetask#`

## Flow

1. Coordinator is started up
2. Workers are started up
3. Worker asks the coordinator for a task
4. Coordinator looks in the task list and sends a pending task to the worker
5. Worker finishes it and reports back to coordinator
