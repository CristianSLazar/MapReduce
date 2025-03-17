# Implementation

## Thread interface

Mapper and reducer threads are represented by classes that implement the Thread interface, providing a `run` function. Thread arguments are given as object parameters and `pthread_create` is called with the static `Thread::call` function and the Thread object as parameter, `Thread::call` simply calling the `run` method of the Thread object.

## Mapping

The main thread communicates with the mapper threads using a job pool (`Pool` object), each job containing a file id and the path to the file. Mappers read the content of the file word by word, process the words and then place the \<processed word, file id\> pairs in a thread local vector. The mappers also share a vector of 26 counters, which they use to count how many words starting with a letter there are.

After all jobs are finished, when the mappers try to take a job, they will get a NULL job placed by the main thread after all the other jobs, indicating there are no more jobs for the mappers. After breaking out of the job loop, mappers wait at a barrier for all the other mappers, then they create aggregate jobs for the reducer threads from their local vector of pairs and place them in the job pool.

## Reducing

Reducers first wait at a barrier for all the other reducer threads, as well as for the mapper threads (the same barrier mentioned before), then they start taking jobs.

### Aggregate jobs

Each aggregate job contains and iterator to the begining and the end of a vector of \<processed word, file id\> pairs. The reducers use a vector of 26 concurrent multimaps, of for each letter of the alphabet, separeting the words by their first letter and then placing the \<processed word, file id\> key-value pair in it's corresponding multimap.
Each time a pair is placed in the multimap, the counter of words starting with the processed word's letter is decremented; if it reaches 0, it means all words starting with that letter have been aggregated, so a sort divide job is created and placed in the job pool.

### Sort jobs

The aggregated lists are sorted using parallel merge sort. For this, 2 types of sort jobs exist: divide and merge jobs.

#### Divide job

A divide job recieves a range of \<processed word, set\<file id\>\> pairs, a merge job pointer and a counter pointer (merge job and the counter are NULL if it's the first divide job). Reducers divide the range into 2 more divide jobs, creating a merge job and a counter shared by the 2 child jobs, initialize with the number of child jobs (in this case, 2), until the range is small enough to sort sequentially (less then 3 elements). The unit ranges are sorted and the counter is decremented; if it reaches 0, it means all the child jobs have finished and the merge job can be placed in the job pool.

#### Merge job

A merge job recieves the same parameters as the divide job that created it. It merges one half of the range with the other, replacing the old range with the merged one. The counter is decremented and, if it reaches 0, it means both it and the other child job have finished, so it can place the parent merge job in the job pool. When the first merge job (the one with NULL counter and parent waiting job) finishes, the output string for the \<letter\>.out file is computed from the sorted list, the output file is mapped into memory and write jobs.

### Write job

A write job recieves the source and destination addresses, an offset, the length of the string to write, a counter indicating how many write jobs are left, and the file descriptor of the output file. It writes at `destination + offset` from `source + offset` `length` bytes, then it decrements the counter. If it reaches 0, it unmaps the file and closes its corresponding file descriptor, then it decrements a shared counter withing the context of the reducer, indicating the number of output files still not writen. When this counter reaches 0, it means all output files have been write, so the job signals the main thread to send NULL jobs to the reducers to kill them.