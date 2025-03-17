#pragma once

#include <bits/stdc++.h>
#include <pthread.h>
#include <semaphore.h>

using namespace std;

// A job pool, utilizing a FIFO implementation.
class Pool {
 private:
    queue<void*> jobPool;
    // Semaphore doubling as a counter for the number of jobs in the queue.
    sem_t sem;
    // Spinlock for ensuring exclusiv insertion/deletion of jobs.
    pthread_spinlock_t lock;

 public:
    Pool();
    ~Pool();
   
    void* takeJob();
    void placeJob(void* job);
};
