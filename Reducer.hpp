#pragma once

#include <unistd.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <atomic>

#include "Thread.hpp"
#include "Pool.hpp"
#include "ConcurrentMultimap.hpp"
#include "Job.hpp"

class Reducer : Thread {
 private:
    // Structure containing the context in which a job is executed (given as argument to the job).
    struct Context {
        Pool* jobPool;
        // Words are grouped by their first letter and placed in their corresponding multimap for aggregation.
        vector<ConcurrentMultimap<string, unsigned int>> *mulMaps;
        // A vector of counters to keep track of how many words starting with a given letter there are among all files.
        vector<atomic_uint32_t> *numWordsPerFirstLetter;
        // A vector for storing the sorted lists of <word, file id> pairs for every output file.
        vector<vector<pair<string, set<unsigned int>>>> *sortedLists;
        // A counter keeping track of how many output files still need to be created/written.
        atomic_uint32_t *filesCounter;
    };

    Context context;

    // Barrier for syncronization with the mapper threads.
    pthread_barrier_t *barrier;

    // A semaphore by which the main thread is alerted that the reducers finished their jobs.
    sem_t *signalMain;

    // Comparetor class for sorting the aggregated lists.
    class Compare {
     public:
        bool operator()(const pair<string, set<unsigned int>> o1, const pair<string, set<unsigned int>> o2) {
            return Reducer::compare(o1, o2);
        }
    };

 public:
    Reducer(
        Pool* jobPool,
        pthread_barrier_t *barrier,
        sem_t *signalMain,
        vector<ConcurrentMultimap<string, unsigned int>> *mulMaps,
        vector<atomic_uint32_t> *numWordsPerFirstLetter,
        vector<vector<pair<string, set<unsigned int>>>> *sortedLists,
        atomic_uint32_t *filesCounter)
        : barrier(barrier)
        , signalMain(signalMain)
        , context({ jobPool, mulMaps, numWordsPerFirstLetter, sortedLists, filesCounter })
        {};

    // Aggregate job class.
    class JobAgregate : Job {
        // The range of pairs to aggregate.
        vector<pair<string, unsigned int>>::iterator begin;
        vector<pair<string, unsigned int>>::iterator end;

     public:
        JobAgregate(
            vector<pair<string, unsigned int>>::iterator begin,
            vector<pair<string, unsigned int>>::iterator end)
            : begin(begin)
            , end(end)
            {}
        ~JobAgregate() {}

        void *execute(void* arg) override;
    };
    // Divide job class.
    class JobSortDivide : Job {
        // The range of pairs to divide.
        vector<pair<string, set<unsigned int>>>::iterator begin;
        vector<pair<string, set<unsigned int>>>::iterator end;

        // Counter keeping track of how many jobs the merge job is waiting on.
        atomic_uint32_t *parentCounter;
        // The merge job to add after the child jobs are finished.
        void *parentWaitingJob;

     public:
        JobSortDivide(
            vector<pair<string, set<unsigned int>>>::iterator begin,
            vector<pair<string, set<unsigned int>>>::iterator end,
            atomic_uint32_t *parentCounter,
            void *parentWaitingJob)
            : begin(begin)
            , end(end)
            , parentCounter(parentCounter)
            , parentWaitingJob(parentWaitingJob)
            {}
        ~JobSortDivide() {}
        
        void *execute(void* arg) override;
    };
    class JobSortMerge : Job {
        // The range of pairs to merge.
        vector<pair<string, set<unsigned int>>>::iterator begin;
        vector<pair<string, set<unsigned int>>>::iterator end;
        
        // Counter keeping track of how many jobs the merge job is waiting on.
        atomic_uint32_t *parentCounter;
        // The merge job to add after the child jobs are finished.
        void *parentWaitingJob;

     public:
        JobSortMerge(
            vector<pair<string, set<unsigned int>>>::iterator begin,
            vector<pair<string, set<unsigned int>>>::iterator end,
            atomic_uint32_t *parentCounter,
            void *parentWaitingJob)
            : begin(begin)
            , end(end)
            , parentCounter(parentCounter)
            , parentWaitingJob(parentWaitingJob)
            {}
        JobSortMerge() {}

        void *execute(void* arg) override;
    };
    // Write job class.
    class JobWrite : Job {
        string *src;
        char *dest;
        // The offset in the src/dest at which to read/write the file.
        unsigned int offset;
        unsigned int len;

        // Counter indicating how many write jobs are left for the output file.
        atomic_uint32_t *counter;
        // The output file's file descriptor.
        int fd;

     public:
        JobWrite(
            string *src,
            char *dest,
            unsigned int offset,
            unsigned int len,
            atomic_uint32_t *counter,
            int fd)
            : src(src)
            , dest(dest)
            , offset(offset)
            , len(len)
            , counter(counter)
            , fd(fd)
            {}
        ~JobWrite() {};

        void *execute(void* arg) override;
    };

    void* run() override;

 private:
    // Compare function for sorting the pairs after aggregation.
    static bool compare(pair<string, set<unsigned int>> o1, pair<string, set<unsigned int>> o2);
};
