#pragma once

#include <unistd.h>
#include <atomic>

#include "Thread.hpp"
#include "Pool.hpp"
#include "Reducer.hpp"

// Class for mapper threads.
class Mapper : Thread {
 private:
    // The pool of jobs, populated by main and used to place aggragate jobs for the reducers after mapping.
    Pool* jobPool;
    // Vector to store the result of mapping the input files as a vector of pairs <word, file id>.
    vector<pair<string, unsigned int>>* localMapping;
    // A vector of counters to keep track of how many words starting with a given letter there are among all files.
    vector<atomic_uint32_t>* numWordsPerFirstLetter;
    // A conditional variable and a corresponding spinlock to ensure that only one of the mapper threads,
    // after all files have been mapped, checks how many of the output files will be empty (there are no words starting with the file's letter),
    // creates those empty files, and decrements the output files counter.
    bool* countedEmptyFiles;
    pthread_spinlock_t *countedEmptyFilesLock;
    // A counter keeping track of how many output files still need to be created/written.
    atomic_uint32_t *filesCounter;

    // A barrier to syncronize the mapper threads and the reducer threads.
    pthread_barrier_t *barrier;
    // An empirical value for the size of a reducer aggregation job.
    unsigned int reducerJobSize;

 public:
    struct Args {
        // The id of the file.
        const unsigned int id;
        // The path to the input file.
        const string file;
    };

    Mapper(
        Pool *jobPool,
        vector<pair<string, unsigned int>>* localMapping,
        vector<atomic_uint32_t>* numWordsPerFirstLetter,
        bool* countedEmptyFiles,
        pthread_spinlock_t *countedEmptyFilesLock,
        atomic_uint32_t *filesCounter,
        pthread_barrier_t *barrier,
        unsigned int reducerJobSize)
        : jobPool(jobPool)
        , localMapping(localMapping)
        , numWordsPerFirstLetter(numWordsPerFirstLetter)
        , countedEmptyFiles(countedEmptyFiles)
        , countedEmptyFilesLock(countedEmptyFilesLock)
        , filesCounter(filesCounter)
        , barrier(barrier)
        , reducerJobSize(reducerJobSize)
        {}

    void* run() override;
};
