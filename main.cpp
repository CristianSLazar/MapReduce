#include <bits/stdc++.h>
#include <pthread.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <atomic>

#include "Mapper.hpp"
#include "Reducer.hpp"

using namespace std;

// Parses the command line argumenst, returning the number of mapper threads and the number of reducer threads to create,
// and a vector of paths to the input files meant to be mapped by the mapper threads. 
void init(char** argv, unsigned int &numMappers, unsigned int &numReducers, vector<string> &inFiles) {
    numMappers = atoi(argv[1]);
    numReducers = atoi(argv[2]);
    ifstream ifs(argv[3]);

    int numInFiles;
    ifs >> numInFiles;

    string file;
    while (numInFiles-- != 0) {
        ifs >> file;
        inFiles.push_back(file);
    }

    ifs.close();
}

int main(int argc, char **argv)
{
    if (argc != 4) {
        cerr << "Invalid number of arguments" << endl;
        return 1;
    }

    int rc;
    unsigned int numMappers, numReducers;
    vector<string> inFiles;

    init(argv, numMappers, numReducers, inFiles);

    vector<pair<void*, pthread_t>> threads(numMappers + numReducers, {NULL, 0});

    // The pool of jobs used by both the mapper and the reducer threads.
    Pool jobPool;
    // Each mapper thread has an entry in the localMappings vector, where they store the result of mapping the input files
    // as a vector of pairs <word, file id>.
    vector<vector<pair<string, unsigned int>>> localMappings(numMappers, vector<pair<string, unsigned int>>());
    // A vector of counters to keep track of how many words starting with a given letter there are among all files.
    vector<atomic_uint32_t> numWordsPerFirstLetter(26);
    for (auto &elem : numWordsPerFirstLetter) {
        atomic_init(&elem, 0);
    }
    // A conditional variable and a corresponding spinlock to ensure that only one of the mapper threads,
    // after all files have been mapped, checks how many of the output files will be empty (there are no words starting with the file's letter),
    // creates those empty files, and decrements the output file counter.
    bool countedEmptyFiles = false;
    pthread_spinlock_t countedEmptyFilesLock;
    pthread_spin_init(&countedEmptyFilesLock, 0);
    // A counter keeping track of how many output files still need to be created/written.
    atomic_uint32_t filesCounter{26};

    // A barrier to syncronize the mapper threads and the reducer threads.
    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, NULL, numMappers + numReducers);
    // An empirical value for the size of a reducer aggregation job.
    unsigned int reducerJobSize = 64;

    for (unsigned int i = 0; i < numMappers; i++) {
        threads[i].first = new Mapper(
                &jobPool,
                &localMappings[i],
                &numWordsPerFirstLetter,
                &countedEmptyFiles,
                &countedEmptyFilesLock,
                &filesCounter,
                &barrier,
                reducerJobSize
            );
        rc = pthread_create(&threads[i].second, NULL, &Thread::call, threads[i].first);
        if (rc < 0) {
            perror("pthread_create");
        }
    }

    // Words are grouped by their first letter and placed in their corresponding multimap for aggregation.
    vector<ConcurrentMultimap<string, unsigned int>> mulMaps(26, ConcurrentMultimap<string, unsigned int>());
    // A semaphore by which the main thread is alerted that the reducers finished their jobs.
    sem_t reducersFinished;
    sem_init(&reducersFinished, 0, 0);
    // A vector for storing the sorted lists of <word, file id> pairs for every output file.
    vector<vector<pair<string, set<unsigned int>>>> sortedLists(26, vector<pair<string, set<unsigned int>>>());

    for (unsigned int i = numMappers; i < numMappers + numReducers; ++i) {
        threads[i].first = new Reducer(
                &jobPool,
                &barrier,
                &reducersFinished,
                &mulMaps,
                &numWordsPerFirstLetter,
                &sortedLists,
                &filesCounter);
        rc = pthread_create(&threads[i].second, NULL, &Thread::call, threads[i].first);
        if (rc < 0) {
            perror("pthread_create");
        }
    }

    // The first file has id 1.
    unsigned int id = 1;
    for (auto file : inFiles) {
        jobPool.placeJob(new Mapper::Args({ id, file }));
        ++id;
    }

    // Place kill pills in the pool, signaling the mappers there are no more jobs.
    for (unsigned int i = 0; i < numMappers; i++)
        jobPool.placeJob(NULL);

    for (unsigned int i = 0; i < numMappers; i++) {
        pthread_join(threads[i].second, NULL);
        delete (Mapper*) threads[i].first;
    }

    sem_wait(&reducersFinished);

    // Place kill pills for the reducers once they finish their jobs.
    for (unsigned int i = 0; i < numReducers; i++)
        jobPool.placeJob(NULL);

    for (unsigned int i = numMappers; i < numMappers + numReducers; ++i) {
        pthread_join(threads[i].second, NULL);
        delete (Reducer*) threads[i].first;
    }

    return 0;
}