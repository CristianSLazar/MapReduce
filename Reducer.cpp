#include "Reducer.hpp"

void* Reducer::run() {
    // Wait for the mappers to finish mapping.
    pthread_barrier_wait(this->barrier);
    while (true) {
        Job *job = (Job*)context.jobPool->takeJob();
        // If its a kill pill (NULL job), it means the reducers have finished their jobs, so it can break the reducing loop.
        if (job == NULL)
            break;

        bool ret = job->execute(&this->context);
        // If 'ret' is true, it means the main thread must be signaled that the reducers are finished.
        if (ret == true)
            sem_post(this->signalMain);
        delete job;
    }

    return NULL;
}

bool Reducer::compare(pair<string, set<unsigned int>> o1, pair<string, set<unsigned int>> o2) {
    bool rez;
    if (o1.first.c_str()[0] == o2.first.c_str()[0]) {
        if (o1.second.size() == o2.second.size())
            return o1.first < o2.first;
        
        return o1.second.size() > o2.second.size();
    }

    return o1.first < o2.first;
}

void* Reducer::JobAgregate::execute(void* arg) {
    Context* context = (Context *) arg;
    for (auto it = this->begin; it != this->end; it++) {
        // Find the index of the multimap in which the word belongs based on its first letter.
        unsigned int index = it->first.c_str()[0] - 'a';
        context->mulMaps->at(index).insert(it->first, it->second);
        
        unsigned int counter = context->numWordsPerFirstLetter->at(index).fetch_sub(1);
        // If the counter reached 1 predecrementation, it means all the words starting with the current staring letter have been aggregated,
        // so the sorting of those words can begin.
        if (counter == 1) {
            context->sortedLists->at(index) = vector<pair<string, set<unsigned int>>>(context->mulMaps->at(index).begin(), context->mulMaps->at(index).end());
            context->jobPool->placeJob(
                new JobSortDivide(
                    context->sortedLists->at(index).begin(),
                    context->sortedLists->at(index).end(),
                    NULL,
                    NULL));
        }
    }

    return (void*) false;
}

void* Reducer::JobSortDivide::execute(void* arg) {
    Context *context = (Context *) arg;
    unsigned int jobSize = this->end - this->begin;

    // Jobs smaller or equal to 2 are no longer divider.
    if (jobSize <= 2) {
        if (jobSize == 2)
            if (!compare(*this->begin, *(this->begin + 1))) {
                auto aux = *this->begin;
                *this->begin = *(this->begin + 1);
                *(this->begin + 1) = aux;
            }

        // If the counter is NULL, it means no divizion of the job is necessary, and the word/words are written to the output file.
        if (this->parentCounter == NULL) {
            string path;
            path.push_back(this->begin->first.c_str()[0]);
            path.append(".txt");
            int fd = open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
            if (fd < 0) {
                perror("open");
                exit(1);
            }

            string* str = new string;
            // The content of the output file is computed.
            for (auto it = this->begin; it != this->end; it++) {
                str->append(it->first)
                    .append(":[");

                auto idLast = it->second.end();
                --idLast;
                for (auto idIter = it->second.begin(); idIter != idLast; idIter++) {
                    str->append(to_string(*idIter))
                        .push_back(' ');
                }
                str->append(to_string(*idLast))
                    .append("]\n");
            }

            // The file is truncated to its new size.
            ftruncate(fd, str->size());
            // The file is mapped to memory to allow for concurrent writing.
            char* map = (char*)mmap(NULL, str->size(), PROT_WRITE, MAP_SHARED, fd, 0);
            if (map == MAP_FAILED) {
                perror("map");
                exit(1);
            }

            // Specify access to the mapped file will be random.
            madvise(map, str->size(), MADV_RANDOM);

            // Empiric value for the job write size.
            unsigned int writeJobSize = 1024;
            // Counter indicating how many write jobs haven't finished.
            atomic_uint32_t *counter = new atomic_uint32_t{(unsigned int)(str->size() / writeJobSize + ((str->size() % writeJobSize) == 0 ? 0 : 1))};
            // Create the write jobs.
            for (unsigned int offset = 0; offset < str->size(); offset += min((unsigned int)str->size() - offset, writeJobSize))
                context->jobPool->placeJob(new JobWrite(str, map, offset, min((unsigned int)str->size() - offset, writeJobSize), counter, fd));
            
            return (void *) false;
        }

        // If the counter isn't NULL, decrement it to indicate one of its children finished execution.
        auto counter = this->parentCounter->fetch_sub(1);
        // If the counter predecrementation reached 1, it means all the jobs the parent was waiting on have finished,
        // so the parent merge job can be placed in the job pool.
        if (counter == 1) {
            context->jobPool->placeJob(this->parentWaitingJob);
            delete this->parentCounter;
        }

        return (void*) false;
    }

    // A new counter and waiting job are created.
    atomic_uint32_t *waitingCounter = new atomic_uint32_t{2};
    JobSortMerge *newWaitingJob = new JobSortMerge(this->begin, this->end, this->parentCounter, this->parentWaitingJob);

    // 2 child jobs are created, dividing the problem further more.
    JobSortDivide *jobLeft = new JobSortDivide(this->begin, this->begin + jobSize / 2, waitingCounter, (void*) newWaitingJob);
    JobSortDivide *jobRight = new JobSortDivide(this->begin + jobSize / 2, this->end, waitingCounter, (void*) newWaitingJob);

    context->jobPool->placeJob(jobLeft);
    context->jobPool->placeJob(jobRight);

    return (void*) false;
}

void* Reducer::JobSortMerge::execute(void* arg) {
    Context *context = (Context*) arg;
    vector<pair<string, set<unsigned int>>> mergedVec;
    unsigned int jobSize = this->end - this->begin;

    // Merge the results of the child jobs.
    std::merge(this->begin, this->begin + jobSize / 2,
        this->begin + jobSize / 2, this->end, std::back_inserter(mergedVec), Compare());

    // Replace the original range with the sorted one.
    auto itSrc = mergedVec.begin();
    auto itDest = this->begin;
    for (unsigned int i = 0; i < jobSize; i++) {
        *itDest = *itSrc;
        itDest++;
        itSrc++;
    }

    // If the counter is NULL, it means the whole vector has been sorted.
    if (this->parentCounter == NULL) {
        string path;
        path.push_back(this->begin->first.c_str()[0]);
        path.append(".txt");
        int fd = open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
        if (fd < 0) {
            perror("open");
            exit(1);
        }

        string* str = new string;
        // The content of the output file is computed.
        for (auto it = this->begin; it != this->end; it++) {
            str->append(it->first)
                .append(":[");

            auto idLast = it->second.end();
            --idLast;
            for (auto idIter = it->second.begin(); idIter != idLast; idIter++) {
                str->append(to_string(*idIter))
                    .push_back(' ');
            }
            str->append(to_string(*idLast))
                .append("]\n");
        }

        // The file is truncated to its new size.
        ftruncate(fd, str->size());
        // The file is mapped to memory to allow for concurrent writing.
        char* map = (char*)mmap(NULL, str->size(), PROT_WRITE, MAP_SHARED, fd, 0);
        if (map == MAP_FAILED) {
            perror("map");
            exit(1);
        }

        // Specify access to the mapped file will be random.
        madvise(map, str->size(), MADV_RANDOM);

        // Empiric value for the job write size.
        unsigned int writeJobSize = 1024;
        // Counter indicating how many write jobs haven't finished.
        atomic_uint32_t *counter = new atomic_uint32_t{(unsigned int)(str->size() / writeJobSize + ((str->size() % writeJobSize) == 0 ? 0 : 1))};
        // Create the write jobs.
        for (unsigned int offset = 0; offset < str->size(); offset += min((unsigned int)str->size() - offset, writeJobSize))
            context->jobPool->placeJob(new JobWrite(str, map, offset, min((unsigned int)str->size() - offset, writeJobSize), counter, fd));
        
        return (void *) false;
    }
    
    // If the counter isn't NULL, decrement it to indicate one of its children finished execution.
    auto counter = this->parentCounter->fetch_sub(1);
    // If the counter predecrementation reached 1, it means all the jobs the parent was waiting on have finished,
    // so the parent merge job can be placed in the job pool.
    if (counter == 1) {
        context->jobPool->placeJob(this->parentWaitingJob);
        delete this->parentCounter;
    }

    return (void*) false;
}

void* Reducer::JobWrite::execute(void* arg) {
    memcpy(this->dest + this->offset, this->src->c_str() + this->offset, this->len);

    auto counter = this->counter->fetch_sub(1);
    // If the number of write jobs predecrementation is 1, it means all the write jobs for the current file have finished,
    // so the mapped file can be unmapped and the file descriptor closed.
    if (counter == 1) {
        munmap(this->dest, this->src->size());
        close(this->fd);
        delete src;
        delete this->counter;

        auto counter = ((Context *)arg)->filesCounter->fetch_sub(1);
        // If the number of remaining output files to compute predecementation is 1, it means all the output files have been created/written,
        // so the reducers have finished their jobs and can signal the main thread.
        if (counter == 1)
            return (void*) true;
    }

    return (void*) false;
}
