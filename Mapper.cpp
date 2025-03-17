#include "Mapper.hpp"

void* Mapper::run() {
    Args* job;

    while (true) {
        job = (Args*)this->jobPool->takeJob();
        // If it is a kill pill (NULL job), it means that all the mapping jobs have been taken,
        // so the mapper breaks out of the mapping loop.
        if (job == NULL)
            break;

        ifstream ifs(job->file);
        string word;
        while (ifs >> word) {
            string processedWord;
            // Every word is processed by removing all non-letter characters and making all letters lowercase.
            for (char character : word) {
                if ('a' <= character && character <= 'z')
                    processedWord.push_back(character);
                else if ('A' <= character && character <= 'Z')
                    processedWord.push_back(character + ('a' - 'A'));
            }

            // Ensure the word isn't empty post processing.
            if (!processedWord.empty()) {
                localMapping->push_back({processedWord, job->id});
                this->numWordsPerFirstLetter->at(processedWord.c_str()[0] - 'a').fetch_add(1);
            }
        }

        ifs.close();
        delete job;
    }

    // Wait for all other mapper threads.
    pthread_barrier_wait(this->barrier);

    if (!*this->countedEmptyFiles) {
        // Ensure only one thread executes the following code.
        pthread_spin_lock(this->countedEmptyFilesLock);
        if (!*this->countedEmptyFiles) {
            for (unsigned int i = 0; i < 26; i++)
                // Check if there are any words starting with any one letter.
                if (this->numWordsPerFirstLetter->at(i) == 0) {
                    // If not, create/truncate the output file corresponding to that letter.
                    string path;
                    path.push_back(i + 'a');
                    path.append(".txt");
                    
                    int fd = open(path.c_str(), O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
                    if (fd < 0) {
                        perror("open");
                        exit(1);
                    }
                    int rc = close(fd);
                    if (rc < 0) {
                        perror("close");
                        exit(1);
                    }

                    // Decrement the number of output files remaining to be created/written.
                    this->filesCounter->fetch_sub(1);
                }
            *this->countedEmptyFiles = true;
        }
        pthread_spin_unlock(this->countedEmptyFilesLock);
    }

    // Create aggregate jobs for the reducers and place them in the job pool.
    for (auto it = this->localMapping->begin(); it != this->localMapping->end(); it += min((unsigned int)(this->localMapping->end() - it), this->reducerJobSize)) {
        jobPool->placeJob(new Reducer::JobAgregate(it, it + min((unsigned int)(this->localMapping->end() - it), this->reducerJobSize)));
    }

    return NULL;
}
