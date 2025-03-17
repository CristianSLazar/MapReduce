#include "Pool.hpp"

Pool::Pool() {
    sem_init(&this->sem, 0, 0);
    pthread_spin_init(&this->lock, 0);
}

Pool::~Pool() {
    sem_destroy(&this->sem);
    pthread_spin_destroy(&this->lock);
}

void* Pool::takeJob() {
    sem_wait(&this->sem);
    pthread_spin_lock(&this->lock);
    void* job = this->jobPool.front();
    this->jobPool.pop();
    pthread_spin_unlock(&this->lock);

    return job;
}

void Pool::placeJob(void* job) {
    pthread_spin_lock(&this->lock);
    this->jobPool.push(job);
    sem_post(&this->sem);
    pthread_spin_unlock(&this->lock);
}
