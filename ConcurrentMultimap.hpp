#pragma once

#include <bits/stdc++.h>
#include <pthread.h>

using namespace std;

// A basic concurrent multimap template, using a lock for insertion.
template<typename _K, typename _V>
class ConcurrentMultimap {
 private:
    unordered_map<_K, set<_V>> mapObj;
    pthread_spinlock_t mapLock;

 public:
    ConcurrentMultimap() {
        pthread_spin_init(&this->mapLock, 0);
    }

    ~ConcurrentMultimap() {
        pthread_spin_destroy(&this->mapLock);
    }

    void insert(_K key, _V value) {
        pthread_spin_lock(&this->mapLock);
        if (this->mapObj.find(key) == this->mapObj.end())
            this->mapObj[key] = set<_V>();
        this->mapObj[key].insert(value);
        pthread_spin_unlock(&this->mapLock);
    }

    typename unordered_map<_K, set<_V>>::iterator begin() {
        return this->mapObj.begin();
    }

    typename unordered_map<_K, set<_V>>::iterator end() {
        return this->mapObj.end();
    }

    unsigned int size() {
        return this->mapObj.size();
    }

    bool empty() {
        return this->mapObj.empty();
    }
};
