#pragma once

// A thread interface.
class Thread {
 public:
    // The function executed by the thread.
    virtual void* run() = 0;

    // Function given as argument to pthread_create, and the Thread object as its argument.
    static void* call(void* thr) {
        return ((Thread*)thr)->run();
    };
};
