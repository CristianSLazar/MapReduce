#pragma once

// Interface for reducer jobs.
class Job {
 public:
    virtual ~Job() {};
    virtual void* execute(void* arg) = 0;
};
