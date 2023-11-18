//
// Author: Joshua Hildred jt<lastname>@gmail.com>
//

#ifndef _DB_ATOMIC_QUEUE_H_
#define _DB_ATOMIC_QUEUE_H_

#include "mutex.h"
#include <queue>
#include <boost/lockfree/queue.hpp>

// simple atomic FIFO queue

#define LOCKFREEQUEUE

#ifndef LOCKFREEQUEUE
template <typename T>
class SAQ {
    Mutex mutex_;
    std::queue<T> queue_;
public:
    //bool hasNext();
    //bool getNext(T *);
    //T getNext();
    //void putback(T);
    bool getNext(T &);
    bool putback(T &);
    //uint64_t size();
    SAQ() : queue_() {};
};

template<typename T>
bool SAQ<T>::putback(T & t) {
    Lock l(&mutex_);
    queue_.push(t);
    return true;
}

template<typename T>
bool SAQ<T>::getNext(T &t) {
    if (queue_.size() == 0) {
        return false;
    }
    Lock l(&mutex_);
    if(queue_.empty()) {
        return false;
    }
    t = queue_.front();
    queue_.pop();
    return true;
}



/*template<typename T>
bool SAQ<T>::hasNext() {
    Lock l(&mutex_);
    return !queue_.empty();
}
// caller must free ret
template<typename T>
T SAQ<T>::getNext() {
    Lock l(&mutex_);
    T ret = queue_.front();
    queue_.pop();
    return ret;
}

template<typename T>
bool SAQ<T>::getNext(T * ret) {
    Lock l(&mutex_);
    if(queue_.empty())
    {
        return false;
    }
    *ret = queue_.front();
    queue_.pop();
    return true;
}

template<typename T>
void SAQ<T>::putback(T e) {
    Lock l(&mutex_);
    queue_.push(e);
}

template<typename T>
uint64_t SAQ<T>::size() {
    //Lock l(&mutex_);
    return queue_.size();
}*/
#else

template <typename T>
class SAQ {
    boost::lockfree::queue<T> queue_;
    public:
    bool getNext(T &);
    bool putback(T &);
    SAQ() : queue_(10000) {};
};


template<typename T>
bool SAQ<T>::getNext(T & ret) {
    return queue_.pop(ret);

}

template<typename T>
bool SAQ<T>::putback(T & e) {
    return queue_.push(e);
}

#endif



#endif //ALVINDB_ATOMIC_QUEUE_H
