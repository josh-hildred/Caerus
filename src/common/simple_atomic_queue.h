//
// Author: Joshua Hildred jt<lastname>@gmail.com>
//

#ifndef _DB_ATOMIC_QUEUE_H_
#define _DB_ATOMIC_QUEUE_H_

#include "mutex.h"
#include <queue>
#include <boost/lockfree/queue.hpp>


template <typename T>
class SAQ {
    boost::lockfree::queue<T> queue_;
    public:
    bool getNext(T &);
    bool putback(T &);
    SAQ() : queue_() {};
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
