//
// Author: Joshua Hildred jt<lastname>@gmail.com>
//

#ifndef _DB_CONCURRENT_RESERVABLE_VECTOR_H_
#define _DB_CONCURRENT_RESERVABLE_VECTOR_H_

#include "mutex.h"
#include <atomic>
#include <glog/logging.h>
#include <queue>
#include <set>
#include <utility>
#include <vector>

template<class V>
class ConcurrentReservableVector
{

    Mutex mtx;
    V* data_;
    std::atomic_uint_fast64_t size_;
    uint64_t max_size_;
    std::atomic_uint_fast64_t num_reserved;
    void increase_size()
    {
        max_size_ = max_size_ * 2;
        V* tmp = new V[max_size_];
        for (int i = 0; i < size_; ++i) {
            tmp[i] = data_[i];
        }
        delete [] data_;
        data_ = tmp;
    }
public:
    friend class rangeIterator;
    class rangeIterator
    {
        ConcurrentReservableVector<V> * container_;
        uint64_t pos;
        uint64_t range_start_;
        uint64_t range_end_;
    public:
        bool operator==(rangeIterator rhs)
        {
            return pos == rhs.pos;
        }
        bool operator!=(rangeIterator rhs)
        {
            return pos != rhs.pos;
        }
        V& operator*()
        {
            return container_->data_[pos];
        }
        V* operator->()
        {
            return &container_->data_[pos];
        };
        void update(V value)
        {
            container_->data_[pos] = value;
            container_->num_reserved--;
        }
        rangeIterator& operator++()
        {
            pos++;
            if(pos >= range_end_)
            {
                pos = -1;
            }
            return *this;
        }
        ~rangeIterator() = default;

        rangeIterator(ConcurrentReservableVector<V> * container, uint64_t start, uint64_t end)
        {
            container_ = container;
            range_start_ = start;
            range_end_ = end;
            pos = start;
        }
    };

    ConcurrentReservableVector()
    {
        num_reserved = 0;
        max_size_ = 100000;
        size_ = 0;
        data_ = new V[max_size_];
    }

    ~ConcurrentReservableVector()
    {
        delete [] data_;
    }

    uint64_t size()
    {
        Lock l(&mtx);
        return size_;
    }

    uint64_t unsafe_size()
    {
        return size_;
    }

    rangeIterator reserve(uint64_t size = 1)
    {
        Lock l(&mtx);
        while (size_ + size >= max_size_)
        {
            increase_size();
        }
        num_reserved += size;
        uint64_t range_start = size_;
        uint64_t range_end = size_ + size;
        size_ += size;
        return rangeIterator(this, range_start, range_end);
    }

    rangeIterator end()
    {
        return rangeIterator(this, -1, -1);
    }

    rangeIterator begin()
    {
        return rangeIterator(this, 0, size_);
    }

    uint64_t numReserved()
    {
        return num_reserved;
    }

    bool empty()
    {
        Lock l(&mtx);
        return size_ == 0;
    }
};

#endif //_DB_CONCURRENT_RESERVABLE_VECTOR_H_
