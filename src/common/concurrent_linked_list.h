//
// Author: Joshua Hildred jt<lastname>@gmail.com>
//

#ifndef _DB_CONCURRENT_LINKED_LIST_H_
#define _DB_CONCURRENT_LINKED_LIST_H_


#include <unordered_map>
#include <unordered_set>
#include <vector>


#include "mutex.h"
#include "utils.h"

//#define PREALLOCATE 15

template<class V>
class ConcurrentLinkedList
{
    class Container
    {
    protected:
        friend class ConcurrentLinkedList;
        friend class iterator;
        bool empty_ = true;
        bool deleted_flag = false;
#ifdef PREALLOCATE
        bool pre_alloc_flag_ = false;
#endif
        V item_;
        Container * prev_ = nullptr;
        Container * next_ = nullptr;
        MutexRW mtx_;
        MutexRW * insert_mtx_;
        uint64_t num_references = 0;
        explicit Container(V value, MutexRW * ll_mtx)
        {
            WriteLock l(&mtx_);
            item_ = std::move(value);
            next_ = nullptr;
            insert_mtx_ = ll_mtx;
            empty_ = false;
        }

        explicit Container(V & value, MutexRW * ll_mtx)
        {
            WriteLock l(&mtx_);
            item_ = std::move(value);
            next_ = nullptr;
            insert_mtx_ = ll_mtx;
            empty_ = false;
        }

        explicit Container(V value, Container * prev, MutexRW * ll_mtx)
        {
            WriteLock l(&mtx_);
            empty_ = false;
            item_ = std::move(value);
            next_ = nullptr;
            prev_ = prev;
            prev_->next_ = this;
            insert_mtx_ = ll_mtx;
            num_references = 0;
        }
        explicit Container(V & value, Container * prev, MutexRW * ll_mtx)
        {
            WriteLock l(&mtx_);
            empty_ = false;
            item_ = std::move(value);
            next_ = nullptr;
            prev_ = prev;
            prev_->next_ = this;
            insert_mtx_ = ll_mtx;
            num_references = 0;
        }
        Container()
        {
        }
        ~Container()
        {

        }


        void init(V value, MutexRW * ll_mtx)
        {
            WriteLock l(&mtx_);
            empty_ = false;
            item_ = std::move(value);
            next_ = nullptr;
            insert_mtx_ = ll_mtx;
            num_references = 0;
        }

        void init(V value, Container * prev, MutexRW * ll_mtx)
        {
            WriteLock l(&mtx_);
            empty_ = false;
            item_ = std::move(value);
            next_ = nullptr;
            prev_ = prev;
            prev_->next_ = this;
            insert_mtx_ = ll_mtx;
            num_references = 0;
        }

        bool try_lock()
        {
            WriteLock l(&mtx_);
            if(deleted_flag)
            {
                return false;
            }
            num_references++;
            return true;
        }
        void unlock()
        {
            WriteLock l(&mtx_);
            num_references--;
            if(deleted_flag && num_references == 0)
            {
                empty_ = true;

            }
        }
        void erase()
        {
            WriteLock l(&mtx_);
            if (num_references > 0)
            {
                deleted_flag = true;
            }
            else {
                deleted_flag = true;
                empty_ = true;

            }
        }
        V* get()
        {
            ReadLock l(&mtx_);
            if(empty_)
            {
                return nullptr;
            }
            return &item_;
        }
        Container * next()
        {
            ReadLock l(insert_mtx_);
            return next_;
        }
        Container * prev()
        {
            return prev_;
        }
    };
    Container * start = nullptr;
    Container * last = nullptr;
    size_t size_ = 0;
    MutexRW ll_mtx_;
#ifdef PREALLOCATE
    Container pre_allocated_containers[PREALLOCATE];
    uint64_t used_ = 0;
#endif
public:
    class iterator
    {
        Container * pos;;
    public:
        bool operator==(const iterator & rhs)
        {
            return pos == rhs.pos;
        }
        bool operator!=(const iterator & rhs)
        {
            return pos != rhs.pos;
        }
        V& operator*()
        {
            return *pos->get();
        }
        V* operator->()
        {
            return pos->get();
        };
        iterator&operator++()
        {
            do {
                if(pos == nullptr)
                {
                    break;
                }
                pos->unlock();
                pos = pos->next();
                if(pos == nullptr)
                {
                    break;
                }
            } while(pos->empty_ == true || !pos->try_lock());
            return *this;
        }
        ~iterator() = default;

        explicit iterator(Container * position)
        {
            pos = position;
        }
    };

    iterator begin()
    {
        return std::move(iterator(start));
    };

    iterator end()
    {
        return iterator(nullptr);
    };
    void push_back(V value)
    {
        WriteLock l(&ll_mtx_);
        size_++;
        if(start == nullptr) {
#ifdef PREALLOCATE
            Container * ptr;
            if(used_ < PREALLOCATE)
            {
                ptr = &pre_allocated_containers[used_++];
                //ptr = new Container();
                ptr->pre_alloc_flag_ = true;
                ptr->init(value, &ll_mtx_);
            }
            else
            {
                ptr = new Container(std::move(value), &ll_mtx_);
            }
#else
            auto ptr = new Container(std::move(value), &ll_mtx_);
#endif
            start = ptr;
            last = ptr;
        } else {
#ifdef PREALLOCATE
            Container * ptr;
            if(used_ < PREALLOCATE)
            {
                ptr = &pre_allocated_containers[used_++];
                //ptr = new Container();
                ptr->pre_alloc_flag_ = true;
                ptr->init(value, last, &ll_mtx_);
            }
            else
            {
                ptr = new Container(std::move(value), last, &ll_mtx_);
            }
#else
            auto ptr = new Container(std::move(value), last, &ll_mtx_);
#endif
            //last->next_ = ptr;
            last = ptr;
        }
    }

    ~ConcurrentLinkedList()
    {
        //clear();
        WriteLock l(&ll_mtx_);
        Container * pos = start;
        while(pos != nullptr)
        {
            auto old = pos;
            pos = pos->next();
#ifdef PREALLOCATE
            if(old->pre_alloc_flag_)
            {
                continue;
            }
#endif
            delete old;

        }
#ifdef PREALLOCATE
        //delete [] pre_allocated_containers;
#endif
        start = nullptr;
        last = nullptr;
        size_ = 0;

    }
};

#endif