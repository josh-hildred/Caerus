//
// Author: Joshua Hildred jt<lastname>@gmail.com>
//

#ifndef _DB_FAST_FIXED_SIZE_MAP_H_
#define _DB_FAST_FIXED_SIZE_MAP_H_

#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <queue>
#include <stack>
#include <algorithm>
#include <cmath>
#include <atomic>
#include <boost/functional/hash.hpp>


#include "types.h"
#include "mutex.h"
#include "utils.h"
#include "tsl/robin_map.h"




template<class V, class Hash>
class concurrent_fast_fixed_size_map
{
public:
    explicit concurrent_fast_fixed_size_map(size_t size)
    {
        size_ = size + 1;
        data = new std::shared_ptr<V>[size_];
        empty_flags = new bool [size_];
        bucket_mutexes_ = new Mutex[size_];
        hash = Hash{};
        for (int i = 0; i < size_; ++i) {
            empty_flags[i] = true;
        }
    }

    ~concurrent_fast_fixed_size_map()
    {
        delete [] data;
        delete [] empty_flags;
        delete [] bucket_mutexes_;
    }

    std::shared_ptr<V> find(uint64_t key)
    {
        auto hashed_key = hash(key);
        Lock l(&bucket_mutexes_[hashed_key]);
        return data[hashed_key];
    }


    bool insert(std::pair<uint64_t, V> pair)
    {
        auto hashed_key = hash(pair.first);
        Lock l(&bucket_mutexes_[hashed_key]);
        if(empty_flags[hashed_key] == false)
        {
            return false;
        }
        empty_flags[hashed_key] = false;
        data[hashed_key] = std::make_shared<V>(std::move(pair.second));
        return true;
    }

    bool insert(uint64_t key, V value)
    {
        auto hashed_key = hash(key);
        Lock l(&bucket_mutexes_[hashed_key]);
        if(empty_flags[hashed_key] == false)
        {
            return false;
        }
        empty_flags[hashed_key] = false;
        data[hashed_key] = std::make_shared<V>(std::move(value));
        return true;
    }

    bool update(uint64_t key, V value)
    {
        auto hashed_key = hash(key);
        Lock l(&bucket_mutexes_[hashed_key]);
        empty_flags[hashed_key] = false;
        data[hashed_key] = std::make_shared<V>(std::move(value));
        return true;
    }

    bool erase(uint64_t key)
    {
        auto hashed_key = hash(key);
        Lock l(&bucket_mutexes_[hashed_key]);
        empty_flags[hashed_key] = true;
        data[hashed_key].reset();
        return true;
    }

private:
    uint64_t size_;
    std::shared_ptr<V> * data;
    bool * empty_flags;
    Mutex * bucket_mutexes_;
    Hash hash;
};

template<class V, class Hash>
class fast_fixed_size_map
{
public:
    explicit fast_fixed_size_map(size_t size)
    {
        size_ = size + 1;
        data = new V[size_];
        empty_flags = new uint64_t [size_];
        hash = Hash{};
        for (int i = 0; i < size_; ++i) {
            empty_flags[i] = 0x0;
        }
    }

    ~fast_fixed_size_map()
    {
        delete [] data;
        delete [] empty_flags;
    }

    V* find(uint64_t key)
    {
        auto hashed_key = hash(key);
        return (V*) ((uint64_t)(&data[hashed_key]) & empty_flags[hashed_key]);
    }

    bool insert(std::pair<uint64_t, V> pair)
    {
        auto hashed_key = hash(pair.first);
        if(empty_flags[hashed_key] == 0xffffffffffffffff)
        {
            return false;
        }
        empty_flags[hashed_key] = 0xffffffffffffffff;
        data[hashed_key] = std::move(pair.second);
        return true;
    }

    bool insert(uint64_t key, V value)
    {
        auto hashed_key = hash(key);
        if(empty_flags[hashed_key] == 0xfffffffffffffff)
        {
            return false;
        }
        empty_flags[hashed_key] = 0xffffffffffffffff;
        data[hashed_key] = std::move(value);
        return true;
    }

    bool update(uint64_t key, V value)
    {
        auto hashed_key = hash(key);
        empty_flags[hashed_key] = 0xffffffffffffffff;
        data[hashed_key] = std::move(value);
        return true;
    }

    bool erase(uint64_t key)
    {
        auto hashed_key = hash(key);
        empty_flags[hashed_key] = 0;
        data[hashed_key] = 0;
        return true;
    }

private:
    uint64_t size_;
    V * data;
    uint64_t * empty_flags;
    Hash hash;
};


#endif
