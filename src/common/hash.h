//
// Author: Joshua Hildred jt<lastname>@gmail.com>
//

#ifndef _DB_HASH_H
#define _DB_HASH_H

#include "utils.h"

// This is a simple hash that maps node ids from the range [size_, x], [0, x - size_]
template<uint64_t size_>
struct VertexIDHash
{
public:
    size_t operator()(uint64_t const& key) const noexcept{
        return key - size_;
    }
};

template<uint64_t size_>
class IndexHash
{
    uint64_t hot_range = 250000;
public:
    uint64_t operator()(uint64_t & key) {
        uint64_t hashed_key = key;
        if (hashed_key <= hot_range) {
            return hashed_key;
        } else {
            return (hashed_key % (size_ - hot_range) + hot_range);
        }
    }
};


struct hash_2_
{
    std::size_t operator()(uint64_t const& s) const noexcept {
        uint64_t key = s;
        key = (~key) + (key << 21);
        key = key ^ (key >> 24);
        key = key + (key << 3) + (key << 8);
        key = key ^ (key >> 14);
        key = key + (key << 2) + (key << 4);
        key = key ^ (key >> 28);
        key = key + (key << 31);
        return key;
    }
};

#endif
