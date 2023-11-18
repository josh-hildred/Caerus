//
// Author: Joshua Hildred jt<lastname>@gmail.com>
//

#include "internal_ids.h"


int InternalIdTracker::getNumberActiveLocalTxns() {
    return active_local_txn_counter;
}

bool InternalIdTracker::incrNumberActiveLocalTxns() {
    active_local_txn_counter++;
    return true;
}


bool InternalIdTracker::decrNumberActiveLocalTxns() {
    active_local_txn_counter--;
    return true;
}

uint64_t InternalIdTracker::getId() {
    if (internal_ids_.empty()) {
        return 0;
    }
    else
    {
        auto ret = internal_ids_.front();
        internal_ids_.pop();
        return ret;
    }
}

void InternalIdTracker::reclaimId(uint64_t id)
{
    internal_ids_.push(id);
}

uint64_t InternalIdTracker::size()
{
    return size_;
}

InternalIdTracker::InternalIdTracker(uint64_t offset, uint64_t max) : internal_ids_(MAX_INTERNAL_ID){
    uint64_t base = (offset + 1) * MAX_INTERNAL_ID;

    for(uint64_t i = 0; i < MAX_INTERNAL_ID; i++)
    {
        internal_ids_.push(base + i);
    }
    size_ = max * MAX_INTERNAL_ID;
}