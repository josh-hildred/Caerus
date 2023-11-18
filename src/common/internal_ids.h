//
// Author: Joshua Hildred jt<lastname>@gmail.com>
//

#ifndef _DB_IDS_H_
#define _DB_IDS_H_

#include "machine/connection.h"
#include <boost/lockfree/spsc_queue.hpp>
#include "machine/cluster_config.h"

class InternalIdTracker
{
    Mutex internal_id_mutex_;
    boost::lockfree::spsc_queue<uint64_t> internal_ids_;
    std::atomic<int_fast32_t> active_local_txn_counter;
    uint64_t size_;
public:
    explicit InternalIdTracker(uint64_t, uint64_t);
    uint64_t getId();
    void reclaimId(uint64_t id);

    int getNumberActiveLocalTxns();

    bool incrNumberActiveLocalTxns();

    bool decrNumberActiveLocalTxns();

    uint64_t size();
};



#endif
