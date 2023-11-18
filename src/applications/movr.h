//
// Author: Joshua Hildred jt<lastname>@gmail.com>
//

#ifndef _DB_APPLICATION_MOVR_H_
#define _DB_APPLICATION_MOVR_H_

#include <set>
#include <string>

#include "machine/connection.h"
#include "applications/application.h"
#include "machine/cluster_config.h"
#include "common/utils.h"
#include "proto/txn.pb.h"

using std::set;
using std::string;

#define SIZERIDETABLE 10000000
#define NUMUSERS 1000000

class Movr : public Application {
public:
    enum TxnType {
        INITIALIZE = 0,
        GETRIDE = 1,
    };

    Movr(ClusterConfig* conf, uint32 hotcount) {
        nparts = conf->nodes_per_replica();
        num_vehicle = hotcount;
        vehicle_end = num_vehicle * nparts;
        user_end = vehicle_end + (nparts * (NUMUSERS - num_vehicle));
        rides_end = user_end + (nparts * SIZERIDETABLE);
        kDBSize = SIZERIDETABLE + NUMUSERS;
        replica_size = conf->replicas_size();
        config_ = conf;
        local_replica_ = config_->local_replica_id();
    }

    Movr(ClusterConfig* conf, ConnectionMultiplexer* multiplexer, uint32 hotcount) : Movr(conf, hotcount) {
        connection_ = multiplexer;
    }

    virtual ~Movr() {}

    virtual TxnProto* NewTxn(int64 txn_id, int txn_type, string args,
                             ClusterConfig* config = NULL) const;

    virtual int Execute(TxnProto* txn, StorageManager* storage) const;

    TxnProto* GetRide(int64 txn_id, uint64_t part1, uint64_t part2, uint32_t replica1, uint32_t replica2);

    uint32 nparts;
    // controls contention

    uint32 num_vehicle;
    uint64_t vehicle_end;
    uint64_t user_end;
    uint64_t rides_end;
    uint64_t replica_size;

    ClusterConfig* config_;
    uint32 local_replica_;
    ConnectionMultiplexer* connection_;

    //static const uint32 kRWSetSize = 10;  // MUST BE EVEN
    uint64 kDBSize;
    static const uint32 kRecordSize = 100;


    virtual void InitializeStorage(Storage* storage, ClusterConfig* conf) const;

private:
    void GetRandomKeysReplica(set<uint64>* keys, uint32 num_keys, uint64 key_start,
                              uint64 key_limit, uint64 part, uint32 replica);

    Movr() {}
};


#endif //_DB_APPLICATION_MOVR_H_
