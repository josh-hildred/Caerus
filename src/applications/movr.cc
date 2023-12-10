//
// Author: Joshua Hildred jt<lastname>@gmail.com>
//

#include "movr.h"
#include <iostream>

#include "backend/storage.h"
#include "backend/storage_manager.h"

#include <bitset>


// Fills '*keys' with num_keys unique ints k where
// 'key_start' <= k < 'key_limit', and k == part (mod nparts), keys's master == replica
// Requires: key_start % nparts == 0
void Movr::GetRandomKeysReplica(set<uint64>* keys, uint32 num_keys, uint64 key_start,
                                          uint64 key_limit, uint64 part, uint32 replica) {
    CHECK(key_start % (nparts*replica_size) == 0);
    keys->clear();

    for (uint32 i = 0; i < num_keys; i++) {
        // Find a key not already in '*keys'.
        uint64 key;
        uint64 order = rand() % ((key_limit - key_start)/(nparts*replica_size));
        key = key_start + part + nparts * (order * replica_size + replica);

        while (keys->count(key)) {
            order = rand() % ((key_limit - key_start)/(nparts*replica_size));
            key = key_start + part + nparts * (order * replica_size + replica);
        }

        CHECK(key <= nparts * kDBSize);

        keys->insert(key);
    }
}


TxnProto *Movr::GetRide(int64 txn_id, uint64_t part1, uint64_t part2, uint32_t replica1, uint32_t replica2) {
    TxnProto* txn = new TxnProto();

    txn->set_txn_id(txn_id);
    txn->set_txn_type(GETRIDE);

    if(replica1 != replica2) {

        // Set the transaction's standard attributes
        txn->set_num_parts(2);
        txn->add_involved_replicas(replica1);
        txn->add_involved_replicas(replica2);

    }
    else {
        txn->set_num_parts(1);
        txn->add_involved_replicas(replica1);
    }


    uint64_t key_start = 0;
    if (key_start % (replica_size*nparts) != 0) {
        key_start = key_start + (replica_size*nparts - (key_start % (replica_size*nparts)));
    }

    set<uint64> keys;
    GetRandomKeysReplica(&keys,
                         1,
                         key_start,
                         vehicle_end,
                         part1,
                         replica1);
    for (set<uint64>::iterator it = keys.begin(); it != keys.end(); ++it) {
        KeyEntry * key_entry = txn->add_read_write_set();
        key_entry->set_key(IntToString(*it));
        key_entry->set_master(replica1);
        key_entry->set_counter(0);
    }


    key_start = vehicle_end;
    if (key_start % (replica_size*nparts) != 0) {
        key_start = key_start + (replica_size*nparts - (key_start % (replica_size*nparts)));
    }



    keys.clear();
    GetRandomKeysReplica(&keys,
                         1,
                         key_start,
                         user_end,
                         part2,
                         replica2);
    for (set<uint64>::iterator it = keys.begin(); it != keys.end(); ++it) {
        KeyEntry * key_entry = txn->add_read_write_set();
        key_entry->set_key(IntToString(*it));
        key_entry->set_master(replica2);
        key_entry->set_counter(0);
    }

    key_start = user_end;
    if (key_start % (replica_size*nparts) != 0) {
        key_start = key_start + (replica_size*nparts - (key_start % (replica_size*nparts)));
    }



    keys.clear();
    GetRandomKeysReplica(&keys,
                         1,
                         key_start,
                         rides_end,
                         part1,
                         replica1);
    for (set<uint64>::iterator it = keys.begin(); it != keys.end(); ++it) {
        KeyEntry * key_entry = txn->add_read_write_set();
        key_entry->set_key(IntToString(*it));
        key_entry->set_master(replica1);
        key_entry->set_counter(0);
    }



    return txn;
}


TxnProto* Movr::NewTxn(int64 txn_id, int txn_type,
                             string args, ClusterConfig* config) const {
    return NULL;
}

int Movr::Execute(TxnProto* txn, StorageManager* storage) const {
    // Remaster txn
    //LOG(ERROR) << "Executing txn " << txn->txn_id();
    if (txn->remaster_txn() == true) {
        LOG(ERROR) <<local_replica_<< ":*********In Execute:  handle remaster txn: "<<txn->txn_id();
        KeyEntry key_entry = txn->read_write_set(0);
        Record* val = storage->ReadObject(key_entry.key());

        CHECK(key_entry.master() == val->master);
        CHECK(key_entry.counter() == val->counter);

        val->master = txn->remaster_to();
        val->counter = key_entry.counter() + 1;

        if (local_replica_ == txn->remaster_from()) {
            val->remastering = false;
        }

        return 0;
    }


    double execution_start = GetTime();

    double factor = 1.0;

    for (uint32 i = 0; i < txn->read_write_set_size(); i++) {
        KeyEntry key_entry = txn->read_write_set(i);
        Record* val = storage->ReadObject(key_entry.key());
        // Not necessary since storage already has a pointer to val.
        //   storage->PutObject(txn->read_write_set(i), val);


        for (int j = 0; j < 8; j++) {
            if ((val->value)[j] + 1 > 'z') {
                (val->value)[j] = 'a';
            } else {
                (val->value)[j] = (val->value)[j] + 1;
            }
        }
    }

    // The following code is for microbenchmark "long" transaction, uncomment it if for "long" transaction
    while (GetTime() - execution_start < 0.00012/factor) {
        int x = 1;
        for(int i = 0; i < 10000; i++) {
            x = x+10;
            x = x-2;
        }
    }

    return 0;
}

void Movr::InitializeStorage(Storage* storage, ClusterConfig* conf) const {
    char* int_buffer = (char *)malloc(sizeof(char)*kRecordSize);
    for (uint32 j = 0; j < kRecordSize - 1; j++) {
        int_buffer[j] = 'a'; //(rand() % 26 + 'a');
    }
    int_buffer[kRecordSize - 1] = '\0';

    for (uint64 i = 0; i < nparts * kDBSize; i++) {
        if (conf->LookupPartition(IntToString(i)) == conf->relative_node_id()) {
            string value(int_buffer);
            uint32 master = conf->LookupPrimary(IntToString(i));
            storage->PutObject(IntToString(i), new Record(value, master));
        }
    }
    LOG(ERROR) << "storage inited";
}


