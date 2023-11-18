// Author: Kun Ren <renkun.nwpu@gmail.com>
//


#ifndef _DB_MACHINE_CLIENT_H_
#define _DB_MACHINE_CLIENT_H_

#include <set>
#include <string>
#include <queue>
#include <iostream>
#include <map>
#include <utility>
#include <list>

#include "machine/cluster_config.h"
#include "common/utils.h"
#include "proto/txn.pb.h"
#include "applications/microbenchmark.h"
#include "applications/tpcc.h"
#include "applications/movr.h"


using std::set;
using std::string;
using std::queue;
using std::map;

#define SAMPLES  600
#define WARMUP_SAMPLES 100
#define SAMPLE_RATE 99

#define LATENCY_TEST

#ifdef LATENCY_TEST
extern map<uint64, double> sequencer_recv;
extern map<uint64, double> scheduler_unlock;
extern map<uint64, double> scheduler_recv;
extern map<uint64_t, bool> mr_txn;
extern vector<double> measured_latency;
extern std::atomic<uint64> latency_counter;
//extern vector<double> measured_latency_start_sch;
//extern vector<double> measured_latency_sch_done;
extern vector<int> mr_flag;
#endif


class ClusterConfig;
class TxnProto;

// Client
class Client {
 public:
  virtual ~Client() {}
  virtual void GetTxn(TxnProto** txn, int txn_id) = 0;
};

// Microbenchmark load generation client.
class MClient : public Client {
 public:
  MClient(ClusterConfig* config, uint32 mp, uint32 hot_records)
      : microbenchmark(config, hot_records), config_(config), percent_mp_(mp),
        nodes_per_replica_(config->nodes_per_replica()), replative_node_id_(config->relative_node_id()) {
  }
  virtual ~MClient() {}
  virtual void GetTxn(TxnProto** txn, int txn_id) {
    if (nodes_per_replica_ > 1 && (uint32)(rand() % 100) < percent_mp_) {
      // Multipartition txn.
      uint64 other;
      do {
        other = (uint64)(rand() % nodes_per_replica_);
      } while (other == replative_node_id_);
      *txn = microbenchmark.MicroTxnMP(txn_id, replative_node_id_, other);
    } else {
      // Single-partition txn.
      *txn = microbenchmark.MicroTxnSP(txn_id, replative_node_id_);
    }
  }

 private:
  Microbenchmark microbenchmark;
  ClusterConfig* config_;
  uint32 percent_mp_;
  uint64 nodes_per_replica_;
  uint64 replative_node_id_;
};

// TPCC load generation client.
class TClient : public Client {
 public:
  TClient(ClusterConfig* config, uint32 mp, uint32 hot_records)
      : tpcc(config, hot_records), config_(config), percent_mp_(mp),
        nodes_per_replica_(config->nodes_per_replica()), replative_node_id_(config->relative_node_id()) {
  }
  virtual ~TClient() {}
  virtual void GetTxn(TxnProto** txn, int txn_id) {
	  // Right now only test 10% multi-warehouse txn (percent_mp_ is used to how much multi-warehouse txn)
    if (nodes_per_replica_ > 1 && (uint32)(rand() % 100) < percent_mp_) {
      // Multipartition txn.
      uint64 other;
      do {
        other = (uint64)(rand() % nodes_per_replica_);
      } while (other == replative_node_id_);
      *txn = tpcc.TpccTxnMP(txn_id, replative_node_id_, other);
    } else {
      // Single-partition txn.
      *txn = tpcc.TpccTxnSP(txn_id, replative_node_id_);
    }
  }

 private:
  Tpcc tpcc;
  ClusterConfig* config_;
  uint32 percent_mp_;
  uint64 nodes_per_replica_;
  uint64 replative_node_id_;
};

// Microbenchmark load generation client.
class Lowlatency_MClient : public Client {
 public:
  Lowlatency_MClient(ClusterConfig* config, uint32 mp, uint32 mr, uint32 hot_records)
      : microbenchmark(config, hot_records), config_(config), percent_mp_(mp), percent_mr_(mr),
        nodes_per_replica_(config->nodes_per_replica()), replative_node_id_(config->relative_node_id()) {
    local_replica_ = config_->local_replica_id();
    num_replicas_ = config_->replicas_size();
  }
  virtual ~Lowlatency_MClient() {}
  virtual void GetTxn(TxnProto** txn, int txn_id) {
    if ((uint32)(rand() % 100) < percent_mr_) {
      // Multi-replica txn.
      uint32 other_replica;
      do {
        other_replica = (uint32)(rand() % num_replicas_);
      } while (other_replica == local_replica_); 
      if (nodes_per_replica_ > 1 && uint32(rand() % 100) < percent_mp_) {
        // Multi-replica multi-partition txn
        uint64 other_node;
        do {
          other_node = (uint64)(rand() % nodes_per_replica_);
        } while (other_node == replative_node_id_);

        *txn = microbenchmark.MicroTxnMRMP(txn_id, replative_node_id_, other_node, local_replica_, other_replica);
      } else {
        // Multi-replica single-partition txn
        *txn = microbenchmark.MicroTxnMRSP(txn_id, replative_node_id_, local_replica_, other_replica);    
      }
    } else {
      // Single-replica txn.
      if (nodes_per_replica_ > 1 && (uint32)(rand() % 100) < percent_mp_) {
        // Single-replica multi-partition txn
        uint64 other_node;
        do {
          other_node = (uint64)(rand() % nodes_per_replica_);
        } while (other_node == replative_node_id_);

        *txn = microbenchmark.MicroTxnSRMP(txn_id, replative_node_id_, other_node, local_replica_);
      } else {
        // Single-replica single-partition txn
        *txn = microbenchmark.MicroTxnSRSP(txn_id, replative_node_id_, local_replica_);         
      }
    }
  }

 private:
  Microbenchmark microbenchmark;
  ClusterConfig* config_;
  uint32 percent_mp_;
  uint32 percent_mr_;
  uint32 local_replica_;
  uint32 num_replicas_;
  uint64 nodes_per_replica_;
  uint64 replative_node_id_;
};

// Tpcc load generation client for slog.
class Lowlatency_TClient : public Client {
 public:
  Lowlatency_TClient(ClusterConfig* config, uint32 mp, uint32 mr, uint32 hot_records)
      : tpcc(config, hot_records), config_(config), percent_mp_(mp), percent_mr_(mr),
        nodes_per_replica_(config->nodes_per_replica()), replative_node_id_(config->relative_node_id()) {
    local_replica_ = config_->local_replica_id();
    num_replicas_ = config_->replicas_size();
  }
  virtual ~Lowlatency_TClient() {}
  virtual void GetTxn(TxnProto** txn, int txn_id) {
	  // Currently use 10% multi-warehouse txn (percent_mp_ is used to how many multi-warehouse txn)
	if ((uint32)(rand() % 100) < percent_mp_) {
        if ((uint32)(rand() % 100) < percent_mr_) {
            // Multi-replica txn.
            uint32 other_replica;
            do {
                other_replica = (uint32)(rand() % num_replicas_);
            } while (other_replica == local_replica_);
            // Multi-replica multi-partition txn
            uint64 other_node;
            do {
                other_node = (uint64)(rand() % nodes_per_replica_);
            } while (other_node == replative_node_id_);

            *txn = tpcc.TpccTxnMRMP(txn_id, replative_node_id_, other_node, local_replica_, other_replica);
        } else {
            // Single-replica txn.
            // Single-replica multi-partition txn
            uint64 other_node;
            do {
               other_node = (uint64)(rand() % nodes_per_replica_);
            } while (other_node == replative_node_id_);

            *txn = tpcc.TpccTxnSRMP(txn_id, replative_node_id_, other_node, local_replica_);
	    }
    } else {
        // Single-replica single-partition txn
        *txn = tpcc.TpccTxnSRSP(txn_id, replative_node_id_, local_replica_);
    }
  }

 private:
  Tpcc tpcc;
  ClusterConfig* config_;
  uint32 percent_mp_;
  uint32 percent_mr_;
  uint32 local_replica_;
  uint32 num_replicas_;
  uint64 nodes_per_replica_;
  uint64 replative_node_id_;
};




class MovrClient : public Client
{
public:
    MovrClient(ClusterConfig* config, uint32 mp, uint32 mr, uint32 hot_records)
            : benchmark(config, hot_records), config_(config), percent_mp_(mp), percent_mr_(mr),
              nodes_per_replica_(config->nodes_per_replica()), replative_node_id_(config->relative_node_id()) {
        local_replica_ = config_->local_replica_id();
        num_replicas_ = config_->replicas_size();
        locality_size_ = config_->locality_size();
        locality_base = local_replica_ / locality_size_;
        LOG(ERROR) << "Movr Client: pTravel = [ " <<  percent_mr_/3 << ", " << 2 * (percent_mr_/3) << ", " << 100 - percent_mr_ << " ]";
    }
    virtual ~MovrClient() {}

    virtual void GetTxn(TxnProto** txn, int txn_id) {
        uint32_t type = rand() % 100;
        uint64_t other_part;
        if(rand() % 100 < percent_mp_)
        {
            do {
                other_part = rand() % nodes_per_replica_;
            } while (other_part != replative_node_id_);
        }
        else
        {
            other_part = replative_node_id_;
        }
        {
            uint64_t pRegion = (rand() % 100);
            uint64_t replica;
            if( pRegion < percent_mr_/3 && locality_size_ != num_replicas_)
            {
                replica = GetReplicaOutsideContinent();
            }
            else if ( pRegion < percent_mr_)
            {
                replica = GetReplicaInContinent();
            }
            else
            {
                replica = local_replica_;
            }
            *txn = benchmark.GetRide(txn_id, replative_node_id_, other_part, local_replica_, replica);
        }
    }

private:
    Movr benchmark;
    ClusterConfig* config_;
    uint32 percent_mp_;
    uint32 percent_mr_;
    uint32 local_replica_;
    uint32 num_replicas_;
    uint32 locality_size_;
    uint64 locality_base;
    uint64 nodes_per_replica_;
    uint64 replative_node_id_;
    vector<uint64_t> pTravel;

    uint64_t GetReplicaInContinent()
    {
        uint64 replica;
        do {

            uint64 selection = rand() % locality_size_;
            replica = selection + locality_base;
        } while (replica == local_replica_);
        return replica;
    }

    uint64_t GetReplicaOutsideContinent()
    {
        uint64 selection;
        do {

            selection = rand() % (num_replicas_ / locality_size_);
        } while (locality_base == selection);

        return selection + rand() % locality_size_;
    }
};


#endif  // _DB_MACHINE_CLIENT_H_
