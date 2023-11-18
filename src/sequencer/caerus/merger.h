//
// Author: Joshua Hildred jt<lastname>@gmail.com>
//


#ifndef _DB_MERGER_H_
#define _DB_MERGER_H_

//#define MAX_INTERNAL_ID 100000

#define NUM_WORKERS NUM_SEQUENCER_WORKERS

#include <atomic>
#include <glog/logging.h>
#include <queue>
#include <set>
#include <utility>
#include <vector>
#include <tr1/unordered_map>

#include "proto/scalar.pb.h"
#include "common/mutex.h"
#include "common/types.h"
#include "log/local_mem_log.h"
#include "proto/scalar.pb.h"
#include "machine/connection.h"
#include "proto/txn.pb.h"
#include "proto/task.pb.h"
#include "conflict_graph.h"
#include "common/concurrent_reservable_vector.h"
#include "common/perf_tracker.h"
#include "partial_sequencer.h"
#include "common/internal_ids.h"


using std::vector;
using std::atomic;
using std::make_pair;
using std::pair;
using std::queue;
using std::set;
using std::tr1::unordered_map;

class Merger {
public:
    Merger(ClusterConfig* config, ConnectionMultiplexer* connection, InternalIdTracker * ids, PerfTracker * perf);
    ~Merger();

    void Stop();
    void sendBatch();

private:


    static void * RunBatchSenderThread(void* arg);

    static void* RunMergerThread(void *arg);


    static void* RunLogWorkerThread(void *arg);


    void RunMerger();

    void RunWorker();

    void RunBatchSender();

    class PartitionedInserter
    {
        string queue_name_;
        Merger * merger_;
        PerfTracker * perf_;
        queue<MessageProto *> message_queue_;
        ConflictGraph::PartitionedInserter * inserter;
        void ReceiveMessage();
        void Run();

    public:
        PartitionedInserter(Merger *log, uint32_t partition_);

        ~PartitionedInserter();

        uint64_t partition_;

        static void* RunThread(void *arg);

        pthread_t thread;
    };


    PerfTracker * perf_;

    std::unordered_map<uint64_t, MessageProto *> batch_messages_;
    std::unordered_map<uint64_t, MessageProto *> sequence_messages_;

    vector<Log*> partial_logs_;

    bool running_;

    ClusterConfig* configuration_;
    uint64 this_machine_id_;

    ConnectionMultiplexer* connection_;

    queue<MessageProto *> message_queue_;

    MutexRW current_batch_mutex_;
    ConcurrentReservableVector<std::pair<uint64_t, TxnProto *>> * current_batch_;


    uint64_t next_batch_id_ = 1;


    // Separate pthread contexts in which to run the leader or follower thread.
    pthread_t merger_thread_;
    pthread_t merger_worker_thread_;
    vector<pthread_t> worker_threads_;
    pthread_t batch_sender_thread_;

    vector<PartitionedInserter *> inserters;

    ConflictGraph * graph_;
    SAQ<TaskProto *> graph_task_queue_;
};



#endif
