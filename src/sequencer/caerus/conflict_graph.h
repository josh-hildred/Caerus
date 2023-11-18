//
// Author: Joshua Hildred jt<lastname>@gmail.com>
//

#ifndef _DB_CONFLICT_GRAPH_H_
#define _DB_CONFLICT_GRAPH_H_


#define INDEX_SIZE 5000000


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


#include "common/types.h"
#include "proto/scalar.pb.h"
#include "proto/txn.pb.h"
#include "proto/task.pb.h"
#include "common/mutex.h"
#include "common/utils.h"
#include "common/tsl/robin_map.h"
#include "common/simple_atomic_queue.h"
#include "common/concurrent_reservable_vector.h"
#include "machine/cluster_config.h"
#include "common/concurrent_linked_list.h"
#include "common/fast_fixed_size_map.h"
#include "common/hash.h"

using std::unordered_set;
using std::vector;
using std::make_pair;
using std::queue;
using std::stack;
using std::tuple;
using std::make_tuple;
using std::atomic_flag;

class Vertex;
class ConflictGraph;
class SCC;


class ConflictGraph {
public:

    struct ConflictInfo
    {
        uint64_t writer_id_;
        uint64_t writer_version_;
        uint64_t reader_group_version_;
        std::shared_ptr<std::list<std::pair<uint64_t, uint64_t>>> readers_;
        ConflictInfo(uint64_t id, uint64_t version_)
        {
            writer_id_ = id;
            writer_version_ = version_;
            reader_group_version_ = 0;
            readers_ = std::make_shared<std::list<std::pair<uint64_t, uint64_t>>>();;
        }
        ConflictInfo(const ConflictInfo& c)
        {
            writer_id_ = c.writer_id_;
            writer_version_ = c.writer_version_;
            readers_ = c.readers_;
            reader_group_version_ = c.reader_group_version_;
        }
        ConflictInfo(){
            writer_id_ = 0;
            writer_version_ = 0;
            reader_group_version_ = 0;
            readers_ = nullptr;
        }
    };


    class PartitionedInserter
    {
        ConflictGraph * graph_;
        fast_fixed_size_map<ConflictGraph::ConflictInfo, IndexHash<INDEX_SIZE>> index_;
    public:
        PartitionedInserter(ConflictGraph * graph_, uint32_t partition);
        int insert(TxnProto * txn);
        uint32_t partition_;
        inline bool getAndUpdateWrite_(unsigned long long int key, uint64_t vertex_id, uint64_t version, ConflictGraph::ConflictInfo & conf_info);
        inline bool getAndUpdateRead_(unsigned long long int key, uint64_t vertex_id, uint64_t version, ConflictInfo &conf_info);
    };


    int findSCCs();
    explicit ConflictGraph(SAQ<TaskProto *> * task_queue, uint32_t num_vertex);
    uint64_t size();
    PartitionedInserter * getInserter(uint32_t partition)
    {
        return new PartitionedInserter(this, partition);
    }
    bool tryRun(const std::shared_ptr<Vertex>& vertex, uint64_t vertex_to_remove, bool reader_conflict, ConcurrentReservableVector<std::pair<uint64_t, TxnProto *>> **batch, MutexRW *batch_lock);
    void contractSCC(std::shared_ptr<SCC> scc);

    std::atomic_int_fast64_t total_txns_submitted_;
    std::atomic_int_fast64_t total_txns_committed_;

    std::atomic_int_fast64_t num_vertex_structs_;

    // in this case the vertex insert locks are immutable so does not need to be concurrent
    fast_fixed_size_map<Mutex *, VertexIDHash<MAX_INTERNAL_ID>> insert_locks_;

    concurrent_fast_fixed_size_map<std::shared_ptr<Vertex>, VertexIDHash<MAX_INTERNAL_ID>> incomplete_vertex_;
    concurrent_fast_fixed_size_map<uint64_t, VertexIDHash<MAX_INTERNAL_ID>> id_version_;


protected:
    friend class Vertex;
    friend class SCC;

    Mutex vertex_search_set_mutex_;
    std::list<uint64_t> *  vertex_search_set_;

    uint32_t num_vertex_;

    std::atomic_int_fast64_t size_{};

    MutexRW mutex_;

    tsl::robin_map<uint64_t , std::pair<uint64_t, uint64_t>, hash_2_> index_;


    uint64_t scc_next_id_ = 1;

    SAQ<TaskProto*> * task_queue_;


    int strong_connect_(const std::shared_ptr<Vertex> &vertex, uint64_t *index,
                        stack<uint64_t> *s,
                        tsl::robin_map<uint64_t, tuple<uint64_t, uint64_t, bool>> *index_tracker, int *num_scc);
};

class Vertex{
public:
    Vertex(uint64 txnid, uint32 mun_parts, uint64_t version_number, ConflictGraph * graph);
    ~Vertex();
    bool tryRemove(uint64_t to_remove);
    uint64_t getID() const
    {
        return txn_id_;
    }


private:
    friend class ConflictGraph;
    friend class SCC;


    void addOutEdge_(uint64_t vertex_id, uint64_t id_version);
    void addInEdge_(uint64_t vertex_id, uint64_t id_version);


    int removeOutEdge_(uint64_t vertex_id);
    void addTxnPart_(TxnProto * txn_part);
    uint64_t getNumberParts_();
    uint64_t getTxnId_();
    bool hasAllParts_();
    bool inSCC_();
    uint64_t getSCCid_();
    void clear_();
    uint64_t getVersion();

    ConflictGraph * graph_;
    MutexRW mutex_;
    uint64 txn_id_;
    uint32 num_parts_;
    vector<TxnProto *> txn_parts_;
    uint64_t version_number_;


    ConcurrentLinkedList<std::pair<uint64_t, uint64_t>> neighbour_ptrs_;
    ConcurrentLinkedList<std::pair<uint64_t, uint64_t>> in_neighbour_ptrs_;

    tsl::robin_map<uint64_t, uint64_t> in_edges_;
    tsl::robin_map<uint64_t, uint64_t> out_edges_;

    tsl::robin_map<uint64_t, uint64_t> reader_waits_;


    bool in_scc = false;
    uint64_t scc_id = -1;
    std::shared_ptr<SCC> scc_ptr_;

    bool in_batch_ = false;

    std::bitset<64> replica_flag_bits;

    //------ performance tracking ---------//
    double first_part_insert_time;
    double last_part_insert_time;
};



class SCC {
    size_t outEdgeSize_();
    bool hasAllParts_();
    bool safeToRemove_();

    unordered_set<uint64_t> subSCCs;

    ConflictGraph * graph_;
    MutexRW mutex_;
    unordered_set<uint64> * vertex_ids_;
    uint64_t id_;

    bool removed = false;

public:
    friend class ConflictGraph;
    friend class Vertex;
    SCC(uint64_t id, unordered_set<uint64_t> * vertex_ids, ConflictGraph * graph);
    ~SCC();
    bool tryRemove(const std::shared_ptr<Vertex>& vertex, uint64_t edge_to_remove);
    vector<uint64_t> * deterministicVertexOrder();
    bool contractVertices(std::shared_ptr<SCC>);
};



#endif
