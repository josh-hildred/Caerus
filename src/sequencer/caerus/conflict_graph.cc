//
// Author: Joshua Hildred jt<lastname>@gmail.com>
//

#include "conflict_graph.h"
#include "partial_sequencer.h"
#include <memory>




Vertex::Vertex(uint64 txn_id, uint32 num_parts, uint64_t version, ConflictGraph * graph){
    this->txn_id_ = txn_id;
    this->num_parts_ = num_parts;
    this->graph_ = graph;
    this->version_number_ = version;
    this->scc_id = 0;
    this->in_scc = false;
    this->replica_flag_bits = 0;
    this->graph_->num_vertex_structs_++;
}


inline void Vertex::addOutEdge_(uint64_t vertex_id, uint64_t id_version)
{
    auto it = out_edges_.find(vertex_id);
    if(it == out_edges_.end())
    {
        out_edges_.insert(make_pair(vertex_id, id_version));
        neighbour_ptrs_.push_back(make_pair(vertex_id, id_version));
    }
}

inline void Vertex::addInEdge_(uint64_t vertex_id, uint64_t id_version){

    auto it = in_edges_.find(vertex_id);
    if(it == in_edges_.end())
    {
        in_edges_.insert(make_pair(vertex_id, id_version));
        in_neighbour_ptrs_.push_back(make_pair(vertex_id, id_version));
    }
}


int Vertex::removeOutEdge_(uint64_t vertex_id) {
    auto it = out_edges_.find(vertex_id);
    if(it != out_edges_.end()) {
        out_edges_.erase(vertex_id);
        if(out_edges_.find(vertex_id) != out_edges_.end())
        {
            LOG(ERROR) << "ERROR failed to delete out edge " << txn_id_ << " --> " << vertex_id;
        }
        return 0;
    }
    LOG(ERROR) << "ERROR: out edge " << txn_id_ << " --> " << vertex_id << " does not exist";
    return -1;
}


void Vertex::addTxnPart_(TxnProto * txn_part) {
    this->txn_parts_.push_back(txn_part);
}

uint64_t Vertex::getNumberParts_()
{
    return this->txn_parts_.size();
}

bool Vertex::hasAllParts_() {
    return getNumberParts_() == num_parts_;
}

uint64_t Vertex::getTxnId_() {
    return this->txn_id_;
}


bool Vertex::inSCC_() {
    return in_scc;
}


uint64_t Vertex::getSCCid_() {
    return scc_id;
}


bool Vertex::tryRemove(uint64_t to_remove) {
    if(to_remove == txn_id_)
    {
        LOG(ERROR) << "ERROR: try remove on vertex " << txn_id_ << "tried to remove a trivial edge that should not exist";
    }
    if(to_remove != 0) {
        // if this txn is waiting in readers
            auto edge = out_edges_.find(to_remove);
            if (edge == out_edges_.end()) {
                LOG(ERROR) << "ERROR: edge " << txn_id_ << " -- > " << to_remove << " does not exist";
            }

            CHECK(edge != out_edges_.end());
            removeOutEdge_(to_remove);
    }
    if(out_edges_.empty() && hasAllParts_() && reader_waits_.empty())
    {
        return true;
    }
    else
    {
        return false;
    }
}

Vertex::~Vertex() {
    for(auto txn : txn_parts_){
        delete txn;
    }
    graph_->num_vertex_structs_--;
}

void Vertex::clear_() {
    scc_ptr_.reset();
    graph_->incomplete_vertex_.erase(txn_id_);
}

uint64_t Vertex::getVersion() {
    return version_number_;
}


int ConflictGraph::strong_connect_(const std::shared_ptr<Vertex>& vertex, uint64_t * index, stack<uint64_t> * s, tsl::robin_map<uint64_t,
                           tuple<uint64_t, uint64_t, bool>> * index_tracker,
                                   int* num_scc)
{
    int search_size = 1;
    uint64_t vertex_id = vertex->txn_id_;
    uint64_t v_index = *index;
    uint64_t v_lowlink = *index;
    bool v_onstack = true;
    index_tracker->insert(make_pair(vertex_id, make_tuple(v_index, v_lowlink, v_onstack)));
    s->push(vertex_id);
    *index = *index + 1;


    ReadLock lVertex(&vertex->mutex_);

    ConcurrentLinkedList<std::pair<uint64_t, uint64_t>>::iterator neighbour_iter = vertex->neighbour_ptrs_.begin();
    lVertex.unlock();

    for(; neighbour_iter != vertex->neighbour_ptrs_.end(); ++neighbour_iter)
    {
        lVertex.lock();

        // if we already went explored this with the read conflicts
        auto neighbour_struct_iter = incomplete_vertex_.find(neighbour_iter->first);
        lVertex.unlock();

        if(neighbour_struct_iter == nullptr)
        {
            continue;
        }

        std::shared_ptr<Vertex> neighbour = *neighbour_struct_iter;

        if(neighbour == nullptr)
        {
            continue;
        }

        if(neighbour->getVersion() != neighbour_iter->second)
        {
            continue;
        }
        {
            ReadLock l(&neighbour->mutex_);
            if(neighbour->in_batch_)
            {
                continue;
            }
        }

        auto neighbour_id = neighbour->txn_id_;

        auto it2 = index_tracker->find(neighbour_id);
        if(it2 == index_tracker->end())
        {
            search_size += strong_connect_(neighbour, index, s, index_tracker, num_scc);
            v_lowlink = std::min(std::get<1>(index_tracker->find(neighbour_id)->second), v_lowlink);
            index_tracker->insert_or_assign(vertex_id, make_tuple(v_index, v_lowlink, v_onstack));
        }
        else if(std::get<2>(it2->second))
        {
            v_lowlink = std::min(std::get<0>(index_tracker->find(neighbour_id)->second), v_lowlink);
            index_tracker->insert_or_assign(vertex_id, make_tuple(v_index, v_lowlink, v_onstack));
        }
    }

    if(v_lowlink == v_index)
    {
        auto * scc_vertex_ids = new unordered_set<uint64_t>();
        uint64_t w;
        do {
            w = s->top();
            s->pop();

            scc_vertex_ids->insert(w);
            auto it = index_tracker->find(w);
            index_tracker->insert_or_assign(w, make_tuple(std::get<0>(it->second), std::get<1>(it->second), false));
        } while (w != vertex_id);

        // do not push a scc if it is of size 1, or we already know about it.

        uint64_t old_scc_size = 1;
        uint64_t old_scc_id = 0;
        bool scc_deleted = false;
        std::shared_ptr<SCC> old_scc_ptr;
        lVertex.lock();
        if(vertex->inSCC_())
        {
            if(vertex->in_batch_)
            {
                scc_deleted = true;
            }
            else {
                old_scc_id = vertex->scc_id;
                old_scc_ptr = vertex->scc_ptr_;
                lVertex.unlock();
                ReadLock lSCC(&old_scc_ptr->mutex_);
                old_scc_size = old_scc_ptr->vertex_ids_->size();
            }
        }

        if(old_scc_size != 1 && old_scc_size == scc_vertex_ids->size())
        {

#ifdef EXPENSIVE_CORRECTNESS_CHECK
            if(!scc_deleted) {
                ReadLock lSCC(&old_scc_ptr->mutex_);
                if (!old_scc_ptr->removed) {
                    for (auto id: *scc_vertex_ids) {
                        auto vertex_iter = incomplete_vertex_.find(id);
                        CHECK(vertex_iter != nullptr);
                        auto vertex_ptr = *vertex_iter;
                        CHECK(vertex_ptr != nullptr);
                        ReadLock lV2(&vertex_ptr->mutex_);
                        CHECK(!vertex_ptr->in_batch_);
                        CHECK(vertex->scc_id == old_scc_id);
                    }
                }
            }
#endif
            auto task = new TaskProto();
            task->set_type(TaskProto::TryRun);
            //queue a vertex in the scc, doesn't matter which one
            task->set_id(*scc_vertex_ids->begin());
            task->set_other_id(0);
            task_queue_->putback(task);


            delete scc_vertex_ids;

        }
        else if(scc_vertex_ids->size() > 1) {
            uint64_t scc_id = scc_next_id_++;
            auto scc = std::make_shared<SCC>(scc_id, scc_vertex_ids, this);

            *num_scc = *num_scc +1;

            auto *task = new TaskProto();
            task->set_type(TaskProto::ReorderSCC);
            auto * sharedSCCPtrPtr = new std::shared_ptr<SCC>(scc);
            task->set_vertex_ptr((int64)sharedSCCPtrPtr);
            task->set_id(scc_id);
            task_queue_->putback(task);
        }
        else
        {
            delete scc_vertex_ids;
        }
    }
    return search_size;
}



int ConflictGraph::findSCCs() {
    std::list<uint64_t> * search_set;
    {
        Lock l(&vertex_search_set_mutex_);
        search_set = vertex_search_set_;
        vertex_search_set_ = new std::list<uint64_t>();
    }


    stack<uint64_t> s;
    uint64_t index = 0;
    tsl::robin_map<uint64_t, tuple<uint64_t, uint64_t, bool>> index_tracker;
    int num_scc = 0;
    for (unsigned long & it : *search_set) {
        if (index_tracker.find(it) != index_tracker.end()) {
            continue;
        }

        std::shared_ptr<std::shared_ptr<Vertex>> v_iter = incomplete_vertex_.find(it);


        if (v_iter == nullptr) {
            //vertex has been deleted
            continue;
        }
        std::shared_ptr<Vertex> vertex = *v_iter;
        if (vertex == nullptr) {
            continue;
        }

        ReadLock lVertex(&vertex->mutex_);

        if (vertex->in_batch_) {
            //vertex has been marked as in batch
            continue;
        }

        lVertex.unlock();
        strong_connect_(vertex, &index, &s, &index_tracker, &num_scc);

    }

    return num_scc;
}

ConflictGraph::ConflictGraph(SAQ<TaskProto *> *task_queue, uint32_t num_vertex) : incomplete_vertex_(MAX_INTERNAL_ID * num_vertex), insert_locks_(MAX_INTERNAL_ID * num_vertex), id_version_(MAX_INTERNAL_ID * num_vertex), index_(10000000){
    this->task_queue_ = task_queue;
    this->vertex_search_set_ = new std::list<uint64_t>();
    this->size_ = 0;
    this->total_txns_submitted_ = 0;
    this->total_txns_committed_ = 0;
    this->num_vertex_structs_ = 0;
    this->num_vertex_ = num_vertex;
    for (int i = MAX_INTERNAL_ID; i <= MAX_INTERNAL_ID * (num_vertex + 1); i++) {
        id_version_.insert(i, 0);
        insert_locks_.insert(i, new Mutex());
    }
}

uint64_t ConflictGraph::size() {
    return size_;
}

bool ConflictGraph::tryRun(const std::shared_ptr<Vertex>& vertex, uint64_t vertex_to_remove, bool reader_conflict, /*vector*/ ConcurrentReservableVector<std::pair<uint64_t, TxnProto *>> **batch,
                           MutexRW *batch_lock) {
    //check and make sure we don't have a trivial loop
    if(vertex == nullptr)
    {
        return false;
    }
    WriteLock lVertex(&vertex->mutex_);
    if(vertex->in_batch_)
    {
        //LOG(ERROR) << "ERROR: transaction " << vertex->txn_id_ << " is already in batch";
        return false;
    }
    if(vertex->inSCC_())
    {
        auto scc_id = vertex->getSCCid_();
        auto scc = vertex->scc_ptr_;

        CHECK(scc_id == scc->id_);

        lVertex.unlock();

        WriteLock lSCC(&scc->mutex_);

        if(scc->removed)
        {
            WriteLock ln(&vertex->mutex_);
            CHECK(vertex->in_batch_ or (!vertex->in_batch_ and vertex->scc_id != scc_id));
            return false;
        }
        if(scc->tryRemove(vertex, vertex_to_remove))
        {
            auto det_ordering = scc->deterministicVertexOrder();

            ReadLock lBatch(batch_lock);
            auto reserved_range = (*batch)->reserve(det_ordering->size());
            lBatch.unlock();
            for(const auto& scc_vertex_id : *det_ordering){

                auto scc_vertex_iter = incomplete_vertex_.find(scc_vertex_id);

                CHECK(scc_vertex_iter != nullptr);

                auto scc_vertex = *scc_vertex_iter;

                WriteLock lSCCVertex(&scc_vertex->mutex_);

                CHECK(scc_vertex->in_batch_ == false);

                scc_vertex->in_batch_ = true;
                //handle write --> write conflicts
                for (const auto& neighbour_id : scc_vertex->in_neighbour_ptrs_)
                {
                    if(scc->vertex_ids_->find(neighbour_id.first) != scc->vertex_ids_->end())
                    {
                        continue;
                    }
                    auto neighbour_iter = incomplete_vertex_.find(neighbour_id.first);
                    if(neighbour_iter == nullptr)
                    {
                        CHECK(false) << " ERROR vertex should not be null";
                        continue;
                    }
                    std::shared_ptr<Vertex> neighbour_vertex = *neighbour_iter;
                    if(neighbour_id.second != neighbour_vertex->version_number_)
                    {
                        CHECK(false) << " ERROR version should be the same ";
                        continue;
                    }

                    if(scc->vertex_ids_->find(neighbour_vertex->txn_id_) == scc->vertex_ids_->end())
                    {
                        CHECK(neighbour_vertex->in_batch_ == false);
                        auto *task = new TaskProto();
                        task->set_type(TaskProto::TryRun);
                        task->set_id(neighbour_vertex->txn_id_);
                        task->set_other_id(scc_vertex->txn_id_);
                        task_queue_->putback(task);
                    }
                }
                double now = GetTime();
                auto t1 = ((now - vertex->first_part_insert_time) * 1000);
                auto t2 = ((now - vertex->last_part_insert_time) * 1000);

                scc_vertex->clear_();

                auto txn = new TxnProto(*scc_vertex->txn_parts_.front());

                txn->add_times(t1);
                txn->add_times(t2);

                reserved_range.update(make_pair(scc_vertex->getID(), txn));
                ++reserved_range;
                size_--;
            }
            delete det_ordering;

            LOG(ERROR) << "SCC sent of size: " << scc->vertex_ids_->size();

            scc->removed = true;
            return true;
        }
        lVertex.lock();
        if(vertex->scc_id != scc->id_)
        {
            LOG(ERROR) << "INFO: Vertex " << vertex->txn_id_ << " now has different scc id " << vertex->scc_id << " than " << scc->id_;
        }
        lVertex.unlock();
        return false;
    }
    else
    {
        if(vertex->tryRemove(vertex_to_remove))
        {
            ReadLock lBatch(batch_lock);
            auto reserved_range = (*batch)->reserve();
            lBatch.unlock();
            vertex->in_batch_ = true;

            for (const auto& neighbour_id : vertex->in_neighbour_ptrs_) {

                auto neighbour_iter = incomplete_vertex_.find(neighbour_id.first);
                if(neighbour_iter == nullptr)
                {
                    CHECK(false) << " ERROR vertex should not be null";
                    continue;
                }
                std::shared_ptr<Vertex> neighbour = *neighbour_iter;
                if(neighbour_id.second != neighbour->version_number_)
                {
                    CHECK(false) << " ERROR vertex should not have different version number";
                    continue;
                }
                CHECK(neighbour->in_batch_ == false);
                auto * task = new TaskProto();
                task->set_type(TaskProto::TryRun);
                task->set_id(neighbour_id.first);
                task->set_other_id(vertex->txn_id_);
                task_queue_->putback(task);
            }

            double now = GetTime();
            auto t1 = ((now - vertex->first_part_insert_time) * 1000);
            auto t2 = ((now - vertex->last_part_insert_time) * 1000);


            vertex->clear_();

            auto txn = new TxnProto(*vertex->txn_parts_.front());

            txn->add_times(t1);
            txn->add_times(t2);

            reserved_range.update(make_pair(vertex->getID(), txn));
            ++reserved_range;

            size_--;
            lVertex.unlock();
            return true;
        }
        return false;
    }
}

void ConflictGraph::contractSCC(std::shared_ptr<SCC> scc){
    WriteLock lSCC(&scc->mutex_);
    if(!scc->contractVertices(scc))
    {
        if(!scc->safeToRemove_())
        {
            LOG(ERROR) << "ERROR: Tried to remove SCC " << scc->id_ << " but it is not safe to remove";
        }
        auto task = new TaskProto();
        task->set_type(TaskProto::TryRun);

        //queue a vertex doesn't matter which one
        task->set_id(*scc->vertex_ids_->begin());
        task->set_other_id(0);
        task_queue_->putback(task);

        scc->removed = true;
    }

}

SCC::SCC(uint64_t id, unordered_set<uint64_t> *vertex_ids, ConflictGraph * graph) {
    graph_ = graph;
    id_ = id;
    vertex_ids_ = vertex_ids;
    removed = false;
}




size_t SCC::outEdgeSize_() {
    size_t size = 0;
    for(const auto& vertex_id : *vertex_ids_)
    {
        auto vertex = *graph_->incomplete_vertex_.find(vertex_id);
        CHECK(vertex != nullptr);
        WriteLock lVertex(&vertex->mutex_);
        for (auto edge : vertex->out_edges_){
            if(vertex_ids_->find(edge.first) == vertex_ids_->end())
            {
                size++;
            }
        }
    }
    return size;
}

bool SCC::hasAllParts_() {
    bool flag = true;
    for(auto vertex_id : *vertex_ids_)
    {
        auto vertex = *graph_->incomplete_vertex_.find(vertex_id);
        CHECK(vertex != nullptr);
        ReadLock l(&vertex->mutex_);
        if(!vertex->hasAllParts_())
        {
            flag = false;
            break;
        }
    }
    return flag;
}


bool SCC::contractVertices(std::shared_ptr<SCC> this_) {
    unordered_set<uint64_t> subSCC;
    unordered_map<uint64_t, std::shared_ptr<SCC>> subSCCptr;

    for(auto vertex_id : *vertex_ids_)
    {
        auto vertex_iter = graph_->incomplete_vertex_.find(vertex_id);

        if(vertex_iter == nullptr)
        {
            continue;
        }

        auto v = *vertex_iter;

        if(v == nullptr){
            continue;
        }
        WriteLock lVertex(&v->mutex_);
        if(v->in_batch_)
        {
            continue;

        }
        WriteLock lSubSCC;

        if(v->inSCC_())
        {
            uint64_t sub_scc_id = v->scc_id;
            auto sub_scc = v->scc_ptr_;
            lVertex.unlock();
            lSubSCC.init(&sub_scc->mutex_);
            lVertex.lock();
            subSCC.insert(sub_scc_id);
            subSCCptr.insert(make_pair(sub_scc_id, sub_scc));
        }
        v->in_scc = true;
        v->scc_id = id_;
        v->scc_ptr_ = this_;
    }


    for(const uint64_t scc_id : subSCC )
    {
        CHECK(id_ > scc_id);
        auto scc = subSCCptr.find(scc_id)->second;
        if(scc == nullptr)
        {
            LOG(ERROR) << "ERROR: this shouldn't happen based on worker being a single thread";
            CHECK(subSCC.size() == 1);
            continue;
        }
        WriteLock lOldSCC(&scc->mutex_);
        if(scc->removed)
        {
            LOG(ERROR) << "ERROR: this shouldn't happen based on worker being a single thread";
            CHECK(subSCC.size() == 1);
            continue;
        }
        if(!scc->safeToRemove_())
        {
            LOG(ERROR) << "ERROR: Sub SCC " << scc_id << "is not safe to remove... This cause inconsistencies" ;
            CHECK(false);
        }
        scc->removed = true;
        lOldSCC.unlock();
    }


    if(safeToRemove_())
    {
        return false;
    }

    if(hasAllParts_() && (outEdgeSize_() == 0)) {
        auto task = new TaskProto();
        task->set_type(TaskProto::TryRun);
        task->set_id(*vertex_ids_->begin());
        task->set_other_id(0);
        graph_->task_queue_->putback(task);
    }

    return true;
}



vector<uint64_t>*  SCC::deterministicVertexOrder() {
    auto sorted_vertex = new vector<uint64_t>();
    for(const auto & v : *vertex_ids_)
    {
        sorted_vertex->push_back(v);
    }
    std::sort(sorted_vertex->begin(), sorted_vertex->end());
    return sorted_vertex;
}

SCC::~SCC() {
    delete vertex_ids_;
}

bool SCC::tryRemove(const std::shared_ptr<Vertex>& vertex, uint64_t edge_to_remove) {
    CHECK(vertex->getID() != edge_to_remove);
    if(vertex->getID() != 0) {
        if(vertex == nullptr)
        {
            LOG(ERROR) << "ERROR: vertex in scc is null but we hold write lock on SCC... This segfaults";
        }
        WriteLock lVertex(&vertex->mutex_);
        if(vertex->scc_id != id_)
        {
            LOG(ERROR) << "INFO: vertex " << vertex->txn_id_ << " now has different scc id " << vertex->scc_id << " than " << id_;
        }
        if(vertex->in_batch_)
        {
            LOG(ERROR) << "INFO: vertex has already been sent";
        }
        if (edge_to_remove != 0) {
                auto edge = vertex->out_edges_.find(edge_to_remove);
                CHECK(edge != vertex->out_edges_.end());
                vertex->removeOutEdge_(edge_to_remove);
        }
        lVertex.unlock();
    }
    if((outEdgeSize_() == 0) && hasAllParts_())
    {
        return true;
    }
    else
    {
        return false;
    }
}

bool SCC::safeToRemove_() {
    for (const auto& vertex_id : *vertex_ids_) {
        auto vertex_iter = graph_->incomplete_vertex_.find(vertex_id);
        if(vertex_iter != nullptr) {
            auto vertex = *vertex_iter;
            if (vertex != nullptr) {
                ReadLock lVertex(&vertex->mutex_);
                if (!vertex->in_batch_) {
                    if (vertex->scc_id == id_) {
                        return false;
                    }
                }
            }
        }
    }
    return true;
}




ConflictGraph::PartitionedInserter::PartitionedInserter(ConflictGraph *graph_, uint32_t partition) : index_(INDEX_SIZE){
    this->partition_ = partition;
    this->graph_ = graph_;
}

int ConflictGraph::PartitionedInserter::insert(TxnProto *txn) {
    uint64_t id = txn->internal_id();


    WriteLock lVertex;

    bool in_scc = false;
    uint64_t scc_id;
    WriteLock lSCC;
    std::shared_ptr<SCC> scc;

    auto t2 = GetTime();

    Lock lIncomplete(*graph_->insert_locks_.find(id));
    auto vertex_iter = graph_->incomplete_vertex_.find(id);
    bool exists = vertex_iter != nullptr;

    std::shared_ptr<Vertex> vertex;


    if(!exists)
    {
        uint64_t next_version;
        auto version_iter = graph_->id_version_.find(id);
        if(version_iter == nullptr)
        {
            next_version = 1;
            graph_->id_version_.insert(id, next_version);
        }
        else
        {
            next_version = *version_iter;
            next_version++;
            graph_->id_version_.erase(id);
            graph_->id_version_.insert(id, next_version);
        }
        auto vertex_ptr = std::make_shared<Vertex>(id, txn->num_parts(), next_version, graph_);
        lVertex.init(&vertex_ptr->mutex_);

        vertex_ptr->replica_flag_bits[txn->replica() * NUM_PARTIAL_SEQUENCERS + txn->partial_sequencer_id()] = true;

        vertex_ptr->first_part_insert_time = GetTime();

        graph_->incomplete_vertex_.insert(make_pair(id, vertex_ptr));

        vertex = vertex_ptr;
        graph_->size_++;
    } else
    {
        vertex = *vertex_iter;
        lVertex.init(&vertex->mutex_);

        //blow up the system if any of these conditions are true
        CHECK(vertex != nullptr);
        CHECK(vertex->in_batch_ == false);
        CHECK(vertex->hasAllParts_() == false);
        CHECK(vertex->replica_flag_bits[txn->replica() * NUM_PARTIAL_SEQUENCERS + txn->partial_sequencer_id()] == false);

        if(vertex->inSCC_()) {
            in_scc = true;

            do {
                lSCC.clear();
                scc_id = vertex->scc_id;
                scc = vertex->scc_ptr_;
                CHECK(scc != nullptr);

                lVertex.unlock();

                lSCC.init(&scc->mutex_);
                lVertex.lock();
            } while (vertex->scc_id != scc_id);

            CHECK(vertex->scc_id == scc_id);
            //make sure vertex was not deleted while we got lock
            CHECK(!vertex->in_batch_);
        }
    }

    if(vertex->num_parts_ == (vertex->txn_parts_.size() + 1))
    {
        vertex->last_part_insert_time = GetTime();
    }


  // handle write set
    auto conflict_set = txn->txn_part_write_set();
    for (const auto &entry: conflict_set) {
        CHECK(entry.master() == partition_);
        unsigned long long key = std::stoul(entry.key());
        ConflictInfo conf_info;

        bool ret = getAndUpdateWrite_(key, id, vertex->getVersion(), conf_info);

        if(!ret)
        {
            continue;
        }

        if(conf_info.readers_->empty())
        {
            if (conf_info.writer_id_ == id) {
                continue;
            }

            auto conf_vertex_iter = graph_->incomplete_vertex_.find(conf_info.writer_id_);

            if (conf_vertex_iter != nullptr) {

                auto conf_vertex = *conf_vertex_iter;

                if (conf_vertex == nullptr) {
                    continue;
                }
                //todo there probably is a faster way to do this
                WriteLock lConf(&conf_vertex->mutex_, false);
                if (id > conf_vertex->txn_id_) {
                    lVertex.unlock();
                    lConf.lock();
                    lVertex.lock();
                } else {
                    lConf.lock();
                }

                if (conf_vertex->getVersion() != conf_info.writer_version_ or conf_vertex->in_batch_) {
                    continue;
                }


                vertex->addOutEdge_(conf_vertex->txn_id_, conf_vertex->getVersion());
                conf_vertex->addInEdge_(id, vertex->getVersion());
            }
        }
        else {
            for (auto reader: *conf_info.readers_) {
                if (reader.first == id) {
                    continue;
                }
                auto conf_vertex_iter = graph_->incomplete_vertex_.find(reader.first);

                if (conf_vertex_iter != nullptr) {
                    auto conf_vertex = *conf_vertex_iter;

                    if (conf_vertex == nullptr) {
                        continue;
                    }

                    //todo there probably is a faster way to do this
                    WriteLock lConf(&conf_vertex->mutex_, false);
                    if (id > conf_vertex->txn_id_) {
                        lVertex.unlock();
                        lConf.lock();
                        lVertex.lock();
                    } else {
                        lConf.lock();
                    }

                    if (conf_vertex->in_batch_) {
                        continue;
                    }

                    if (conf_vertex->getVersion() != reader.second) {
                        continue;
                    }


                    vertex->addOutEdge_(conf_vertex->txn_id_, conf_vertex->getVersion());
                    conf_vertex->addInEdge_(id, vertex->getVersion());
                }
            }
        }
    }

    //handle reads
    conflict_set = txn->txn_part_read_set();
    for (const auto &entry: conflict_set) {
        unsigned long long key = std::stoul(entry.key());
        ConflictInfo conf_vertex_info;

        bool ret = getAndUpdateRead_(key, id, vertex->getVersion(), conf_vertex_info);

        if(!ret)
        {
            continue;
        }

        if (conf_vertex_info.writer_id_ == id) {
            continue;
        }

        auto conf_vertex_iter = graph_->incomplete_vertex_.find(conf_vertex_info.writer_id_);

        if (conf_vertex_iter != nullptr) {
            auto conf_vertex = *conf_vertex_iter;

            if (conf_vertex == nullptr) {
                continue;
            }
            //todo there probably is a faster way to do this
            WriteLock lConf(&conf_vertex->mutex_, false);
            if (id > conf_vertex->txn_id_) {
                lVertex.unlock();
                lConf.lock();
                lVertex.lock();
            } else {
                lConf.lock();
            }

            if (conf_vertex->getVersion() != conf_vertex_info.writer_version_ or conf_vertex->in_batch_) {
                continue;
            }

            vertex->addOutEdge_(conf_vertex->txn_id_, conf_vertex->getVersion());
            conf_vertex->addInEdge_(id, vertex->getVersion());
        }
    }

    vertex->addTxnPart_(txn);


    if(in_scc)
    {
        lVertex.unlock();
        if (scc->hasAllParts_() && scc->outEdgeSize_() == 0) {
            auto *task = new TaskProto;
            task->set_type(TaskProto::TryRun);
            task->set_id(vertex->getTxnId_());
            task->set_other_id(0);
            graph_->task_queue_->putback(task);
        }
    }
    else
    {
        if (vertex->out_edges_.empty() && vertex->hasAllParts_()) {
            auto *task = new TaskProto;
            task->set_type(TaskProto::TryRun);
            task->set_id(vertex->getTxnId_());
            task->set_other_id(0);
            graph_->task_queue_->putback(task);
        }
    }
    {
        Lock l(&graph_->vertex_search_set_mutex_);
        graph_->vertex_search_set_->emplace_back(id);
    }
    if(vertex->hasAllParts_())
    {
        {
            graph_->total_txns_submitted_++;
        }
    }

    return 0;
}

inline bool ConflictGraph::PartitionedInserter::getAndUpdateWrite_(unsigned long long int key, uint64_t vertex_id, uint64_t version, ConflictGraph::ConflictInfo & conf_info)
{
    auto it = index_.find(key);
    if(it == nullptr)
    {
        index_.insert(key, ConflictGraph::ConflictInfo(vertex_id, version));
        return false;
    }
    conf_info = *it;
    index_.update(key, ConflictGraph::ConflictInfo(vertex_id, version));
    return true;
}

inline bool ConflictGraph::PartitionedInserter::getAndUpdateRead_(unsigned long long int key, uint64_t vertex_id, uint64_t version, ConflictGraph::ConflictInfo & conf_info)
{
    auto it = index_.find(key);
    if(it == nullptr)
    {
        index_.insert(key, ConflictGraph::ConflictInfo(vertex_id, version));
        return false;
    }
    it->readers_->push_back(make_pair(vertex_id, version));
    conf_info = *it;
    return true;
}