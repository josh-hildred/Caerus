//
// Author: Joshua Hildred jt<lastname>@gmail.com>
//

#include <fstream>
#include "merger.h"
#include <list>

//#define LATENCY_TEST_MERGING
#define PRINT_STATS

//#define DEBUG_MERGING_LOG_L0
//#define DEBUG_MERGING_LOG_L1
//#define DEBUG_MERGING_LOG_L2

#define MERGING_LOG_STATS

#define STAT_INTERVAL 1.0

#define MERGING_LOG_THROUPUT

#ifdef MERGING_LOG_THROUPUT
double log_time = GetTime();
uint64_t ready_txns;
#endif

#ifdef MERGING_LOG_STATS
double time_ = GetTime();
double start_time_log = 0;
#endif




Merger::Merger(ClusterConfig* config, ConnectionMultiplexer* connection, InternalIdTracker * ids, PerfTracker * perf) : perf_(perf) {
    LOG(ERROR) << "starting Merging sequence";
    configuration_ = config;
    connection_ = connection;
    graph_ = new ConflictGraph(&graph_task_queue_, config->all_nodes_size());
    current_batch_ = new ConcurrentReservableVector<pair<uint64_t, TxnProto *>>();
    running_ = true;
    this_machine_id_ = configuration_->local_node_id();



    uint32_t local_replica = configuration_->local_replica_id();

    for (int i = 0; i < configuration_->nodes_per_replica(); ++i) {
        MessageProto * sequence_message = new MessageProto();
        MessageProto * batch_message = new MessageProto();

        sequence_message->set_type(MessageProto::PAXOS_BATCH_ORDER);
        sequence_message->set_destination_channel("scheduler_");
        sequence_message->set_destination_node(configuration_->LookupMachineID(i, local_replica));

        batch_message->set_type(MessageProto::TXN_SUBBATCH);
        batch_message->set_destination_channel("scheduler_");
        batch_message->set_destination_node(configuration_->LookupMachineID(i, local_replica));

        sequence_messages_.insert(make_pair(i, sequence_message));
        batch_messages_.insert(make_pair(i, batch_message));
    }



    cpu_set_t cpuset;

    CPU_ZERO(&cpuset);
    CPU_SET(4, &cpuset);
    pthread_attr_t attr_sender;
    pthread_attr_init(&attr_sender);
    pthread_attr_setaffinity_np(&attr_sender, sizeof(cpu_set_t), &cpuset);
    pthread_create(&batch_sender_thread_, &attr_sender, RunBatchSenderThread, reinterpret_cast<void*>(this));


    CPU_ZERO(&cpuset);
    CPU_SET(5, &cpuset);
    pthread_attr_t attr_merger;
    pthread_attr_init(&attr_merger);
    pthread_attr_setaffinity_np(&attr_merger, sizeof(cpu_set_t), &cpuset);
    pthread_create(&merger_thread_, &attr_merger, RunMergerThread, reinterpret_cast<void*>(this));

    int cpu = 5 + NUM_EXECUTORS;
    for(int j = 0; j < configuration_->replicas_size(); j++) {
        partial_logs_.push_back(new LocalMemLog());
        auto inserter = new PartitionedInserter(this, j);
        inserters.push_back(inserter);

#ifdef BIND_CPU
        cpu++;
        cpu_set_t cpuset;
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        CPU_ZERO(&cpuset);
        CPU_SET(cpu, &cpuset);
        pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);

        pthread_create(&inserter->thread, &attr, inserter->RunThread, reinterpret_cast<void *>(inserter));
#else
        pthread_create(&inserter->thread, nullptr, inserter->RunThread, reinterpret_cast<void *>(inserter));
#endif
    }



    connection_->NewChannel("merging_log_");


    for(int i = 0; i < NUM_WORKERS; i++)
    {
        pthread_t thread;
#ifdef BIND_CPU
        cpu++;
        CPU_ZERO(&cpuset);
        CPU_SET(cpu, &cpuset);
        pthread_attr_t attr_worker;
        pthread_attr_init(&attr_worker);
        pthread_attr_setaffinity_np(&attr_worker, sizeof(cpu_set_t), &cpuset);
        pthread_create(&thread, &attr_worker, RunLogWorkerThread, reinterpret_cast<void*>(this));
#else
        pthread_create(&thread, nullptr, RunLogWorkerThread, reinterpret_cast<void*>(this));
#endif
        worker_threads_.push_back(thread);
    }



#ifdef DEBUG_MERGING_LOG
    std::cout << "Info: Done init" << std::endl;
#endif
}

Merger::~Merger() {
    Stop();

    for(auto inserter : inserters) {
        delete inserter;
    }

    for (auto worker_thread : worker_threads_) {
        pthread_join(worker_thread, nullptr);
    }

    // join threads
    pthread_join(merger_thread_, nullptr);
    pthread_join(merger_worker_thread_, nullptr);

}

void Merger::Stop() {
    running_ = false;
}

void Merger::RunMerger() {
    std::cout << "Info: Starting Merger" << std::endl;

    while(running_) {
        graph_->findSCCs();
    }
}


void Merger::RunWorker() {
    LOG(ERROR) << "running worker";
    while (running_)
    {
            TaskProto *task;
            bool success = graph_task_queue_.getNext(task);
            if(!success)
            {
                usleep(50);
                continue;
            }
            if(task->type() == TaskProto::ReorderSCC)
            {
                std::shared_ptr<SCC> scc(*(std::shared_ptr<SCC> * ) task->vertex_ptr());
                delete (std::shared_ptr<SCC> * ) task->vertex_ptr();

                graph_->contractSCC(scc);


            }
            else if (task->type() == TaskProto::TryRun)
            {
                uint64_t id = task->id();

                bool reader_conflict = false;
                if(task->has_read())
                    reader_conflict = task->read();

                CHECK(!task->has_read() or task->read() == true);

                uint64_t other_id = task->other_id();
                auto vertex_iter = graph_->incomplete_vertex_.find(id);
                if(vertex_iter == nullptr)
                {
                    continue;
                }
                CHECK(*vertex_iter != nullptr);
                std::shared_ptr<Vertex> vertex(*vertex_iter);
                CHECK(vertex != nullptr);
                graph_->tryRun(vertex, other_id, reader_conflict, &current_batch_, &current_batch_mutex_);

            }
            delete task;
    }
}

void *Merger::RunMergerThread(void *arg) {
    reinterpret_cast<Merger*>(arg)->RunMerger();
    return nullptr;
}


void *Merger::RunLogWorkerThread(void *arg) {
    reinterpret_cast<Merger*>(arg)->RunWorker();
    return nullptr;
}


void *Merger::RunBatchSenderThread(void *arg) {
    reinterpret_cast<Merger*>(arg)->RunBatchSender();
    return nullptr;
}


void Merger::sendBatch() {
#ifdef MERGING_LOG_THROUPUT
    if (GetTime() > log_time + STAT_INTERVAL) {
        double total_time = GetTime() - log_time;

        LOG(ERROR) << "\n*******************************************\nMachine: "<<this_machine_id_<<" SEQUENCER THROUGHPUT -- Sent "<< (static_cast<double>(ready_txns) / total_time)
                   << " txns/sec, remaining log size " << graph_->size() <<  "\n"
                   << " \n*******************************************";


        //todo this isn't great but otherwise this runs during startup skewing stats.
        // ready_transactions should not reach 0 unless there is a crash
        if(ready_txns > 0 )
        {
            perf_->addData(STATSYMBOLS::SEQUENCER_THROUGHPUT, 0, static_cast<double>(ready_txns) / total_time);
        }

        log_time = GetTime();
        ready_txns = 0;
    }
#endif




    ConcurrentReservableVector<pair<uint64_t, TxnProto *>> * batch = nullptr;
    {
        {
            if(current_batch_->unsafe_size() == 0)
            {
                return;
            }
            WriteLock l(&current_batch_mutex_);
            if (current_batch_->empty()) {
                return;
            }
            batch = current_batch_;
            current_batch_ = new ConcurrentReservableVector<pair<uint64_t, TxnProto *>>();
        }

        uint64_t num_reserved = batch->numReserved();

        while(num_reserved > 0)
        {
            usleep(20);
            num_reserved = batch->numReserved();
        }
    }

    uint64_t batch_id = next_batch_id_;

    next_batch_id_++;

    Sequence seq;
    seq.add_batch_ids(batch_id);

    std::string seq_str;
    seq.SerializeToString(&seq_str);


    for(auto message : sequence_messages_) {
        message.second->clear_misc_int();
        message.second->clear_data();
        message.second->add_misc_int(batch_id);
        message.second->add_data(seq_str);
        connection_->Send(*message.second);
    }

    for(auto message : batch_messages_) {
        message.second->clear_batch_number();
        message.second->set_batch_number(batch_id);
    }



    for(auto txn : *batch) {
        auto txn_proto = txn.second;
        std::string txn_string;

        txn_proto->SerializeToString(&txn_string);

        for (auto it : txn_proto->readers()) {
            batch_messages_[it]->add_data(txn_string);
        }
        delete txn.second;
        graph_->total_txns_committed_++;
    }

    for (auto message : batch_messages_) {
        connection_->Send(*message.second);
        message.second->clear_data();
    }

#ifdef MERGING_LOG_THROUPUT
    ready_txns += batch->size();
#endif
    delete batch;
}

void Merger::RunBatchSender() {
    while (running_)
    {
        sendBatch();

    }
}

void Merger::PartitionedInserter::Run() {
#ifdef PRINT_STATS
    double tput = 0;
    start_time_log = GetTime();
#endif

    LOG(ERROR) << "INFO: starting Inserter for channel " << queue_name_;


    while (merger_->running_) {

        while (message_queue_.empty() && merger_->running_) {
            usleep(20);

            ReceiveMessage();
        }
        //drain message queue
        while (!message_queue_.empty()) {
#ifdef PRINT_STATS
            auto t = GetTime();
            if (time_ + STAT_INTERVAL < t) {

                perf_->addData(STATSYMBOLS::INSERTER_BACKLOG, partition_, (double) message_queue_.size());
                //perf_->pp(STATSYMBOLS::INSERTER_BACKLOG, partition_, LOG(ERROR));

                if(tput > 0 )
                {
                    perf_->addData(STATSYMBOLS::INSERTER_THROUGHPUT, partition_, static_cast<double>(tput) / STAT_INTERVAL);
                    //perf_->pp(STATSYMBOLS::INSERTER_THROUGHPUT, partition_, LOG(ERROR));;
                }

                tput = 0;
                time_ = GetTime();
            }
#endif


            MessageProto *msg = message_queue_.front();
            message_queue_.pop();
            for (int i = 0; i < msg->data_size(); i++) {
                auto *txn = new TxnProto();
                txn->ParseFromString(msg->data(i));
                    auto runtime_start = GetTime();
                    inserter->insert(txn);
                    perf_->addData(STATSYMBOLS::INSERTER_TIME, partition_, (double) GetTime() - runtime_start);



#ifdef PRINT_STATS
                    tput++;
#endif
            }
            delete msg;
        }
    }
}

void Merger::PartitionedInserter::ReceiveMessage() {

    MessageProto message;

    while(merger_->connection_->GotMessage(queue_name_, &message)) {
        if (message.type() == MessageProto::TXN_SUBBATCH) {
            auto *txn_batch_message = new MessageProto();
            txn_batch_message->CopyFrom(message);
            message_queue_.push(txn_batch_message);
        } else {
            LOG(ERROR) << "ERROR: Received unexpected message type";
        }
    }
}

Merger::PartitionedInserter::PartitionedInserter(Merger *log, uint32_t partition) {
    this->merger_ = log;
    this->perf_ = log->perf_;
    this->partition_ = partition;
    this->queue_name_ = "merging_log_partition_" + std::to_string(partition);
    this->merger_->connection_->NewChannel(queue_name_);
    this->inserter = merger_->graph_->getInserter(partition);
}

void *Merger::PartitionedInserter::RunThread(void *arg) {
    reinterpret_cast<Merger::PartitionedInserter*>(arg)->Run();
    return nullptr;
}

Merger::PartitionedInserter::~PartitionedInserter()
{
    pthread_join(thread, nullptr);
}
