//
// Author: Joshua Hildred jt<lastname>@gmail.com>
//

#include "partial_sequencer.h"
#include <list>
#include <unordered_set>

#define PRINT_STATS
#define STAT_INTERVAL 1





PartialSequencer::PartialSequencer(ClusterConfig *conf, ConnectionMultiplexer *connection, Client *client,
                                   uint32 max_batch_size, InternalIdTracker * ids)
        : epoch_duration_(EPOCH_DURATION), /*log_(log),*/ configuration_(conf),
          connection_(connection), ids_(ids), client_(client), deconstructor_invoked_(false), max_batch_size_(max_batch_size){
    // Start Sequencer main loops running in background thread.

    for (int i = 0; i < NUM_PARTIAL_SEQUENCERS; ++i) {
        connection_->NewChannel("sequencer_" + std::to_string(i));
    }

    start_working_ = false;

    cpu_set_t cpuset;

    for (int i = 0; i < NUM_PARTIAL_SEQUENCERS; ++i) {
        CPU_ZERO(&cpuset);
        CPU_SET(3, &cpuset);
        pthread_attr_t attr_reader;
        pthread_attr_init(&attr_reader);
        pthread_attr_setaffinity_np(&attr_reader, sizeof(cpu_set_t), &cpuset);

        void **args = new void *[2];
        args[0] = reinterpret_cast<void*>(this);
        args[1] = (void *) i;
        pthread_create(&reader_thread_, &attr_reader, RunSequencerReader, args);
    }

    pthread_create(&writer_thread_, nullptr, RunSequencerWriter, reinterpret_cast<void*>(this));

}

void* PartialSequencer::RunSequencerWriter(void *arg) {
    reinterpret_cast<PartialSequencer*>(arg)->RunWriter();
    return NULL;
}

void* PartialSequencer::RunSequencerReader(void *arg) {
    reinterpret_cast<PartialSequencer*>(reinterpret_cast<void **>(arg)[0])->RunReader((uint64)reinterpret_cast<void **>(arg)[1]);
    return NULL;
}

PartialSequencer::~PartialSequencer() {
    deconstructor_invoked_ = true;
    pthread_join(writer_thread_, NULL);
    pthread_join(reader_thread_, NULL);
}

void PartialSequencer::RunWriter() {

    uint64 batch_number;
    uint32 txn_id_offset;

    auto nodes_per_replica = configuration_->nodes_per_replica();

    vector<MessageProto> batch_messages(configuration_->replicas_size() * NUM_PARTIAL_SEQUENCERS);

    //send the batch to the head node of each replica and each partial sequencer thread at the replica
    for(int replica = 0; replica < configuration_->replicas_size(); replica++) {
        uint64_t machine_id = configuration_->LookupMachineID(0, replica);
        uint64_t replica_offset = replica * NUM_PARTIAL_SEQUENCERS;
        for (int sequencer = 0; sequencer < NUM_PARTIAL_SEQUENCERS; sequencer++) {
            batch_messages[replica_offset + sequencer].set_destination_channel("sequencer_" + std::to_string(sequencer));
            batch_messages[replica_offset + sequencer].set_type(MessageProto::TXN_BATCH);
            batch_messages[replica_offset + sequencer].set_destination_node(machine_id);
        }
    }


    vector<bool> flags(configuration_->replicas_size() * NUM_PARTIAL_SEQUENCERS);

    uint32 local_replica = configuration_->local_replica_id();
    uint64_t machine_id = configuration_->local_node_id();

#ifdef LATENCY_TEST
    latency_counter = 0;
uint64 local_machine = configuration_->local_node_id();
#endif


    connection_->NewChannel("synchronization_sequencer_channel");
    MessageProto synchronization_message;
    synchronization_message.set_type(MessageProto::EMPTY);
    synchronization_message.set_destination_channel("synchronization_sequencer_channel");
    //for (uint64 i = 0; i < (uint64)(configuration_->all_nodes_size()); i++) {
    //TODO change back
    for (uint64 i = 0; i < (uint64)(configuration_->all_nodes_size()); i++) {
        synchronization_message.set_destination_node(i);
        if (i != static_cast<uint64>(configuration_->local_node_id())) {
            connection_->Send(synchronization_message);
        }
    }

    uint32 synchronization_counter = 1;
    while (synchronization_counter < (uint64)(configuration_->all_nodes_size())) {
    synchronization_message.Clear();
        if (connection_->GotMessage("synchronization_sequencer_channel", &synchronization_message)) {
            CHECK(synchronization_message.type() == MessageProto::EMPTY);
            synchronization_counter++;
        }
    }



    connection_->DeleteChannel("synchronization_sequencer_channel");
    LOG(ERROR) << "In sequencer:  After synchronization. Starting sequencer writer.";



    uint32 warmup_bs = MIN(15, max_batch_size_);
    uint32 catchup_bs = 20;
    uint64_t bs = MAX(catchup_bs, max_batch_size_);

    start_working_ = true;

    std::deque<TxnProto *> waiting_txns;

    usleep(100000);

#ifdef PRINT_STATS
    uint64_t num_txns_generated = 0;
    double interval = GetTime();
#endif

    auto start = GetTime();

    while (!deconstructor_invoked_) {
        if(GetTime() - start > RUNTIME)
        {
            configuration_->Stop();
            LOG(ERROR) << "STARTING STOP PROCESS";
            break;
        }
        if(GetTime() - start > 15)
        {
            warmup_bs = max_batch_size_;
        }
        //todo change getGUID
        batch_number = configuration_->GetGUID();
        double epoch_start = GetTime();
        for (auto& batch_message : batch_messages) {
            batch_message.set_batch_number(batch_number);
            batch_message.clear_data();
        }
        uint64_t batch_size = 0;
        uint64_t actual_bs = 0;
        txn_id_offset = 0;
        for (auto flag : flags) {
            flag = false;
        }

        while (!deconstructor_invoked_ && GetTime() < epoch_start + epoch_duration_) { //slow mode

            if ((uint32) batch_size < max_batch_size_ && (uint32) batch_size < warmup_bs) {
                TxnProto *txn;
                string txn_string;


                client_->GetTxn(&txn, batch_number * max_batch_size_ + txn_id_offset);
                txn->set_origin_replica(local_replica);
                txn->set_generated_machine(machine_id);

#ifdef PRINT_STATS
                num_txns_generated++;
#endif

#ifdef LATENCY_TEST
                if (txn->txn_id() % SAMPLE_RATE == 0 && latency_counter < SAMPLES) {
                sequencer_recv[txn->txn_id()] = GetTime();
                txn->set_generated_machine(local_machine);
                }
#endif

                uint64_t num_parts = 0;




                txn_id_offset++;
                batch_size++;

                if(ids_->getNumberActiveLocalTxns() > 2000)
                {
                    waiting_txns.emplace_back(txn);
                } else {
                    waiting_txns.emplace_back(txn);
                    while (!waiting_txns.empty() && actual_bs < bs) {
                        actual_bs++;
                        txn = waiting_txns.front();
                        waiting_txns.pop_front();
                        ids_->incrNumberActiveLocalTxns();
                        uint64_t internal_id = ids_->getId();
                        CHECK(internal_id != 0);
                        txn->set_internal_id(internal_id);
                        txn->SerializeToString(&txn_string);
                        for (auto replica: txn->involved_replicas()) {
                            batch_messages[replica].add_data(txn_string);
                        }
                        delete txn;
                    }
                }
            } else {
                usleep(50);
            }
        }
        // Send this epoch's transactions to the central machine of each replica
        for (uint32 i = 0; i < configuration_->replicas_size() * NUM_PARTIAL_SEQUENCERS; i++) {
            connection_->Send(batch_messages[i]);
        }
#ifdef PRINT_STATS
        auto now = GetTime();
        if(now - interval > STAT_INTERVAL)
        {
            LOG(ERROR) << "Machine " << machine_id << " generated " << num_txns_generated / (now - interval) << " transactions per second... Waiting txns " << waiting_txns.size();
            interval = GetTime();
            num_txns_generated = 0;
        }
#endif
    }
}

void PartialSequencer::RunReader(int id) {

    map<uint64, MessageProto> batches;
    map<uint64, MessageProto> paxos_data_messages;
    map<uint64, MessageProto> paxos_commit_messages;
    MessageProto paxos_leader_message;

    uint64_t quorum = configuration_->nodes_per_replica()/2 + 1;
    uint64_t num_replicas = configuration_->nodes_per_replica();
    map<uint64_t, uint64_t> quorum_map;


    std::string incoming_channel("sequencer_" + std::to_string(id));

    LOG(ERROR) << "Listening on " << incoming_channel;


    queue<MessageProto> partial_sequence;

    for (uint64 i = 0; i < configuration_->replicas_size();i++) {
        batches[i].set_destination_channel("merging_log_partition_" + std::to_string(configuration_->local_replica_id()));
        //todo fix this for multiple partitions
        uint64 machine_id = configuration_->LookupMachineID(0, i);
        LOG(ERROR) << configuration_->local_node_id() << ": In sequencer writer:  setting up conn to :" << machine_id;
        batches[i].set_destination_node(machine_id);
        batches[i].set_type(MessageProto::TXN_SUBBATCH);
    }

    paxos_leader_message.set_destination_node(configuration_->LookupMachineID(0, configuration_->local_replica_id()));
    paxos_leader_message.set_type(MessageProto::PAXOS_DATA_ACK);
    paxos_leader_message.set_destination_channel(string("sequencer_" + std::to_string(0)));

    for (uint64 i = 1; i < configuration_->nodes_per_replica();i++) {
        paxos_data_messages[i].set_destination_channel(string("sequencer_" + std::to_string(0)));
        uint64 machine_id = configuration_->LookupMachineID(i, configuration_->local_replica_id());
        LOG(ERROR) << configuration_->local_node_id() << ": In sequencer writer:  setting up paxos conn to :" << machine_id;
        paxos_data_messages[i].set_destination_node(machine_id);
        paxos_data_messages[i].set_type(MessageProto::PAXOS_DATA);
    }

    for (uint64 i = 1; i < configuration_->nodes_per_replica();i++) {
        paxos_commit_messages[i].set_destination_channel(string("sequencer_" + std::to_string(0)));
        uint64 machine_id = configuration_->LookupMachineID(i, configuration_->local_replica_id());
        LOG(ERROR) << configuration_->local_node_id() << ": In sequencer writer:  setting up conn to :" << machine_id;
        paxos_commit_messages[i].set_destination_node(machine_id);
        paxos_commit_messages[i].set_type(MessageProto::PAXOS_COMMIT);
    }

    while (start_working_ != true) {
        usleep(100);
    }

    MessageProto message;
    uint64 batch_number = 0;

    while (!deconstructor_invoked_) {
        bool got_message = connection_->GotMessage(incoming_channel, &message);
        if (got_message == true) {

            double start = GetTime();
            if (message.type() == MessageProto::TXN_BATCH) {
                if(quorum == 1)
                {
                        for (int i = 0; i < message.data_size(); i++) {
                            TxnProto txn;
                            txn.ParseFromString(message.data(i));
                            txn.set_replica(configuration_->local_node_id());
                            txn.set_partial_sequencer_id(id);

                            // Compute readers & writers; store in txn proto.
                            set<uint64> readers;
                            set<uint64> writers;
                            set<string> local_read_set;
                            set<string> local_write_set;
                            for (uint32 j = 0; j < (uint32) (txn.read_set_size()); j++)
                            {
                                const KeyEntry &key_entry = txn.read_set(j);
                                uint64 mds = configuration_->LookupPartition(key_entry.key());
                                readers.insert(mds);
                                if (configuration_->LookupPrimary(key_entry.key()) ==
                                    configuration_->local_replica_id())
                                {
                                    local_read_set.insert(key_entry.key());
                                }
                            }
                            for (uint32 j = 0; j < (uint32) (txn.read_write_set_size()); j++)
                            {
                                const KeyEntry &key_entry = txn.read_write_set(j);
                                uint64 mds = configuration_->LookupPartition(key_entry.key());
                                writers.insert(mds);
                                readers.insert(mds);
                                if (configuration_->LookupPrimary(key_entry.key()) ==
                                    configuration_->local_replica_id())
                                {

                                    CHECK(local_read_set.find(key_entry.key()) == local_read_set.end());
                                    local_write_set.insert(key_entry.key());
                                }
                                if (configuration_->LookupPrimary(key_entry.key()) == -1) {
                                    CHECK(false) << "ERROR: Key is bad... This causes issues";
                                }
                            }

                            for (unsigned long reader: readers) {
                                txn.add_readers(reader);
                            }
                            for (unsigned long writer: writers) {
                                txn.add_writers(writer);
                            }

                            // if we don't master any data items in the txn's read or write sets don't do anything with it
                            if (local_read_set.empty() and local_write_set.empty()) {
                                continue;
                            }

                            for (const auto &it: local_read_set) {
                                auto key_entry = txn.add_txn_part_read_set();
                                key_entry->set_key(it);
                                key_entry->set_master(configuration_->local_replica_id());
                                key_entry->set_counter(0);
                            }

                            for (const auto &it: local_write_set) {
                                auto key_entry = txn.add_txn_part_write_set();
                                key_entry->set_key(it);
                                key_entry->set_master(configuration_->local_replica_id());
                                key_entry->set_counter(0);
                            }


                            string txn_data;
                            txn.SerializeToString(&txn_data);

                            for (auto &batch: batches) {
                                batch.second.add_data(txn_data);
                            }
                        }

                        for (auto &batch: batches) {
                            if(batch.second.data_size() == 0)
                            {
                                continue;
                            }
                            batch.second.set_batch_number(batch_number);
                            connection_->Send(batch.second);
                            batch.second.clear_data();
                        }
                } else {
                    CHECK(quorum_map.count(message.batch_number()) == 0);
                    quorum_map.insert(make_pair(message.batch_number(), 1));
                    partial_sequence.push(message);


                    for (auto paxos_data: paxos_data_messages) {
                        paxos_data.second.set_batch_number(message.batch_number());
                        connection_->Send(paxos_data.second);
                    }
                }
            }
            else if (message.type() == MessageProto::PAXOS_DATA_ACK) {
                uint64_t batch_id = message.batch_number();
                auto quorum_iter = quorum_map.find(batch_id);
                CHECK(quorum_iter != quorum_map.end());
                    auto num_acks = ++quorum_iter->second;
                    if (quorum_iter->second == num_replicas)
                    {
                        quorum_map.erase(quorum_iter);
                    }


                    if (num_acks == quorum) {
                        CHECK(partial_sequence.front().batch_number() == batch_id);
                        message.CopyFrom(partial_sequence.front());
                        partial_sequence.pop();
                        for (int i = 0; i < message.data_size(); i++) {
                            TxnProto txn;
                            txn.ParseFromString(message.data(i));
                            txn.set_replica(configuration_->local_node_id());
                            txn.set_partial_sequencer_id(id);

                            // Compute readers & writers; store in txn proto.
                            set<uint64> readers;
                            set<uint64> writers;
                            set<string> local_read_set;
                            set<string> local_write_set;
                            for (uint32 j = 0; j < (uint32) (txn.read_set_size()); j++) {
                                const KeyEntry &key_entry = txn.read_set(j);
                                uint64 mds = configuration_->LookupPartition(key_entry.key());
                                readers.insert(mds);
                                if (configuration_->LookupPrimary(key_entry.key()) ==
                                    configuration_->local_replica_id()) {
                                    local_read_set.insert(key_entry.key());
                                }
                            }
                            for (uint32 j = 0; j < (uint32) (txn.read_write_set_size()); j++) {
                                const KeyEntry &key_entry = txn.read_write_set(j);
                                uint64 mds = configuration_->LookupPartition(key_entry.key());
                                writers.insert(mds);
                                readers.insert(mds);
                                if (configuration_->LookupPrimary(key_entry.key()) ==
                                    configuration_->local_replica_id()) {
                                    CHECK(local_read_set.find(key_entry.key()) == local_read_set.end());
                                    local_write_set.insert(key_entry.key());
                                }
                                if (configuration_->LookupPrimary(key_entry.key()) == -1) {
                                    CHECK(false) << "ERROR: Key is bad... This causes issues";
                                }
                            }

                            for (unsigned long reader: readers) {
                                txn.add_readers(reader);
                            }
                            for (unsigned long writer: writers) {
                                txn.add_writers(writer);
                            }

                            // if we have the primary copy for any data items in the txn's read or write sets don't do anything with it
                            if (local_read_set.empty() and local_write_set.empty()) {
                                continue;
                            }

                            for (const auto &it: local_read_set) {
                                auto key_entry = txn.add_txn_part_read_set();
                                key_entry->set_key(it);
                                key_entry->set_master(configuration_->local_replica_id());
                                key_entry->set_counter(0);
                            }

                            for (const auto &it: local_write_set) {
                                auto key_entry = txn.add_txn_part_write_set();
                                key_entry->set_key(it);
                                key_entry->set_master(configuration_->local_replica_id());
                                key_entry->set_counter(0);
                            }


                            string txn_data;
                            txn.SerializeToString(&txn_data);

                            for (auto &batch: batches) {
                                batch.second.add_data(txn_data);
                            }
                        }
                        for (auto &batch: batches) {
                            if(batch.second.data_size() == 0)
                            {
                                continue;
                            }
                            batch.second.set_batch_number(batch_number);
                            connection_->Send(batch.second);
                            batch.second.clear_data();
                        }
                }
            } else if(message.type() == MessageProto::PAXOS_DATA)
            {
                paxos_leader_message.set_batch_number(message.batch_number());
                connection_->Send(paxos_leader_message);
            } else if(message.type() == MessageProto::PAXOS_COMMIT)
            {
            }
        }
    }
}


