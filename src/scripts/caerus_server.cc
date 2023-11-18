//
// Author: Joshua Hildred jt<lastname>@gmail.com>
//

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "machine/cluster_config.h"
#include "sequencer/caerus/merger.h"
#include "sequencer/caerus/partial_sequencer.h"
#include "backend/simple_storage.h"
#include "machine/connection.h"
#include "applications/microbenchmark.h"
#include "applications/tpcc.h"
#include "applications/movr.h"
#include "scheduler/deterministic_scheduler.h"
#include "scripts/script_utils.h"
#include "common/perf_tracker.h"
#include <vector>

DEFINE_bool(calvin_version, false, "Print Calvin version information");
DEFINE_string(binary, "caerus_server", "Caerus binary executable program");
DEFINE_string(config, "cluster.conf", "conf file of Caerus cluster");
DEFINE_int32(machine_id, 0, "machine id");
DEFINE_int32(mode, 0, "0: Origin CalvinDB; 1: Low latency CalvinDB; 2; Low latency CalvinDB with access pattern remaster");
DEFINE_int32(type, 0, "[CalvinDB: 0: 3 replicas; 1: 6 replicas]; [Low latency: 0: 3 replicas normal; 1: 6 replicas normal; 2: 6 replicas strong availbility ] ");
DEFINE_string(experiment, "TPC-C", "the experiment that you want to run, default is TPC-C");
DEFINE_int32(locality_size, 1, "the size of locality groups for stats collections");
DEFINE_int32(percent_mp, 0, "percent of distributed txns");
DEFINE_int32(percent_mr, 0, "percent of multi-replica txns");
DEFINE_int32(hot_records, 10000, "number of hot records");
DEFINE_int32(max_batch_size, 100, "max batch size of txns per epoch");


int main(int argc, char** argv) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);

    // Print Calvin version
    if (FLAGS_calvin_version) {
        // Check whether Calvin have been running
        if (is_process_exist((char *)FLAGS_binary.c_str()) == true) {
            return -2;
        } else {
            printf("Machine %d: (Geo-replicated CalvinDB) 0.1 (c) UMD 2017.\n",
                   (int)FLAGS_machine_id);
            return 0;
        }
    }

    LOG(ERROR) <<FLAGS_machine_id<<":Preparing to start Calvin node ";



    // Build this node's configuration object.
    auto* config = new ClusterConfig(FLAGS_machine_id, FLAGS_locality_size);
    config->FromFile(FLAGS_config);

    LOG(ERROR)<<FLAGS_machine_id <<":Created config ";


    vector<pair<string, uint64_t>> perf_conf = {{"Inserter throughput", config->replicas_size()},
                                                {"Inserter backlog", config->replicas_size()},
                                                {"Inserter time", config->replicas_size()},
                                                {"Sequencer throughput", 1}};

    PerfTracker perf(FLAGS_machine_id, perf_conf);

    // Build connection context and start multiplexer thread running.
    ConnectionMultiplexer* multiplexer = new ConnectionMultiplexer(config);


    InternalIdTracker ids(config->local_replica_id(), config->all_nodes_size());


    Spin(1);

    LOG(ERROR) << FLAGS_machine_id <<":Created connection ";

    Client* client = NULL;
    // Artificial loadgen clients. Right now only microbenchmark
    if (FLAGS_experiment == "TPC-C") {
        client = reinterpret_cast<Client*>(new Lowlatency_TClient(config, FLAGS_percent_mp, FLAGS_percent_mr, FLAGS_hot_records));
    } else {
        client = reinterpret_cast<Client*>(new MovrClient(config, FLAGS_percent_mp, FLAGS_percent_mr, FLAGS_hot_records));
    }


    Storage* storage;
    storage = new SimpleStorage();

    LOG(ERROR) << FLAGS_machine_id<< ":Created storage ";

    Application* application = NULL;

    if (FLAGS_experiment == "TPC-C") {
        // Other benchmark
        application = new Tpcc(config, multiplexer, FLAGS_hot_records);
        application->InitializeStorage(storage, config);
    } else {
        application = new Movr(config, multiplexer, FLAGS_hot_records);
        application->InitializeStorage(storage, config);
    }

    LOG(ERROR) << FLAGS_machine_id << ":Created application ";


    Merger *log = NULL;
    //only run merger if we are the head machine for a replica
    if(config->local_node_id() % config->nodes_per_replica() == 0) {
        if (FLAGS_machine_id % config->nodes_per_replica() < 3) {
            log = new Merger(config, multiplexer, &ids, &perf);
        }
        // Initialize sequencer component and start sequencer thread running.
        LOG(ERROR) << FLAGS_machine_id << ":Created log ";
    }

    // Run scheduler in main thread.
    DeterministicScheduler scheduler(config,
                                     storage,
                                     application,
                                     multiplexer,
                                     FLAGS_mode, &ids);

    LOG(ERROR) << FLAGS_machine_id << ":Created scheduler ";

    PartialSequencer * mergingSequencer;

    mergingSequencer = new PartialSequencer(config, multiplexer, client, FLAGS_max_batch_size, &ids);
    LOG(ERROR) << FLAGS_machine_id << ":Created sequencer ";

    // Run scheduler in main thread.

    while (!config->Stopped()) {
        usleep(1000000);
    }

    printf("Machine %d : Calvin server exit!\n", (int)FLAGS_machine_id);
    usleep(1000*1000*5);
    delete log;
    delete mergingSequencer;
    usleep(1000*1000*5);
    perf.dumpToCSV(string(STATS_DIR) + "perf." + std::to_string(config->local_replica_id()) + ".csv");
    quick_exit (EXIT_SUCCESS);
}