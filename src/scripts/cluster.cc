// Author: Kun Ren <renkun.nwpu@gmail.com>
//

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "machine/cluster_manager.h"

DEFINE_string(command, "status", "cluster command");
DEFINE_string(config, "cluster.conf", "conf file of the cluster");
DEFINE_string(path, "./", "path to the main directory");
DEFINE_string(binary, "calvindb_server", "Calvin binary executable program");
DEFINE_string(lowlatency_binary, "lowlatency_calvindb_server", "Lowlatency Calvin binary executable program");
DEFINE_string(merging_binary, "caerus_server", "Caerus binary");
DEFINE_string(username, "user", "username for ssh");
DEFINE_int32(lowlatency, 3, "0: Original CalvinDB ; 1: low latency version of CalvinDB; 2: low latency with access pattern remasters; 3: merging calvin;");
DEFINE_int32(type, 0, "[CalvinDB: 0: 3 replicas; 1: 6 replicas]; [Low latency: 0: 3 replicas normal; 1: 6 replicas normal; 2: 6 replicas strong availbility ] ");
DEFINE_string(experiment, "TPC-C", "the experiment that you want to run, default is TPC-C");
DEFINE_int32(percent_mp, 0, "percent of distributed txns");
DEFINE_int32(percent_mr, 0, "percent of multi-replica txns");
DEFINE_int32(hot_records, 10000, "number of hot records--to control contention");
DEFINE_int32(max_batch_size, 100, "max batch size of txns per epoch");
DEFINE_string(binary_arguments, "", "args to be passed to the binary");



int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);


  ClusterManager* cm;
  if (FLAGS_lowlatency == 0) {
      cm = new ClusterManager(FLAGS_config, FLAGS_path, FLAGS_binary, FLAGS_lowlatency, FLAGS_type, FLAGS_username,  FLAGS_binary_arguments);
  } else if(FLAGS_lowlatency == 3){
      cm = new ClusterManager(FLAGS_config, FLAGS_path, FLAGS_merging_binary, 0, FLAGS_type, FLAGS_username, FLAGS_binary_arguments);
  }
  else {
      cm = new ClusterManager(FLAGS_config, FLAGS_path, FLAGS_lowlatency_binary, FLAGS_lowlatency, FLAGS_type, FLAGS_username, FLAGS_binary_arguments);
  }

  if (FLAGS_command == "update") {
    cm->Update();

  } else if (FLAGS_command == "put-config") {
    cm->PutConfig();

  } else if (FLAGS_command == "get-data") {
    cm->GetTempFiles();

  } else if (FLAGS_command == "start") {
    cm->DeployCluster(FLAGS_experiment, FLAGS_percent_mp, FLAGS_percent_mr, FLAGS_hot_records, FLAGS_max_batch_size);

  } else if (FLAGS_command == "kill") {
    cm->KillCluster();

  } else if (FLAGS_command == "status") {
    cm->ClusterStatus();

  } else if (FLAGS_command == "push-code") {
      cm->PushCode();
  } else if (FLAGS_command == "make") {
      cm->Make();
  } else if (FLAGS_command == "install-ext") {
      cm->Setup();
  } else if (FLAGS_command == "cleanup") {
      cm->Cleanup();
  } else {
    LOG(FATAL) << "unknown command: " << FLAGS_command;
  }
  return 0;
}

