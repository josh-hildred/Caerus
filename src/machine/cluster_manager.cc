// Author: Kun Ren <renkun.nwpu@gmail.com>
//

#include <stdio.h>
#include <unistd.h>
#include <netdb.h>
#include <sys/socket.h>

#include <glog/logging.h>
#include <string>
#include <map>
#include <utility>
#include <vector>

#include "machine/cluster_manager.h"
#include "common/utils.h"

using std::string;


void* SystemFunction(void* arg) {
  // Run the specified command.
  int status = system(reinterpret_cast<string*>(arg)->c_str());
  if(status == -1){
    LOG(FATAL)<<"system error";
  } else if(WIFEXITED(status) && (WEXITSTATUS(status) == 0)){
    // printf("run command successful\n");
  } else {
    LOG(FATAL) << "run command fail and exit code is " << WEXITSTATUS(status);
  }

  delete reinterpret_cast<string*>(arg);
  return NULL;
}

void ClusterManager::PutConfig() {
  // Copy config file to all machines.
  vector<pthread_t> threads;
  for (map<uint64, MachineInfo>::const_iterator it =
          config_.machines().begin();
       it != config_.machines().end(); ++it) {
    threads.resize(threads.size()+1);
      string* ssh_command = new string(
           "scp " + config_file_ +
           " "+ ssh_username_ + "@" + it->second.host() + ":" + calvin_path_
           + "/" + config_file_);
    pthread_create(
        &threads[threads.size()-1],
        NULL,
        SystemFunction,
        reinterpret_cast<void*>(ssh_command));
  }
  for (uint32 i = 0; i < threads.size(); i++) {
    pthread_join(threads[i], NULL);
  }
}

void ClusterManager::GetTempFiles() {
  vector<pthread_t> threads;
  for (const auto & it : config_.machines()) {
    threads.resize(threads.size()+1);
      auto* ssh_command = new string(
              "scp " + ssh_username_ + "@" + it.second.host() +
              ":" + STATS_DIR + "*" + " data/");
    pthread_create(
        &threads[threads.size()-1],
        NULL,
        SystemFunction,
        reinterpret_cast<void*>(ssh_command));
  }
  for (uint32 i = 0; i < threads.size(); i++) {
    pthread_join(threads[i], NULL);
  }
}

void ClusterManager::Cleanup() {
    vector<pthread_t> threads;
    for (const auto & it : config_.machines()) {
        threads.resize(threads.size()+1);
        auto* ssh_command = new string(
                "ssh " + ssh_username_ + "@" + it.second.host() +
                " 'cd " + string(STATS_DIR)+ "; rm *'");
        pthread_create(
                &threads[threads.size()-1],
                NULL,
                SystemFunction,
                reinterpret_cast<void*>(ssh_command));
    }
    for (uint32 i = 0; i < threads.size(); i++) {
        pthread_join(threads[i], NULL);
    }
    threads.clear();
}

void ClusterManager::Update() {
  vector<pthread_t> threads;
  for (map<uint64, MachineInfo>::const_iterator it =
       config_.machines().begin();
       it != config_.machines().end(); ++it) {
    threads.resize(threads.size()+1);
    string* ssh_command = new string(
      "ssh " + ssh_username_ + "@" + it->second.host() +
      " 'cd " + calvin_path_ + ";git checkout calvin.conf;  git pull; cd src; cp Makefile.default Makefile; make clean; make -j'");
    pthread_create(
        &threads[threads.size()-1],
        NULL,
        SystemFunction,
        reinterpret_cast<void*>(ssh_command));
  }
  for (uint32 i = 0; i < threads.size(); i++) {
    pthread_join(threads[i], NULL);
  }
  threads.clear();
}

void ClusterManager::Setup() {
    vector<pthread_t> threads;
    for (map<uint64, MachineInfo>::const_iterator it =
            config_.machines().begin();
         it != config_.machines().end(); ++it) {
        threads.resize(threads.size()+1);
        string* ssh_command = new string(
                "ssh " + ssh_username_ + "@" + it->second.host() +
                " 'cd " + calvin_path_ + ";pwd; ./install-ext'");
        pthread_create(
                &threads[threads.size()-1],
                NULL,
                SystemFunction,
                reinterpret_cast<void*>(ssh_command));
    }
    for (uint32 i = 0; i < threads.size(); i++) {
        pthread_join(threads[i], NULL);
    }
    threads.clear();
}

void ClusterManager::Make() {
    vector<pthread_t> threads;
    for (map<uint64, MachineInfo>::const_iterator it =
            config_.machines().begin();
         it != config_.machines().end(); ++it) {
        threads.resize(threads.size()+1);
        string* ssh_command = new string(
                "ssh " + ssh_username_ + "@" + it->second.host() +
                " 'cd " + calvin_path_ + "/src/; source ../setup ;make clean; make -j'");
        pthread_create(
                &threads[threads.size()-1],
                NULL,
                SystemFunction,
                reinterpret_cast<void*>(ssh_command));
    }
    for (uint32 i = 0; i < threads.size(); i++) {
        pthread_join(threads[i], NULL);
    }
    threads.clear();
}

void ClusterManager::PushCode() {
    vector<pthread_t> threads;
    for (const auto & it : config_.machines()) {
        threads.resize(threads.size()+1);
        auto* ssh_command = new string(
                "ssh " +  ssh_username_ + "@" + it.second.host() + "  'mkdir -p " + calvin_path_ + "/src';" +
                "scp -r -o StrictHostKeyChecking=no " + "./src/* " + ssh_username_ + "@" + it.second.host() + ":~/caerus/src/;" +
                "scp -r -o StrictHostKeyChecking=no " + "./install-ext " + ssh_username_ + "@" + it.second.host() + ":~/caerus/install-ext;" +
                        "scp -r -o StrictHostKeyChecking=no " + "./setup " + ssh_username_ + "@" + it.second.host() + ":~/caerus/setup;");
        pthread_create(
                &threads[threads.size()-1],
                NULL,
                SystemFunction,
                reinterpret_cast<void*>(ssh_command));
    }
    for (uint32 i = 0; i < threads.size(); i++) {
        pthread_join(threads[i], NULL);
    }
    threads.clear();
}

void ClusterManager::DeployCluster(const string &experiment, int percent_mp, int percent_mr, int hot_records, int max_batch_size) {
  vector<pthread_t> threads;
  // Now ssh into all machines and start 'binary' running.
  for (const auto & it : config_.machines()) {
    string val;
    threads.resize(threads.size()+1);
      auto* ssh_command = new string(
       "ssh " +  ssh_username_ + "@" + it.second.host() +
       "  'mkdir -p " + string(STATS_DIR) + "; cd " + calvin_path_ + "; source setup; " + " bin/scripts/" + binary_ +
       " --machine_id=" + IntToString(it.second.id()) + " --mode=" + IntToString(mode_) + " --type=" + IntToString(type_) +
       "  --config=" + config_file_ + " --experiment=" + experiment + " --percent_mp=" + IntToString(percent_mp) + " --percent_mr=" + IntToString(percent_mr) +
       " --hot_records=" + IntToString(hot_records) + " --max_batch_size=" + IntToString(max_batch_size) + args_ + " ' &");

      std::cout << "running " << ssh_command << std::endl;


      pthread_create(
        &threads[threads.size()-1],
        NULL,
        SystemFunction,
        reinterpret_cast<void*>(ssh_command));
  }
  for (uint32 i = 0; i < threads.size(); i++) {
    pthread_join(threads[i], NULL);
  }
}

void ClusterManager::KillCluster() {
  vector<pthread_t> threads;

  for (map<uint64, MachineInfo>::const_iterator it =
          config_.machines().begin();
       it != config_.machines().end(); ++it) {
    threads.resize(threads.size()+1);
      string* ssh_command = new string(
      "ssh " +  ssh_username_ + "@" + it->second.host() +
      " killall -9 " + binary_);
    pthread_create(
        &threads[threads.size()-1],
        NULL,
        SystemFunction,
        reinterpret_cast<void*>(ssh_command));
  }
  for (uint32 i = 0; i < threads.size(); i++) {
    pthread_join(threads[i], NULL);
  }
}

void ClusterManager::ClusterStatus() {
  // 0: Unreachable
  // 1: Calvin not found
  // 2: Running
  // 3: Not Running
  vector<int > cluster_status(config_.machines().size());
  int index = 0;

  std::cout << "running cluster status" << std::endl;

  for (map<uint64, MachineInfo>::const_iterator it =
       config_.machines().begin();
       it != config_.machines().end(); ++it) {
    uint64 machine_id = it->second.id();
    string host = it->second.host();

    string ssh_command = "ssh " + ssh_username_ + "@" + host
                       + "  'cd " + calvin_path_ + "; source setup; bin/scripts/" + binary_
                       + "  --calvin_version=true" + "  --machine_id="
                       + IntToString(machine_id) + "'";
      int status = system(ssh_command.c_str());\
      std::cout << status << std::endl;
    if (status == -1 || WIFEXITED(status) == false ||
        WEXITSTATUS(status) != 0) {
      if (WEXITSTATUS(status) == 255 || WIFEXITED(status) == false) {
        cluster_status[index++] = 0;
      } else if (WEXITSTATUS(status) == 127) {
        cluster_status[index++] = 1;
      } else if (WEXITSTATUS(status) == 254) {
        cluster_status[index++] = 2;
      }
    } else {
      cluster_status[index++] = 3;
    }
  }


  printf("----------------------Cluster Status-----------------------------\n");
  printf("machine id                 host:port                      status \n");
  index = 0;
  for (map<uint64, MachineInfo>::const_iterator it =
       config_.machines().begin();
       it != config_.machines().end(); ++it) {
    uint64 machine_id = it->second.id();
    string host = it->second.host();
    int port = it->second.port();

    switch (cluster_status[index++]) {
      case 0:
        printf("%-10d          %16s:%d              Unreachable\n",
               (int)machine_id, host.c_str(), port);
        break;
      case 1:
        printf("%-10d          %16s:%d              Calvin not found\n",
               (int)machine_id, host.c_str(), port);
        break;
      case 2:
        printf("%-10d          %16s:%d              Running\n",
               (int)machine_id, host.c_str(), port);
        break;
      case 3:
        printf("%-10d          %16s:%d              Not Running\n",
               (int)machine_id, host.c_str(), port);
        break;
      default:
        break;
    }
  }
  printf("-----------------------------------------------------------------\n");
}

const ClusterConfig& ClusterManager::GetConfig() {
  return config_;
}

