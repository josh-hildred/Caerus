// Author: Kun Ren <renkun.nwpu@gmail.com>
//


#ifndef _DB_MACHINE_SEQUENCER_H_
#define _DB_MACHINE_SEQUENCER_H_

#include "../../machine/connection.h"
#include "../../../obj/proto/message.pb.h"
#include "paxos.h"
#include "../../machine/client.h"

class Sequencer {
 public:
  // The constructor creates background threads and starts the Sequencer's main
  // loops running.
  Sequencer(ClusterConfig* conf, ConnectionMultiplexer* connection, Client* client, Paxos* paxos, uint32 max_batch_size);

  // Halts the main loops.
  ~Sequencer();

 private:
  // Sequencer's main loops:
  //
  // RunWriter:
  //  while true:
  //    Spend epoch_duration collecting client txn requests into a batch.
  //
  // RunReader:
  //  while true:
  //    Distribute the txns to relevant machines;
  //    Send txns to other replicas;
  //    Append the batch id to paxos log
  //
  // Executes in a background thread created and started by the constructor.
  void RunWriter();
  void RunReader();

  // Functions to start the Multiplexor's main loops, called in new pthreads by
  // the Sequencer's constructor.
  static void* RunSequencerWriter(void *arg);
  static void* RunSequencerReader(void *arg);

  // Sets '*nodes' to contain the node_id of every node participating in 'txn'.
  void FindParticipatingNodes(const TxnProto& txn, set<int>* nodes);

  // Length of time spent collecting client requests before they are ordered,
  // batched, and sent out to schedulers.
  double epoch_duration_;

  // Configuration specifying node & system settings.
  ClusterConfig* configuration_;

  // Connection for sending and receiving protocol messages.
  ConnectionMultiplexer* connection_;

  // Client from which to get incoming txns.
  Client* client_;

  // Separate pthread contexts in which to run the sequencer's main loops.
  pthread_t writer_thread_;
  pthread_t reader_thread_;

  // False until the deconstructor is called. As soon as it is set to true, the
  // main loop sees it and stops.
  bool deconstructor_invoked_;

  Paxos* paxos_log_;

  uint32 max_batch_size_;

  // Number of votes for each batch (used only by machine 0).
  map<uint64, uint32> batch_votes_;

  bool start_working_;
};
#endif  // _DB_MACHINE_SEQUENCER_H_
