//
// Author: Joshua Hildred jt<lastname>@gmail.com>
//

#ifndef _DB_PARTIAL_SEQUENCER_H_
#define _DB_PARTIAL_SEQUENCER_H_
#include "machine/connection.h"
#include "proto/message.pb.h"
#include "sequencer/calvin/paxos.h"
#include "machine/client.h"
#include "common/internal_ids.h"
#include <boost/lockfree/spsc_queue.hpp>





class PartialSequencer {
public:
    // The constructor creates background threads and starts the Sequencer's main
    // loops running.
    PartialSequencer(ClusterConfig *conf, ConnectionMultiplexer *connection, Client *client, uint32 max_batch_size,
                     InternalIdTracker * ids);

    // Halts the main loops.
    ~PartialSequencer();


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
    void RunReader(int i);

    // Functions to start the Multiplexor's main loops, called in new pthreads by
    // the Sequencer's constructor.
    static void* RunSequencerWriter(void *arg);
    static void* RunSequencerReader(void *arg);

    // Length of time spent collecting client requests before they are ordered,
    // batched, and sent out to schedulers.
    double epoch_duration_;
    

    // Configuration specifying node & system settings.
    ClusterConfig* configuration_;

    // Connection for sending and receiving protocol messages.
    ConnectionMultiplexer* connection_;

    InternalIdTracker * ids_;


    // Client from which to get incoming txns.
    Client* client_;

    // Separate pthread contexts in which to run the sequencer's main loops.
    pthread_t writer_thread_;
    pthread_t reader_thread_;

    // False until the deconstructor is called. As soon as it is set to true, the
    // main loop sees it and stops.
    bool deconstructor_invoked_;

    uint32 max_batch_size_;

    // Number of votes for each batch (used only by machine 0).

    bool start_working_;





};
#endif
