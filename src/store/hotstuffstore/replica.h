/***********************************************************************
 *
 * Copyright 2021 Florian Suri-Payer <fsp@cs.cornell.edu>
 *                Yunhao Zhang <yz2327@cornell.edu>
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 **********************************************************************/
#ifndef _HOTSTUFF_REPLICA_H_
#define _HOTSTUFF_REPLICA_H_

#include <memory>

#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/message.h"
#include "lib/persistent_register.h"
#include "lib/udptransport.h"
#include "lib/crypto.h"
#include "lib/keymanager.h"

#include "store/common/stats.h"
#include "store/hotstuffstore/pbft-proto.pb.h"
#include "store/hotstuffstore/slots.h"
#include "store/hotstuffstore/app.h"
#include "store/hotstuffstore/common.h"

// HotStuff library
#include "store/hotstuffstore/libhotstuff/examples/indicus_interface.h"


namespace hotstuffstore {

class Replica : public TransportReceiver {
public:
  Replica(const transport::Configuration &config, KeyManager *keyManager,
    App *app, int groupIdx, int idx, bool signMessages, uint64_t maxBatchSize,
    uint64_t batchTimeoutMS, bool primaryCoordinator, bool requestTx, Transport *transport);
  ~Replica();

  // Message handlers.
  void ReceiveMessage(const TransportAddress &remote, const std::string &type,
                      const std::string &data, void *meta_data);
  void ReceiveMessage_batch(const TransportAddress &remote, const std::vector<std::string> &types,
                      const std::vector<std::string> &datas, void *meta_data);
  void HandleRequest(const TransportAddress &remote,
                           const proto::Request &msg);
  void HandleBatchedRequest(const TransportAddress &remote,
                           proto::BatchedRequest &msg);
  void HandlePreprepare(const TransportAddress &remote,
                              const proto::Preprepare &msg,
                            const proto::SignedMessage& signedMsg);
  void HandlePrepare(const TransportAddress &remote,
                           const proto::Prepare &msg,
                         const proto::SignedMessage& signedMsg);
  void HandleCommit(const TransportAddress &remote,
                          const proto::Commit &msg,
                        const proto::SignedMessage& signedMsg);
  void HandleGrouped(const TransportAddress &remote,
                          const proto::GroupedSignedMessage &msg);

 private:
  // HotStuff
  typedef std::function<void(const std::string&)> hotstuff_exec_callback;
  IndicusInterface hotstuff_interface;
  
  const transport::Configuration &config;
  KeyManager *keyManager;
  App *app;
  int groupIdx;
  int idx; // the replica index within the group
  int id; // unique replica id (across all shards)
  bool signMessages;
  uint64_t maxBatchSize;
  uint64_t batchTimeoutMS;
  bool primaryCoordinator;
  bool requestTx;
  Transport *transport;
  int currentView;
  int nextSeqNum;

  // members to reduce alloc
  proto::SignedMessage tmpsignedMessage;
  proto::Request recvrequest;
  proto::Preprepare recvpreprepare;
  proto::Prepare recvprepare;
  proto::Commit recvcommit;
  proto::BatchedRequest recvbatchedRequest;
  proto::GroupedSignedMessage recvgrouped;
  proto::RequestRequest recvrr;
  proto::ABRequest recvab;

  std::unordered_map<uint64_t, std::string> sessionKeys;
  bool ValidateHMACedMessage(const proto::SignedMessage &signedMessage, std::string &data, std::string &type);
  void CreateHMACedMessage(const ::google::protobuf::Message &msg, proto::SignedMessage& signedMessage);

  Slots slots;

  bool batchTimerRunning;
  int batchTimerId;
  int nextBatchNum;
  // the map from 0..(N-1) to pending digests
  std::unordered_map<uint64_t, std::string> pendingBatchedDigests;
  void sendBatchedPreprepare();

  bool sendMessageToAll(const ::google::protobuf::Message& msg);
  bool sendMessageToPrimary(const ::google::protobuf::Message& msg);

  // map from batched digest to received batched requests
  std::unordered_map<std::string, proto::BatchedRequest> batchedRequests;
  // map from digest to received requests
  std::unordered_map<std::string, proto::PackedMessage> requests;

  // the next sequence number to be executed
  uint64_t execSeqNum;
  uint64_t execBatchNum;
  // map from seqnum to the digest pending execution at that sequence number
  std::unordered_map<uint64_t, std::string> pendingExecutions;

  void SendPreprepare(uint64_t seqnum, const proto::Preprepare& preprepare);
  // map from seqnum to timer ids. If the primary commits the sequence number
  // before the timer expires, then it cancels the timer
  std::unordered_map<uint64_t, int> seqnumCommitTimers;

  // map from tx digest to reply address
  std::unordered_map<std::string, TransportAddress*> replyAddrs;

  // tests to see if we are ready to send commit or executute the slot
  void testSlot(uint64_t seqnum, uint64_t viewnum, std::string digest, bool gotPrepare);

  void executeSlots();

  // map from seqnum to view num to
  std::unordered_map<uint64_t, std::unordered_map<uint64_t, std::unordered_map<std::string, int>>> actionTimers;

  void startActionTimer(uint64_t seq_num, uint64_t viewnum, std::string digest);

  void cancelActionTimer(uint64_t seq_num, uint64_t viewnum, std::string digest);

  Stats* stats;
};

} // namespace hotstuffstore

#endif
