/***********************************************************************
 *
 * Copyright 2021 Florian Suri-Payer <fsp@cs.cornell.edu>
 *                Matthew Burke <matthelb@cs.cornell.edu>
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
// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * common/client.h:
 *   Interface for a multiple shard transactional client.
 *
 **********************************************************************/

#ifndef _SYNC_CLIENT_API_H_
#define _SYNC_CLIENT_API_H_

#include "lib/assert.h"
#include "lib/message.h"
#include "store/common/timestamp.h"
#include "store/common/partitioner.h"
#include "store/common/frontend/client.h"
#include "store/common/promise.h"

#include <functional>
#include <string>
#include <vector>

class SyncClient {
 public:
  SyncClient(Client *client);
  virtual ~SyncClient();

  //virtual void Execute(SyncTransaction *txn, execute_callback ecb, bool retry=false) = 0;

  //virtual void Execute_batch(SyncTransaction *txn, execute_callback_batch ecb, bool retry=false) = 0;

  // Begin a transaction.
  virtual void Begin(uint32_t timeout);

  // Get the value corresponding to key.
  virtual void Get(const std::string &key, std::string &value,
      uint32_t timeout);

  // Get value without waiting.
  void Get(const std::string &key, uint32_t timeout);

  // Wait for outstanding Gets to finish in FIFO order.
  void Wait(std::vector<std::string> &values);

  // Set the value for the given key.
  virtual void Put(const std::string &key, const std::string &value,
      uint32_t timeout);

  // Commit all Get(s) and Put(s) since Begin().
  virtual transaction_status_t Commit(uint32_t timeout);
  
  // Abort all Get(s) and Put(s) since Begin().
  virtual void Abort(uint32_t timeout);

 private:
  void GetCallback(Promise *promise, int status, const std::string &key, const std::string &value,
      Timestamp ts);
  void GetTimeoutCallback(Promise *promise, int status, const std::string &key);
  void PutCallback(Promise *promise, int status, const std::string &key,
      const std::string &value);
  void PutTimeoutCallback(Promise *promise, int status, const std::string &key,
      const std::string &value);
  void CommitCallback(Promise *promise, transaction_status_t status);
  void CommitTimeoutCallback(Promise *promise);
  void AbortCallback(Promise *promise);
  void AbortTimeoutCallback(Promise *promise);

  std::vector<Promise *> getPromises;

  Client *client;
};

#endif /* _SYNC_CLIENT_API_H_ */
