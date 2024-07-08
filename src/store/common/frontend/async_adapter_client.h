/***********************************************************************
 * Copyright 2024 AoiKida
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
#ifndef ASYNC_ADAPTER_CLIENT_H
#define ASYNC_ADAPTER_CLIENT_H

#include "store/common/frontend/async_client.h"


class AsyncAdapterClient : public AsyncClient {
 public:
  AsyncAdapterClient(Client *client, uint32_t timeout);
  virtual ~AsyncAdapterClient();

  // Begin a transaction.
  virtual void Execute(AsyncTransaction *txn, execute_callback ecb, bool retry = false);

  virtual void Execute_batch(AsyncTransaction *txn, execute_big_callback ecb, bool retry = false);

 private:
  void ExecuteNextOperation();
  void GetCallback(int status, const std::string &key, const std::string &val,
      Timestamp ts);
  void GetTimeout(int status, const std::string &key);
  void PutCallback(int status, const std::string &key, const std::string &val);
  void PutTimeout(int status, const std::string &key, const std::string &val);
  void CommitCallback(transaction_status_t result);
  void CommitBigCallback(transaction_status_t result);
  void CommitTimeout();
  void AbortCallback();
  void AbortTimeout();

  //追加
  void MakeTransaction_no_abort(uint64_t txNum, uint64_t txSize, uint64_t batchSize, std::vector<int> abort_tx_nums);
  void MakeTransaction_single_abort(uint64_t txNum, uint64_t txSize, uint64_t batchSize, std::vector<int> abort_tx_nums);
  void MakeTransaction_multi_abort(uint64_t txNum, uint64_t txSize, uint64_t batchSize, std::vector<int> abort_tx_nums);
  void ExecuteWriteOperation();
  void ExecuteReadOperation();
  void ExecuteCommit();
  int writeOpNum = 0;
  int readOpNum = 0;
  int commitTxNum = 0;
  int putCbCount = 0;
  int getCbCount = 0;
  int commitCbCount = 0;
  std::vector<transaction_status_t> results;
  std::multimap<std::string, int> keyTxMap;
  std::vector<std::pair<int, std::vector<Operation>>> txNum_writeSet;

  Client *client;
  uint32_t timeout;
  size_t outstandingOpCount;
  size_t finishedOpCount;
  std::map<std::string, std::string> readValues;
  execute_callback currEcb;
  execute_big_callback currEcbcb;
  execute_callback_batch currEcbb;
  AsyncTransaction *currTxn;

  //追加
  std::vector<Operation> transaction;
  std::map<int, std::vector<Operation>> batch;
  std::vector<Operation> read_set;
  std::vector<Operation> pre_read_set;
  std::vector<Operation> write_set;
  std::vector<Operation> pre_write_set;
  std::vector<Operation> conflict_write_set;
  std::vector<std::vector<Operation>> abort_set;
  uint64_t batch_size;
  int txSize;
  int batchSize;

  std::vector<std::string> key_list;

  std::vector<get_callback> gcb_list;

  bool wait_flag;
  bool writeread = false;
  bool readwrite = false;

  void PutCallback_batch(int status, const std::string &key,
    const std::string &val);

  void GetCallback_batch(int status, const std::string &key,
    const std::string &val, Timestamp ts);

  //void ExecuteNextOperation_batch();

};

#endif /* ASYNC_ADAPTER_CLIENT_H */
