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
#include "store/common/frontend/async_adapter_client.h"

AsyncAdapterClient::AsyncAdapterClient(Client *client, uint32_t timeout) :
    client(client), timeout(100000UL), outstandingOpCount(0UL), finishedOpCount(0UL) {
}

AsyncAdapterClient::~AsyncAdapterClient() {
}

void AsyncAdapterClient::Execute(AsyncTransaction *txn,
    execute_callback ecb, bool retry) {
  currEcb = ecb;
  currTxn = txn;
  outstandingOpCount = 0UL;
  finishedOpCount = 0UL;
  readValues.clear();
  client->Begin([this](uint64_t id) {
    ExecuteNextOperation();
  }, []{}, timeout, retry);
}

void AsyncAdapterClient::Execute_batch(AsyncTransaction *txn,
    execute_big_callback ecb, bool retry) {
  currEcbcb = ecb;
  currTxn = txn;
  readValues.clear();
  client->Begin_batch([this](uint64_t txNum, uint64_t txSize, uint64_t batchSize) {
    ReconstructTransaction(txNum, txSize, batchSize);
  }, []{}, timeout, retry);
}

void AsyncAdapterClient::ReconstructTransaction(uint64_t txNum, uint64_t txSize, uint64_t batchSize){

  bool tx_conflict_finish = false;
  bool op_conflict_finish = false;
  int OpCount = 0;
  bool duplicate = false;
  batch_size = batchSize;
  
  //Initialize
  tx_num = 0;
  read_set.clear(); 
  readOpNum = 0;
  write_set.clear();
  writeOpNum = 0;
  writeread = false;
  readwrite = false;
  includeRetryTx = false;

  Debug("ReconstructTransaction: txNum: %d, txSize: %d, batchSize: %d\n", txNum, txSize, batchSize);


  //前回のバッチに入れなかったトランザクションを再度バッチに入れる
  if (retry_tx.size() != 0) {
    for (auto op = retry_tx.begin(); op != retry_tx.end(); ++op){
      switch (op->type) {
        case GET: {
          for (auto itr = pre_read_set.begin(); itr != pre_read_set.end(); ++itr){
            if ((*itr).key == op->key){
              duplicate = true;
              break;
            }
          }
          if (duplicate == true){
            Debug("read key is duplicated, so skip following steps");
            duplicate = false;
            continue;
          }
          for (auto itr = pre_write_set.begin(); itr != pre_write_set.end(); ++itr){
            if ((*itr).key == op->key){
              Debug("write-read dependency in same transaction");
              if (readwrite == true){
                Debug("read-write dependency and write-read dependency in same transaction");
                op_conflict_finish = true;
              }
              else {
                writeread = true;
              }
            }
            break;
          }
          pre_read_set.push_back(*op);
          break;
        }
        case PUT: {
          for (auto itr = pre_write_set.begin(); itr != pre_write_set.end(); ++itr){
            if ((*itr).key == op->key){
              duplicate = true;
              break;
            }
          }
          if (duplicate == true){
            Debug("write key is duplicated, so skip following steps");
            duplicate = false;
            continue;
          }
          for (auto itr = pre_read_set.begin(); itr != pre_read_set.end(); ++itr){
            if ((*itr).key == op->key){
              Debug("read-write dependency in same transaction");
              if (writeread == true){
                Debug("read-write dependency and write-read dependency in same transaction");
                op_conflict_finish = true;
              }
              else {
                readwrite = true;
              }
            }
            break;
          }
          pre_write_set.push_back(*op);
          break;
        }
      }
    }

    if (op_conflict_finish == false){
      Debug("%d : transaction finish\n", tx_num);
      for(auto itr = pre_read_set.begin(); itr != pre_read_set.end(); ++itr){
        (*itr).txId = tx_num;
        read_set.push_back(*itr);
        readOpNum++;
      }
      for(auto itr = pre_write_set.begin(); itr != pre_write_set.end(); ++itr){
        (*itr).txId = tx_num;
        write_set.push_back(*itr);
        writeOpNum++;
      }
      pre_write_set.clear();
      pre_read_set.clear();
      tx_num++;
      retry_tx.clear();
      includeRetryTx = true;
    }
    else {
      Panic("This transaction will never commit");
    }
  }



  //トランザクションを再構築する
  while(tx_num < batchSize){
    Debug("tx_num: %d\n", tx_num);
    while (true) {
      Operation op = currTxn->GetNextOperation_batch(OpCount++, tx_num, readValues);
      if (op.type == COMMIT) {
        OpCount = 0;
        break;
      }
      switch (op.type) {
        case GET: {
          retry_tx.push_back(op);
          for (auto itr = pre_read_set.begin(); itr != pre_read_set.end(); ++itr){
            if ((*itr).key == op.key){
              duplicate = true;
              break;
            }
          }
          if (duplicate == true){
            Debug("read key is duplicated, so skip following steps");
            duplicate = false;
            continue;
          }
          for(auto itr = write_set.begin(); itr != write_set.end(); ++itr){
            if ((*itr).key == op.key){
              Debug("write-read conflict with other transaction");
              tx_conflict_finish = true;
            }
            break;
          }
          for (auto itr = pre_write_set.begin(); itr != pre_write_set.end(); ++itr){
            if ((*itr).key == op.key){
              Debug("write-read dependency in same transaction");
              if (readwrite == true){
                Debug("read-write dependency and write-read dependency in same transaction");
                op_conflict_finish = true;
              }
              else {
                writeread = true;
              }
            }
            break;
          }
          pre_read_set.push_back(op);
          break;
        }
        case PUT: {
          retry_tx.push_back(op);
          for (auto itr = pre_write_set.begin(); itr != pre_write_set.end(); ++itr){
            if ((*itr).key == op.key){
              duplicate = true;
              break;
            }
          }
          if (duplicate == true){
            Debug("write key is duplicated, so skip following steps");
            duplicate = false;
            continue;
          }
          for(auto itr = read_set.begin(); itr != read_set.end(); ++itr){
            if ((*itr).key == op.key){
              Debug("write-read conflict with other transaction");
              tx_conflict_finish = true;
            }
            break;
          }
          for (auto itr = pre_read_set.begin(); itr != pre_read_set.end(); ++itr){
            if ((*itr).key == op.key){
               Debug("read-write dependency in same transaction");
              if (writeread == true){
                Debug("read-write dependency and write-read dependency in same transaction");
                op_conflict_finish = true;
              }
              else {
                readwrite = true;
              }
            }
            break;
          }
          pre_write_set.push_back(op);
          break;
        }
      }
    }
    if (tx_conflict_finish == false && op_conflict_finish == false){
      Debug("%d : transaction finish\n", tx_num);
      for(auto itr = pre_read_set.begin(); itr != pre_read_set.end(); ++itr){
        (*itr).txId = tx_num;
        read_set.push_back(*itr);
        readOpNum++;
      }
      for(auto itr = pre_write_set.begin(); itr != pre_write_set.end(); ++itr){
        (*itr).txId = tx_num;
        write_set.push_back(*itr);
        writeOpNum++;
      }

      pre_write_set.clear();
      pre_read_set.clear();
      retry_tx.clear();
      tx_num++;
    }
    else {
      pre_write_set.clear();
      pre_read_set.clear();
      break;
    }
  }

  //basic
  if (writeOpNum == 0){
    ExecuteReadOperation();
  }
  else if (readwrite == true){
    ExecuteReadOperation();
  }
  else{ //Basically
    writeread = true;
    ExecuteWriteOperation();
  }

}

void AsyncAdapterClient::ExecuteWriteOperation(){

  for(auto op = write_set.begin(); op != write_set.end(); ++op){

    // writeする値を作る
    auto strValueItr = readValues.find(op->key);
      std::string strValue;
        if (strValueItr != readValues.end()) {
            strValue = strValueItr->second;
        } else {
            strValue = "";
        }
      if (strValue.length() == 0) {
        op->value = std::string(100, '\0'); //make a longer string
      } else {
        uint64_t intValue = 0;
        for (int i = 0; i < 100; ++i) {
          intValue = intValue | (static_cast<uint64_t>(strValue[i]) << ((99 - i) * 8));
        }
        intValue++;
        for (int i = 0; i < 100; ++i) {
          op->value += static_cast<char>((intValue >> (99 - i) * 8) & 0xFF);
        }
      }

    // writeを実行する
    client->Put(op->key, op->value, std::bind(&AsyncAdapterClient::PutCallback_batch,
            this, std::placeholders::_1, std::placeholders::_2,
            std::placeholders::_3), std::bind(&AsyncAdapterClient::PutTimeout,
              this, std::placeholders::_1, std::placeholders::_2,
              std::placeholders::_3), timeout);
  }

}


void AsyncAdapterClient::ExecuteReadOperation(){
  
   for(auto op = read_set.begin(); op != read_set.end(); ++op){
    client->Get(op->key, std::bind(&AsyncAdapterClient::GetCallback_batch, this,
        std::placeholders::_1, std::placeholders::_2, std::placeholders::_3,
        std::placeholders::_4), std::bind(&AsyncAdapterClient::GetTimeout, this,
          std::placeholders::_1, std::placeholders::_2), timeout);
   }

}


void AsyncAdapterClient::ExecuteCommit(){

  client->Commit(std::bind(&AsyncAdapterClient::CommitBigCallback, this,
        std::placeholders::_1), std::bind(&AsyncAdapterClient::CommitTimeout,
          this), timeout);

}

void AsyncAdapterClient::RedivisionTransaction(){
  std::vector<std::vector<Operation>> returnTx(batch_size);
  for (auto op = write_set.begin(); op != write_set.end(); ++op){
    returnTx[op->txId].push_back(*op);
  }
  for (auto op = read_set.begin(); op != read_set.end(); ++op){
    returnTx[op->txId].push_back(*op);
  }
}


void AsyncAdapterClient::ExecuteNextOperation() {
  //GetNextOperationはstore/benchmark/async/rw/rw_transaction.ccのGetNextOperationである。
  Operation op = currTxn->GetNextOperation(outstandingOpCount, finishedOpCount, readValues);
  switch (op.type) {
    case GET: {
      client->Get(op.key, std::bind(&AsyncAdapterClient::GetCallback, this,
        std::placeholders::_1, std::placeholders::_2, std::placeholders::_3,
        std::placeholders::_4), std::bind(&AsyncAdapterClient::GetTimeout, this,
          std::placeholders::_1, std::placeholders::_2), timeout);
      ++outstandingOpCount;
      // timeout doesn't really matter?
      ExecuteNextOperation();
      break;
    }
    case PUT: {
      client->Put(op.key, op.value, std::bind(&AsyncAdapterClient::PutCallback,
            this, std::placeholders::_1, std::placeholders::_2,
            std::placeholders::_3), std::bind(&AsyncAdapterClient::PutTimeout,
              this, std::placeholders::_1, std::placeholders::_2,
              std::placeholders::_3), timeout);
      ++outstandingOpCount;
      // timeout doesn't really matter?
      ExecuteNextOperation();
      break;
    }
    case COMMIT: {
      client->Commit(std::bind(&AsyncAdapterClient::CommitCallback, this,
        std::placeholders::_1), std::bind(&AsyncAdapterClient::CommitTimeout,
          this), timeout);
      // timeout doesn't really matter?
      break;
    }
    case ABORT: {
      client->Abort(std::bind(&AsyncAdapterClient::AbortCallback, this),
          std::bind(&AsyncAdapterClient::AbortTimeout, this), timeout);
      // timeout doesn't really matter?
      currEcb(ABORTED_USER, std::map<std::string, std::string>());
      break;
    }
    case WAIT: {
      break;
    }
    default:
      NOT_REACHABLE();
  }
}

void AsyncAdapterClient::GetCallback(int status, const std::string &key,
    const std::string &val, Timestamp ts) {
  Debug("Get(%s) callback.", key.c_str());
  readValues.insert(std::make_pair(key, val));
  finishedOpCount++;
  ExecuteNextOperation();
}


void AsyncAdapterClient::GetCallback_batch(int status, const std::string &key,
    const std::string &val, Timestamp ts) {
  Debug("Get(%s) callback batch", key.c_str());
  readValues.insert(std::make_pair(key, val));
  getCbCount++;
  if (readOpNum <= getCbCount){
      if (writeOpNum != 0 && readwrite){
        ExecuteWriteOperation();
      }
      else{
        ExecuteCommit();
      }
      getCbCount = 0;
  }
}

void AsyncAdapterClient::GetTimeout(int status, const std::string &key) {
  Warning("Get(%s) timed out :(", key.c_str());
  client->Get(key, std::bind(&AsyncAdapterClient::GetCallback, this,
        std::placeholders::_1, std::placeholders::_2, std::placeholders::_3,
        std::placeholders::_4), std::bind(&AsyncAdapterClient::GetTimeout, this,
          std::placeholders::_1, std::placeholders::_2), timeout);
}

void AsyncAdapterClient::PutCallback(int status, const std::string &key,
    const std::string &val) {
  Debug("Put(%s,%s) callback.", key.c_str(), val.c_str());
  finishedOpCount++;
  ExecuteNextOperation();
}

void AsyncAdapterClient::PutCallback_batch(int status, const std::string &key,
    const std::string &val){
    Debug("Put(%s,%s) callback batch.", key.c_str(), val.c_str());
    putCbCount++;
    if (writeOpNum <= putCbCount){
      if (readOpNum != 0 && writeread){
        ExecuteReadOperation();
      }
      else {
        ExecuteCommit();
      }
      putCbCount = 0;
    }
}

void AsyncAdapterClient::PutTimeout(int status, const std::string &key,
    const std::string &val) {
  Warning("Put(%s,%s) timed out :(", key.c_str(), val.c_str());
}


void AsyncAdapterClient::CommitCallback(transaction_status_t result) {
  Debug("Commit callback.");
  currEcb(result, readValues);
}

void AsyncAdapterClient::CommitBigCallback(transaction_status_t result) {
  Debug("Commit Big callback.");
  RedivisionTransaction();
  currEcbcb(result, readValues, tx_num, includeRetryTx);
}

void AsyncAdapterClient::CommitTimeout() {
  Warning("Commit timed out :(");
}

void AsyncAdapterClient::AbortCallback() {
  Debug("Abort callback.");
}

void AsyncAdapterClient::AbortTimeout() {
  Warning("Abort timed out :(");
}