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
#ifndef ASYNC_DELIVERY_H
#define ASYNC_DELIVERY_H

#include <string>
#include <unordered_map>
#include <vector>
#include <random>

#include "store/benchmark/async/tpcc/delivery.h"
#include "store/benchmark/async/tpcc/async/tpcc_transaction.h"

namespace tpcc {

enum DeliveryState {
  DS_EARLIEST_NO = 0,
  DS_NEW_ORDER,
  DS_ORDER,
  DS_ORDER_LINES,
  DS_CUSTOMER
};

class AsyncDelivery : public AsyncTPCCTransaction, public Delivery {
 public:
  AsyncDelivery(uint32_t w_id, uint32_t d_id, std::mt19937 &gen);
  virtual ~AsyncDelivery();

  Operation GetNextOperation(size_t outstandingOpCount, size_t finishedOpCount,
      std::map<std::string, std::string> readValues);

  /*
  
  Operation GetNextOperation_batch(size_t outstandingOpCount, size_t finishedOpCount,
      std::map<std::string, std::string> readValues, int batchSize, Xoroshiro128Plus &rnd, FastZipf &zipf);
  */

 private:
  uint8_t currDId;
  DeliveryState state;
};

} // namespace tpcc

#endif /* ASYNC_DELIVERY_H */
