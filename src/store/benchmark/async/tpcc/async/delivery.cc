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
#include "store/benchmark/async/tpcc/async/delivery.h"

#include <chrono>
#include <sstream>
#include <ctime>

#include "store/benchmark/async/tpcc/tpcc-proto.pb.h"
#include "store/benchmark/async/tpcc/tpcc_utils.h"

namespace tpcc {

AsyncDelivery::AsyncDelivery(uint32_t w_id, uint32_t d_id, std::mt19937 &gen) :
    Delivery(w_id, d_id, gen) {
}

AsyncDelivery::~AsyncDelivery() {
}

Operation AsyncDelivery::GetNextOperation(size_t outstandingOpCount, size_t finishedOpCount,
  std::map<std::string, std::string> readValues) {

    if (finishedOpCount != outstandingOpCount){
      return Wait();
    }
    else if (finishedOpCount == 0) {
      Debug("Warehouse: %u", w_id);
      Debug("District: %u", d_id);
      // Get the oldest outstanding new-order
      ero_key = EarliestNewOrderRowKey(w_id, d_id);
      return Get(ero_key);
    }
    else if (finishedOpCount == 1) {
      auto eno_row_itr = readValues.find(ero_key);
      UW_ASSERT(eno_row_itr != readValues.end());
      EarliestNewOrderRow eno_row;
      UW_ASSERT(eno_row.ParseFromString(eno_row_itr->second));
      o_id = eno_row.o_id();
      Debug("  Earliest New Order: %u", o_id);
      eno_row.set_o_id(o_id + 1);
      std::string eno_row_out;
      eno_row.SerializeToString(&eno_row_out);
      return Put(ero_key, eno_row_out);
    }
    else if (finishedOpCount == 2) {
      // delete new order
      return Put(NewOrderRowKey(w_id, d_id, o_id), ""); 
    }
    else if (finishedOpCount == 3) {
      o_key = OrderRowKey(w_id, d_id, o_id);
      return Get(o_key);
    }
    else if (finishedOpCount == 4) {
      auto o_row_itr = readValues.find(o_key);
      UW_ASSERT(o_row_itr != readValues.end());
      UW_ASSERT(o_row.ParseFromString(o_row_itr->second));
      o_row.set_carrier_id(o_carrier_id);
      Debug("  Carrier ID: %u", o_carrier_id);
      std::string o_row_out;
      o_row.SerializeToString(&o_row_out);
      return Put(o_key, o_row_out);
    }
    else if (5 <= finishedOpCount && finishedOpCount < 5 + o_row.ol_cnt()) {
      int ol_number = finishedOpCount - 5;
      ol_key = OrderLineRowKey(w_id, d_id, o_id, ol_number);
      return Get(ol_key);
    }
    else if (5 + o_row.ol_cnt() <= finishedOpCount && finishedOpCount < 5 + 2 * o_row.ol_cnt()) {
      int ol_number = finishedOpCount - o_row.ol_cnt();
      Debug("Order Line %lu", ol_number);
      auto ol_row_itr = readValues.find(ol_key);
      OrderLineRow ol_row;
      UW_ASSERT(ol_row.ParseFromString(ol_row_itr->second));
      Debug("Amount: %i", ol_row.amount());
      total_amount += ol_row.amount();
      ol_row.set_delivery_d(ol_delivery_d);
      std::string ol_row_out;
      ol_row.SerializeToString(&ol_row_out);
      Debug("Delivery Date: %u", ol_delivery_d);
      return Put(ol_key, ol_row_out);
    }
    else if (finishedOpCount == 5 + 2 * o_row.ol_cnt()) {
      Debug("Customer: %u", o_row.c_id());
      c_key = CustomerRowKey(w_id, d_id, o_row.c_id());
      return Get(c_key);
    }
    else if (finishedOpCount == 5 + 2 * o_row.ol_cnt() + 1) {
      auto c_row_itr = readValues.find(c_key);
      UW_ASSERT(c_row_itr != readValues.end());
      CustomerRow c_row;
      UW_ASSERT(c_row.ParseFromString(c_row_itr->second));
      Debug("Old Balance: %i", c_row.balance());
      Debug("Total Amount: %i", total_amount);
      c_row.set_balance(c_row.balance() + total_amount);
      Debug("New Balance: %i", c_row.balance());
      c_row.set_delivery_cnt(c_row.delivery_cnt() + 1);
      std::string c_row_out;
      c_row.SerializeToString(&c_row_out);
      Debug("Delivery Count: %u", c_row.delivery_cnt());
      return Put(c_key, c_row_out);
    }
    else {
      Debug("Delivery Transaction Commit");
      return Commit();
    }
}


Operation AsyncDelivery::GetNextOperation_batch(size_t OpCount, std::map<std::string, std::string> readValues) {}


}
