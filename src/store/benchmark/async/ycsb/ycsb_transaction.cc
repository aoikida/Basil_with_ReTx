/***********************************************************************
 *
 * Copyright 2024 Aoi Kida
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
#include "store/benchmark/async/ycsb/ycsb_transaction.h"
#include <cstdlib> 

namespace ycsb {

YCSBTransaction::YCSBTransaction(KeySelector *keySelector, int numOps, int readRatio, 
std::mt19937 &rand) : keySelector(keySelector), numOps(numOps), readRatio(readRatio){
  for (int i = 0; i < numOps; ++i) {
    uint64_t key = keySelector->GetKey(rand);
    std::cout << "key:" << key << std::endl;
    keyIdxs.push_back(key);
  }
}

YCSBTransaction::~YCSBTransaction() {
}

Operation YCSBTransaction::GetNextOperation(size_t outstandingOpCount, size_t finishedOpCount,
    std::map<std::string, std::string> readValues) {
  if (finishedOpCount < GetNumOps()) {
    std::cerr << "outstanding: " << outstandingOpCount << "; finished: " << finishedOpCount << "num ops: " << GetNumOps() << std::endl;
    if (finishedOpCount != outstandingOpCount) {
      return Wait();
    }
    else if ((rand() % 100) < readRatio) {
      std::cerr << "read: " << GetKey(finishedOpCount) << std::endl;
      return Get(GetKey(finishedOpCount));
    } 
    else {
        std::cerr << "write: " << GetKey(finishedOpCount) << std::endl;
        auto strValueItr = readValues.find(GetKey(finishedOpCount));

        std::string strValue;
        if (strValueItr != readValues.end()) {
            strValue = strValueItr->second;
        } else {
            strValue = "";
        }

        std::string writeValue;
        if (strValue.length() == 0) {
            writeValue = std::string(100, '\0'); // make a longer string
        } else {
            uint64_t intValue = 0;
            for (int i = 0; i < 100; ++i) {
                intValue = intValue | (static_cast<uint64_t>(strValue[i]) << ((99 - i) * 8));
            }
            intValue++;
            for (int i = 0; i < 100; ++i) {
                writeValue += static_cast<char>((intValue >> (99 - i) * 8) & 0xFF);
            }
        }
        return Put(GetKey(finishedOpCount), writeValue);
    }
  }
  else if (finishedOpCount == GetNumOps()) {
    std::cerr << "commit" << std::endl;
    return Commit();
  }
  else {
    return Wait();
  }
}


Operation YCSBTransaction::GetNextOperation_batch(size_t OpCount, std::map<std::string, std::string> readValues) {
  
  Debug("Operation count: %d\n", OpCount);
  if ((rand() % 100) < readRatio) {
    std::cerr << "read: " << GetKey(OpCount) << std::endl;
    return Get(GetKey(OpCount));
  } 
  else {
      std::cerr << "write: " << GetKey(OpCount) << std::endl;
      auto strValueItr = readValues.find(GetKey(OpCount));

      std::string strValue;
      if (strValueItr != readValues.end()) {
          strValue = strValueItr->second;
      } else {
          strValue = "";
      }

      std::string writeValue;
      if (strValue.length() == 0) {
          writeValue = std::string(100, '\0'); // make a longer string
      } else {
          uint64_t intValue = 0;
          for (int i = 0; i < 100; ++i) {
              intValue = intValue | (static_cast<uint64_t>(strValue[i]) << ((99 - i) * 8));
          }
          intValue++;
          for (int i = 0; i < 100; ++i) {
              writeValue += static_cast<char>((intValue >> (99 - i) * 8) & 0xFF);
          }
      }
      return Put(GetKey(OpCount), writeValue);
  }
}


} // namespace ycsb
