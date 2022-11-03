/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SIGNAL_PROCESSOR_H_
#define OCEANBASE_SIGNAL_PROCESSOR_H_

#include <stdint.h>
#include "lib/ob_errno.h"

namespace oceanbase
{
namespace common
{

/*
  各接口调用的时间与空间(从上到下为时间序列)
         worker_thread         destination_thread

     worker_thread: start()
                               thread1: prepare()
     worker_thread: process()
                               thread2: prepare()
     worker_thread: process()
                               thread3: prepare()
     worker_thread: process()
                               thread4: prepare()
     worker_thread: end()
 */
class ObSigProcessor
{
public:
  virtual int start() { return OB_SUCCESS; }
  virtual int end() { return OB_SUCCESS; }
  virtual int prepare() = 0;
  virtual int process() = 0;
  virtual ~ObSigProcessor() = 0;
};
inline ObSigProcessor::~ObSigProcessor() {}

class ObSigBTOnlyProcessor : public ObSigProcessor
{
public:
  ObSigBTOnlyProcessor();
  ~ObSigBTOnlyProcessor();
  int start() override;
  int prepare() override;
  int process() override;
protected:
  int fd_;
  char filename_[128];
  char buf_[1024];
  int64_t pos_;
};

class ObSigBTSQLProcessor: public ObSigBTOnlyProcessor
{
public:
  ObSigBTSQLProcessor() = default;
  virtual int prepare() override;
  virtual int process() override;
private:
};

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_SIGNAL_PROCESSOR_H_
