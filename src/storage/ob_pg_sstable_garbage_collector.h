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

#ifndef OB_PG_SSTABLE_GARBAGE_COLLECTOR_H_
#define OB_PG_SSTABLE_GARBAGE_COLLECTOR_H_

#include "lib/task/ob_timer.h"

namespace oceanbase {
namespace storage {

class ObPGSSTableGCTask : public common::ObTimerTask {
public:
  ObPGSSTableGCTask();
  virtual ~ObPGSSTableGCTask();
  virtual void runTimerTask() override;

private:
  static const int64_t ONE_ROUND_RECYCLE_COUNT_THRESHOLD = 100000L;
};

class ObPGSSTableGarbageCollector {
public:
  ObPGSSTableGarbageCollector();
  ~ObPGSSTableGarbageCollector();
  int init();
  static ObPGSSTableGarbageCollector& get_instance();
  void stop();
  void wait();
  void destroy();

private:
  int schedule_gc_task();

private:
  static const int64_t GC_INTERVAL_US = 1 * 1000 * 1000L;
  bool is_inited_;
  common::ObTimer timer_;
  ObPGSSTableGCTask gc_task_;
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OB_PG_SSTABLE_GARBAGE_COLLECTOR_H_
