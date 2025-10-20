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

#ifndef OCEANBASE_SQL_OB_MEMORY_TRACKER_H
#define OCEANBASE_SQL_OB_MEMORY_TRACKER_H

#include "lib/alloc/alloc_func.h"

namespace oceanbase
{
namespace lib
{
class MemoryContext;

struct ObMemTracker
{
  ObMemTracker() :
    cur_mem_used_(0), peek_mem_used_(0),  
    cache_mem_limit_(-1), check_status_times_(0), try_check_tick_(0), mem_quota_pct_(0),
    mem_context_(nullptr)
  {}
  void reset()
  {
    cur_mem_used_ = 0;
    peek_mem_used_ = 0;
    cache_mem_limit_ = -1;
    check_status_times_ = 0;
    try_check_tick_ = 0;
    mem_quota_pct_ = 0;
    mem_context_ = nullptr;
  }

  int64_t cur_mem_used_; 
  int64_t peek_mem_used_;
  int64_t cache_mem_limit_;
  uint16_t check_status_times_;
  uint16_t try_check_tick_;
  int64_t mem_quota_pct_;
  lib::MemoryContext *mem_context_;
};

class ObMemTrackerGuard
{
public:
  const static uint64_t DEFAULT_CHECK_STATUS_TRY_TIMES = 1024;
  const static uint64_t UPDATE_MEM_LIMIT_THRESHOLD = 512;
  const static int64_t UNLIMITED_MEM_QUOTA_PCT = 100;
  ObMemTrackerGuard(lib::MemoryContext &mem_context)
  {
    mem_tracker_.reset();
    mem_tracker_.mem_context_ = &mem_context;
  }
  ~ObMemTrackerGuard()
  {
    mem_tracker_.reset();
  }
  static void reset_try_check_tick();
  static void dump_mem_tracker_info();
  static void update_config();
  static int check_status();
  static int try_check_status(int64_t check_try_times = DEFAULT_CHECK_STATUS_TRY_TIMES);

private:
  static thread_local ObMemTracker mem_tracker_;
};

} // end namespace lib
} // end namespace oceanbase

#define MEM_TRACKER_GUARD(mem_context)                                      \
oceanbase::lib::ObMemTrackerGuard mem_tracker_guard(mem_context);
#define RESET_TRY_CHECK_TICK                                                \
oceanbase::lib::ObMemTrackerGuard::reset_try_check_tick();
#define CHECK_MEM_STATUS()                                                  \
oceanbase::lib::ObMemTrackerGuard::check_status()
#define TRY_CHECK_MEM_STATUS(check_try_times)                               \
oceanbase::lib::ObMemTrackerGuard::try_check_status(check_try_times)

#endif /* OCEANBASE_SQL_OB_MEMORY_TRACKER_H */
