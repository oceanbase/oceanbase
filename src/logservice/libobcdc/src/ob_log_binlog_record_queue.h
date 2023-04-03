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
 *
 * Binlog Record Queue
 */

#ifndef OCEANBASE_LIBOBCDC_OB_LOG_BINLOG_RECORD_QUEUE_
#define OCEANBASE_LIBOBCDC_OB_LOG_BINLOG_RECORD_QUEUE_

#include "lib/queue/ob_fixed_queue.h"     // ObFixedQueue
#include "common/ob_queue_thread.h"       // ObCond

#include "ob_log_binlog_record.h"         // ObLogBR

namespace oceanbase
{
namespace libobcdc
{
class BRQueue
{
public:
  BRQueue() :
      inited_(false),
      queue_(),
      cond_(),
      dml_br_count_(0),
      ddl_br_count_(0),
      part_trans_task_count_(0)
  {}

  virtual ~BRQueue() { destroy(); }

public:
  int init(const int64_t queue_size);
  void destroy();

  // To support large transactions - implement a streaming commit model where each push and pop is a separate ObLogBR
  int push(ObLogBR *data, const int64_t timeout);
  int pop(IBinlogRecord *&record, const int64_t timeout);
  int pop(IBinlogRecord *&record, int32_t &major_version, uint64_t &tenant_id, const int64_t timeout);

  int64_t get_dml_br_count() const;
  int64_t get_ddl_br_count() const;
  int64_t get_part_trans_task_count() const;

private:
  int pop_next_br_(ObLogBR *&data, const int64_t timeout);
  int do_stat_for_part_trans_task_count_(ObLogBR &data,
      bool need_accumulate_stat);

private:
  bool                                    inited_;
  common::ObFixedQueue<ObLogBR>           queue_;
  common::ObCond                          cond_;

  int64_t dml_br_count_ CACHE_ALIGNED;
  int64_t ddl_br_count_ CACHE_ALIGNED;

  // Statistics on the number of partitioned transaction tasks
  int64_t part_trans_task_count_ CACHE_ALIGNED;
};
} // namespace libobcdc
} // namespace oceanbase
#endif /* OCEANBASE_LIBOBCDC_OB_LOG_BINLOG_RECORD_QUEUE_ */
