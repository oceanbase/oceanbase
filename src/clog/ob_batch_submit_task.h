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

#ifndef OCEANBASE_CLOG_OB_BATCH_SUBMIT_TASK_H_
#define OCEANBASE_CLOG_OB_BATCH_SUBMIT_TASK_H_

#include "common/ob_partition_key.h"
#include "ob_buffer_task.h"
#include "ob_log_common.h"
#include "ob_log_define.h"
#include "storage/transaction/ob_trans_define.h"

namespace oceanbase {
namespace common {
class ObILogAllocator;
}
namespace clog {
class ObICLogMgr;
class ObILogEngine;
class ObBatchSubmitDiskTask : public ObIBufferTask {
public:
  ObBatchSubmitDiskTask()
  {
    reset();
  }
  ~ObBatchSubmitDiskTask()
  {
    reset();
  }

public:
  void reset();
  int init(const transaction::ObTransID& trans_id, const common::ObPartitionArray& partition_array,
      const ObLogInfoArray& log_info_array, ObICLogMgr* clog_mgr, ObILogEngine* log_engine);
  virtual int64_t get_data_len() const;
  virtual int64_t get_entry_cnt() const;
  virtual int fill_buffer(char* buf, const offset_t offset);
  virtual int st_after_consume(const int handle_err);
  virtual int after_consume(const int handle_err, const void* arg, const int64_t before_push_cb_ts);
  TO_STRING_KV(K(partition_array_), K(log_info_array_), K(offset_));

private:
  bool is_inited_;
  transaction::ObTransID trans_id_;
  common::ObPartitionArray partition_array_;
  ObLogInfoArray log_info_array_;
  ObICLogMgr* clog_mgr_;
  ObILogEngine* log_engine_;
  offset_t offset_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBatchSubmitDiskTask);
};

class ObBatchSubmitDiskTaskFactory {
public:
  static ObBatchSubmitDiskTask* alloc(common::ObILogAllocator* alloc_mgr);
  static void free(ObBatchSubmitDiskTask* task);
  static void statistics();

private:
  static int64_t alloc_cnt_;
  static int64_t free_cnt_;
};
}  // namespace clog
}  // namespace oceanbase

#endif  // OCEANBASE_CLOG_OB_BATCH_SUBMIT_TASK_H_
