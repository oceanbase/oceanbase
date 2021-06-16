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

#ifndef OCEANBASE_ARCHIVE_THREAD_POOL_H_
#define OCEANBASE_ARCHIVE_THREAD_POOL_H_

#include "share/ob_thread_pool.h"  // ObThreadPool
#include "ob_log_archive_define.h"
#include "lib/queue/ob_lighty_queue.h"  // ObLightyQueue

namespace oceanbase {
namespace archive {
class ObArchiveTaskStatus;
class ObArchiveThreadPool : public share::ObThreadPool {
public:
  ObArchiveThreadPool();
  ~ObArchiveThreadPool();

  int init();
  int modify_thread_count();
  int64_t get_total_task_num();
  int push_task_status(ObArchiveTaskStatus* task_status);
  void destroy();

public:
  virtual int64_t cal_work_thread_num() = 0;
  virtual void set_thread_name_str(char* str) = 0;
  virtual int handle_task_list(ObArchiveTaskStatus* status) = 0;

private:
  void run1();
  void do_thread_task_();

protected:
  bool inited_;
  bool round_stop_flag_;
  int64_t total_task_count_;
  common::ObLightyQueue task_queue_;  // task queue of PG ObArchiveTaskStatus

  char thread_name_[MAX_ARCHIVE_THREAD_NAME_LENGTH];
};
}  // namespace archive
}  // namespace oceanbase
#endif /* OCEANBASE_ARCHIVE_THREAD_POOL_H_ */
