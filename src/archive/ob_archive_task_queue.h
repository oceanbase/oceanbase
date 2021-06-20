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

#ifndef OCEANBASE_ARCHIVE_TASK_QUEUE_H_
#define OCEANBASE_ARCHIVE_TASK_QUEUE_H_

#include "ob_log_archive_struct.h"
#include "ob_archive_util.h"
#include "lib/queue/ob_link_queue.h"  // ObSpLinkQueue

namespace oceanbase {
namespace archive {
typedef common::SpinRWLock RWLock;
typedef common::SpinRLockGuard RLockGuard;
typedef common::SpinWLockGuard WLockGuard;
class ObArchiveSender;
class ObArCLogSplitEngine;
class ObArchiveThreadPool;
// partition archive task management
// single consumer model
struct ObArchiveTaskStatus : common::ObLink {
public:
  ObArchiveTaskStatus();
  virtual ~ObArchiveTaskStatus();
  int64_t count();
  void inc_ref();
  int push_unlock(common::ObLink* link);
  int pop(ObLink*& link, bool& task_exist);
  // remove from global task status queue
  int retire(bool& is_empty, bool& is_discarded);
  // only free in pg_archive_task
  void free(bool& is_discarded);

  VIRTUAL_TO_STRING_KV(K(issue_), K(ref_), K(num_), K(pg_key_));

private:
  int retire_unlock(bool& is_discarded);

protected:
  bool issue_;  // flag of task status in global queue or not
  int64_t ref_;
  int64_t num_;  // num of this pg's total tasks
  common::ObPGKey pg_key_;
  SimpleQueue queue_;  // task queue
  mutable RWLock rwlock_;
};

// Partition Send Task Status
// clog_splitter produce, sender consume
struct ObArchiveSendTaskStatus : public ObArchiveTaskStatus {
public:
  ObArchiveSendTaskStatus(const common::ObPGKey& pg_key);
  ~ObArchiveSendTaskStatus();

  int push(ObArchiveSendTask& task, ObArchiveThreadPool& worker);
  int top(common::ObLink*& link, bool& task_exist);
  common::ObLink* next(common::ObLink& pre);
  int pop_front(const int64_t num);
  int mock_push(ObArchiveSendTask& task, common::ObSpLinkQueue& queue);
  bool mark_io_error();
  void clear_error_info();
  INHERIT_TO_STRING_KV("ObArchiveTaskStatus", ObArchiveTaskStatus, K(error_occur_timestamp_));

private:
  // first IO error occur timestamp when adjoint IO errors occur, reset when success
  int64_t error_occur_timestamp_;
  // IO error count when adjoint IO errors occur, reset when success
  int64_t error_occur_count_;
};

// Partition Clog Split Task Status
// ilog_fetcher produce, clog_splitter consume
struct ObArchiveCLogTaskStatus : public ObArchiveTaskStatus {
public:
  ObArchiveCLogTaskStatus(const common::ObPGKey& pg_key);
  ~ObArchiveCLogTaskStatus();

public:
  int push(ObPGArchiveCLogTask& task, ObArchiveThreadPool& worker);
};

}  // namespace archive
}  // namespace oceanbase

#endif /* OCEANBASE_ARCHIVE_TASK_QUEUE_H_ */
