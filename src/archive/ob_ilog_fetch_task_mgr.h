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

#ifndef OCEANBASE_ARCHIVE_OB_ILOG_FETCH_TASK_MGR_H_
#define OCEANBASE_ARCHIVE_OB_ILOG_FETCH_TASK_MGR_H_

#include "lib/utility/ob_print_utils.h"
#include "clog/ob_log_define.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "ob_archive_util.h"
#include "ob_log_archive_struct.h"

namespace oceanbase {
namespace archive {
using oceanbase::clog::file_id_t;
class ObArchiveAllocator;
struct PGFetchTask {
public:
  explicit PGFetchTask();
  ~PGFetchTask();
  bool is_valid();
  int assign(const PGFetchTask& task);
  void free();  // free clog_task_, only call when stop archive
  void destroy();
  TO_STRING_KV(K(pg_key_), K(incarnation_), K(archive_round_), K(epoch_), K(start_log_id_), K(ilog_file_id_),
      K(clog_task_), K(clog_size_), K(clog_count_), K(first_log_gen_tstamp_));

public:
  // start_log_id = max_log_id + 1, this log maybe not exist
  common::ObPGKey pg_key_;
  int64_t incarnation_;
  int64_t archive_round_;
  int64_t epoch_;
  uint64_t start_log_id_;
  file_id_t ilog_file_id_;

  ObPGArchiveCLogTask* clog_task_;
  int64_t clog_size_;
  int64_t clog_count_;
  int64_t first_log_gen_tstamp_;
};

class IlogPGFetchQueue {
public:
  IlogPGFetchQueue(ObArchiveAllocator& allocator);
  ~IlogPGFetchQueue();

public:
  int push_task(PGFetchTask& task);
  int pop_task(PGFetchTask& task);
  void set_ilog_file_id(const file_id_t ilog_file_id);
  file_id_t get_ilog_file_id();
  void set_next(IlogPGFetchQueue* next);
  IlogPGFetchQueue* get_next();
  int64_t get_task_count();
  void destroy();
  TO_STRING_KV(K(ilog_file_id_), K(next_), K(pg_array_));
  typedef common::ObSEArray<PGFetchTask, 32> PGArray;

private:
  file_id_t ilog_file_id_;
  IlogPGFetchQueue* next_;
  ObArchiveAllocator& allocator_;
  PGArray pg_array_;
};

class ObArchiveIlogFetchTaskMgr {
public:
  static const int64_t ILOG_FETCH_WAIT_INTERVAL = 100 * 1000L;
  typedef SimpleSortList<IlogPGFetchQueue> IlogPGFetchQueueList;
  typedef common::SpinRWLock RWLock;
  typedef common::SpinWLockGuard WLockGuard;
  typedef common::SpinRLockGuard RLockGuard;

public:
  ObArchiveIlogFetchTaskMgr();
  ~ObArchiveIlogFetchTaskMgr();

public:
  int init(ObArchiveAllocator* allocator);
  void reset();
  int destroy();

public:
  int add_ilog_fetch_task(PGFetchTask& task);

  int pop_ilog_fetch_task(PGFetchTask& task, bool& exist_task);

  int64_t get_alloc_size();

private:
  int generate_cur_ilog_fetch_mgr_();
  void destroy_task_list_();
  // locate ilog pg queue, return the pre one if the queue not exist
  IlogPGFetchQueue* locat_ilog_pg_fetch_mgr_(const file_id_t ilog_file_id, bool& located_exist);
  int handle_add_task_with_ilog_exist_(PGFetchTask& task, IlogPGFetchQueue* location);
  int handle_add_task_with_ilog_not_exist_(PGFetchTask& task, IlogPGFetchQueue* location);
  int pop_ilog_fetch_task_(PGFetchTask& task, bool& exist_task, bool& new_ilog_file);

private:
  bool stop_flag_;
  int64_t ilog_count_;
  IlogPGFetchQueue* cur_ilog_fetch_queue_;
  IlogPGFetchQueueList list_;

  ObArchiveAllocator* allocator_;
  RWLock rwlock_;
};
}  // namespace archive
}  // namespace oceanbase

#endif /* OCEANBASE_ARCHIVE_OB_ILOG_FETCH_TASK_MGR_H_ */
