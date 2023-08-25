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

#ifndef OCEANBASE_LOGSERVICE_OB_FETCH_LOG_ENGINE_
#define OCEANBASE_LOGSERVICE_OB_FETCH_LOG_ENGINE_
#include <stdint.h>
#include "lib/thread/ob_simple_thread_pool.h"
#include "lib/thread/thread_mgr_interface.h"
#include "log_define.h"
#include "log_req.h"
#include "lsn.h"

namespace oceanbase
{
namespace common
{
class ObILogAllocator;
}
namespace palf
{
class IPalfEnvImpl;
class FetchLogTask
{
public:
  FetchLogTask()
  { reset(); }
  ~FetchLogTask() { reset(); }
  void reset();
public:
  bool is_valid() const;
  int set(const int64_t id,
          const common::ObAddr &server,
          const FetchLogType fetch_type,
          const int64_t &proposal_id,
          const LSN &prev_lsn,
          const LSN &start_lsn,
          const int64_t log_size,
          const int64_t log_count,
          const int64_t accepted_mode_pid);
  int64_t get_timestamp_us() const { return timestamp_us_; }
  int64_t get_id() const { return id_; }
  const common::ObAddr &get_server() const { return server_; }
  FetchLogType get_fetch_type() const { return fetch_type_; }
  const int64_t &get_proposal_id() const { return proposal_id_; }
  const LSN &get_prev_lsn() const { return prev_lsn_; }
  const LSN &get_start_lsn() const { return start_lsn_; }
  int64_t get_log_size() const { return log_size_; }
  int64_t get_log_count() const { return log_count_; }
  int64_t get_accepted_mode_pid() const { return accepted_mode_pid_; }
  FetchLogTask& operator=(const FetchLogTask &task);

  TO_STRING_KV(K_(timestamp_us), K_(id), K_(server), "fetch_type", fetch_type_2_str(fetch_type_), K_(proposal_id),
               K_(prev_lsn), K_(start_lsn), K_(log_size), K_(log_count), K_(accepted_mode_pid));
private:
  int64_t timestamp_us_;
  int64_t id_;
  common::ObAddr server_;
  FetchLogType fetch_type_;
  int64_t proposal_id_;
  LSN prev_lsn_;
  LSN start_lsn_;
  int64_t log_size_;
  int64_t log_count_;
  int64_t accepted_mode_pid_;
};

class FetchLogEngine : public lib::TGTaskHandler
{
public:
  // dynamic with tenant unit
  static const int64_t FETCH_LOG_THREAD_COUNT = 1;
  static const int64_t MINI_MODE_FETCH_LOG_THREAD_COUNT = 1;
  static const int64_t FETCH_LOG_TASK_MAX_COUNT_PER_LS = 64;
  static const int64_t DEFAULT_CACHED_FETCH_TASK_NUM = 8;
  static const int64_t MAX_CACHED_FETCH_TASK_NUM = 1024;
public:
  FetchLogEngine();
  ~FetchLogEngine() { destroy(); }
public:
  int init(IPalfEnvImpl *palf_env_impl, ObILogAllocator *alloc_mgr);
  void destroy();
  int submit_fetch_log_task(FetchLogTask *fetch_log_task);
public:
  int start();
  int stop();
  int wait();
  void handle(void *task);
  void handle_drop(void *task);
  FetchLogTask *alloc_fetch_log_task();
  void free_fetch_log_task(FetchLogTask *task);
  int update_replayable_point(const share::SCN &replayable_scn);
private:
  int try_remove_task_from_cache_(FetchLogTask *fetch_log_task);
  int push_task_into_cache_(FetchLogTask *fetch_log_task);
private:
  typedef common::ObSpinLock SpinLock;
  typedef common::ObSpinLockGuard SpinLockGuard;
private:
  int tg_id_;
  bool is_inited_;
  IPalfEnvImpl *palf_env_impl_;
  common::ObILogAllocator *allocator_;
  share::SCN replayable_point_;
  mutable SpinLock cache_lock_;
  ObSEArray<FetchLogTask, DEFAULT_CACHED_FETCH_TASK_NUM> fetch_task_cache_;
  ObMiniStat::ObStatItem fetch_wait_cost_stat_;
  ObMiniStat::ObStatItem fetch_log_cost_stat_;
  DISALLOW_COPY_AND_ASSIGN(FetchLogEngine);
};
} // namespace logservice
} // namespace oceanbase

#endif // OCEANBASE_LOGSERVICE_OB_FETCH_LOG_ENGINE_
