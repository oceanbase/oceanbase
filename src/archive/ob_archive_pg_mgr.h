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

#ifndef OCEANBASE_ARCHIVE_PG_MGR_H_
#define OCEANBASE_ARCHIVE_PG_MGR_H_

#include "ob_pg_archive_task.h"        // ObPGArchiveTask, PGArchiveAlloc
#include "lib/hash/ob_link_hashmap.h"  // ObLinkHashMap
#include "common/ob_partition_key.h"   // ObPartitionKey
#include "lib/queue/ob_link_queue.h"   // ObSpLinkQueue
#include "ob_archive_util.h"           //
#include "clog/ob_log_define.h"        // file_id_t
#include "ob_archive_log_wrapper.h"
#include "ob_start_archive_helper.h"
#include "share/ob_thread_pool.h"

namespace oceanbase {
namespace storage {
class ObIPartitionGroup;
class ObPartitionService;
}  // namespace storage
namespace archive {
using oceanbase::common::ObPGKey;
using oceanbase::storage::ObIPartitionGroup;
using oceanbase::storage::ObPartitionService;
typedef common::ObLinkHashMap<ObPGKey, ObPGArchiveTask> PGArchiveMap;

struct ObPGArchiveCLogTask;
class ObArchiveMgr;
class ObArchiveRoundMgr;
class StartArchiveHelper;
class ObArchivePGMgr : public share::ObThreadPool {
public:
  static const int64_t THREAD_RUN_INTERVAL = 1 * 1000 * 1000L;
  static const int64_t RECONFIRM_PG_INTERVAL = 30 * 1000 * 1000L;
  static const int64_t PG_MGR_QUEUE_SIZE = 5;
  static const int64_t PG_MGR_THREAD_COUNT = PG_MGR_QUEUE_SIZE;

public:
  ObArchivePGMgr();
  virtual ~ObArchivePGMgr();

public:
  int init(ObArchiveAllocator* allocator, ObArchiveLogWrapper* log_wrapper, ObPartitionService* partition_service,
      ObArchiveRoundMgr* archive_round_mgr, ObArchiveMgr* archive_mgr);
  void destroy();
  int start();
  void stop();
  void wait();
  int reset_tasks();
  void clear_archive_info();

private:
  void run1();

public:
  int add_pg_archive_task(ObIPartitionGroup* partition, bool& is_added);
  int add_all_pg_on_start_archive_task(const int64_t incarnation, const int64_t archive_round);
  int delete_pg_archive_task(ObIPartitionGroup* partition);
  int inner_delete_pg_archive_task(const ObPGKey& pg_key);
  bool is_prepare_pg_empty();
  int revert_pg_archive_task(ObPGArchiveTask* pg_archive_task);
  int get_pg_archive_task_guard(const ObPGKey& key, ObPGArchiveTaskGuard& guard);
  int get_pg_archive_task_guard_with_status(const ObPGKey& key, ObPGArchiveTaskGuard& guard);

  int update_clog_split_progress(ObPGArchiveCLogTask* clog_task);
  int get_clog_split_info(const ObPGKey& key, const int64_t epoch, const int64_t incarnation, const int64_t round,
      uint64_t& last_split_log_id, int64_t& last_split_log_ts, int64_t& last_checkpoint_ts);
  int set_archive_round_info(const int64_t round, const int64_t incarnation);
  int set_server_start_archive_ts(const int64_t start_archive_ts);
  int mark_fatal_error(
      const ObPGKey& pg_key, const int64_t epoch_id, const int64_t incarnation, const int64_t log_archive_round);

  int check_if_task_expired(
      const ObPGKey& pg_key, const int64_t incarnation, const int64_t log_archive_round, bool& is_expired);
  int get_archive_pg_map(PGArchiveMap*& map);
  int64_t get_pg_count()
  {
    return pg_map_.count();
  }

private:
  void do_thread_task_();
  bool need_dispatch_pg_();
  bool need_confirm_pg_();
  bool need_check_start_archive_();
  void do_dispatch_pg_();
  void handle_check_start_archive_round_();

  int check_pg_task_exist_(const ObPGKey& pg_key, bool& pg_exist);
  int put_pg_archive_task_(const ObPGKey& pg_key, const int64_t epoch, const int64_t takeover_ts,
      const int64_t timestamp, const bool is_add);
  int handle_add_task_(
      const ObPGKey& pg_key, const int64_t timestamp, const int64_t leader_epoch, const int64_t takeover_ts);
  int check_active_pg_archive_task_exist_(const ObPGKey& pg_key, const int64_t leader_epoch, bool& pg_exist);
  bool get_and_check_compatible_(bool& compatible);
  int record_for_residual_data_file_(StartArchiveHelper& helper, ObPGArchiveTask* task);
  int handle_gc_task_(const ObPGKey& pg_key);
  int insert_or_update_pg_(StartArchiveHelper& helper, ObPGArchiveTask*& task);
  int remove_pg_(const ObPGKey& pg_key);
  int signal_ilog_fetcher_min_ilog_id_(const file_id_t min_ilog_file_id);

  int add_pg_to_ilog_fetch_queue_(StartArchiveHelper& helper);
  int generate_and_submit_first_log_(StartArchiveHelper& helper);
  void notify_start_archive_round_succ_();

  int reconfirm_pg_delete_();
  int reconfirm_pg_add_();

  bool is_pre_task_empty_();
  int pop_pre_task_(ObLink*& link);
  int push_pre_task_(const ObPGKey& pg_key, ObLink* link);
  int64_t thread_index_();

private:
  class CheckDeletePGFunctor;
  class CheckArchiveRoundStartFunctor;

private:
  bool inited_;
  int64_t thread_counter_;
  int64_t log_archive_round_;
  int64_t incarnation_;
  int64_t start_archive_ts_;

  int64_t last_reconfirm_pg_tstamp_;
  PGArchiveMap pg_map_;
  int64_t thread_num_;
  int64_t queue_num_;
  ObSpLinkQueue pre_pg_queue_[PG_MGR_QUEUE_SIZE];

  ObArchiveAllocator* allocator_;
  ObArchiveLogWrapper* log_wrapper_;
  ObArchiveRoundMgr* archive_round_mgr_;
  ObArchiveMgr* archive_mgr_;
  ObPartitionService* partition_service_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObArchivePGMgr);
};

class PreArchiveLinkedPGKey : public common::ObLink {
public:
  PreArchiveLinkedPGKey()
  {
    reset();
  }
  PreArchiveLinkedPGKey(const ObPGKey& pg_key, const int64_t epoch, const int64_t takeover_ts,
      const int64_t create_timestamp, const bool is_add);
  ~PreArchiveLinkedPGKey()
  {
    reset();
  }
  void reset();
  TO_STRING_KV(K(pg_key_), K(type_), K(epoch_), K(takeover_ts_), K(create_timestamp_), K(retry_times_));

public:
  ObPGKey pg_key_;
  bool type_;  // add task true, del task false
  int64_t epoch_;
  int64_t takeover_ts_;
  int64_t create_timestamp_;
  uint64_t retry_times_;

private:
  DISALLOW_COPY_AND_ASSIGN(PreArchiveLinkedPGKey);
};

}  // namespace archive
}  // namespace oceanbase

#endif /* OCEANBASE_ARCHIVE_PG_MGR_H_ */
