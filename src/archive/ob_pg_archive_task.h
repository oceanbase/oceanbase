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

#ifndef OCEANBASE_ARCHIVE_OB_PG_ARCHIVE_TASK_H_
#define OCEANBASE_ARCHIVE_OB_PG_ARCHIVE_TASK_H_

#include "common/ob_partition_key.h"  // ObPartitionKey
#include "ob_log_archive_struct.h"
#include "lib/atomic/ob_atomic.h"      // ATOMIC_LOAD
#include "lib/lock/ob_spin_rwlock.h"   // RWLock
#include "lib/hash/ob_link_hashmap.h"  // LinkHashNode
#include "ob_archive_destination_mgr.h"
#include "ob_archive_path.h"
#include "ob_archive_task_queue.h"

namespace oceanbase {
namespace observer {
struct PGLogArchiveStat;
}
namespace archive {
using namespace oceanbase::common;
class ObArchiveAllocator;
class ObArchivePGMgr;
class StartArchiveHelper;
class ObArchiveThreadPool;
typedef common::LinkHashValue<ObPGKey> PGArchiveTaskValue;
class ObPGArchiveTask : public PGArchiveTaskValue {
public:
  ObPGArchiveTask();
  ~ObPGArchiveTask();

  typedef common::SpinRWLock RWLock;
  typedef common::SpinRLockGuard RLockGuard;
  typedef common::SpinWLockGuard WLockGuard;

public:
  int init(StartArchiveHelper& helper, ObArchiveAllocator* allocator);

public:
  ObPGKey get_pg_key()
  {
    return pg_key_;
  }
  void get_pg_log_archive_status(clog::ObPGLogArchiveStatus& status, int64_t& epoch);
  int64_t get_pg_leader_epoch()
  {
    return epoch_;
  }
  int64_t get_pg_incarnation()
  {
    return incarnation_;
  }
  int64_t get_pg_archive_round()
  {
    return archive_round_;
  }
  bool is_pg_mark_delete()
  {
    return pg_been_deleted_;
  }
  bool check_upload_first_log_succ()
  {
    return is_first_record_finish_;
  }
  void update_pg_archive_task_on_new_start(StartArchiveHelper& helper);

  int submit_send_task(ObArchiveSendTask* task);
  int submit_clog_task(ObPGArchiveCLogTask* task);

  int get_last_split_log_info(const int64_t epoch, const int64_t incarnation, const int64_t round,
      uint64_t& last_split_log_id, int64_t& last_split_log_ts, int64_t& last_split_checkpoint_ts);
  int get_max_archived_info(const int64_t epoch, const int64_t incarnation, const int64_t round, uint64_t& max_log_id,
      int64_t& max_log_ts, int64_t& checkpoint_ts, int64_t& clog_epoch_id, int64_t& accum_checksum);
  int update_pg_archive_progress(const bool need_update_log_ts, const int64_t epoch, const int64_t incarnation,
      const int64_t archive_round, const uint64_t archived_log_id, const int64_t tstamp, const int64_t checkpoint_ts,
      const int64_t clog_epoch_id, const int64_t accum_checksum);
  int update_pg_archive_checkpoint_ts(const int64_t epoch, const int64_t incarnation, const int64_t archive_round,
      const int64_t log_submit_ts, const int64_t checkpoint_ts);
  int get_fetcher_max_split_log_id(
      const int64_t epoch, const int64_t incarnation, const int64_t round, uint64_t& fetcher_max_split_id);
  int update_last_split_log_info(const bool need_update_log_ts, const int64_t epoch_id, const int64_t incarnation,
      const int64_t log_archive_round, const uint64_t log_id, const int64_t log_submit_ts, const int64_t checkpoint_ts);
  int set_encount_fatal_error(const int64_t epoch, const int64_t incarnation, const int64_t round);

  void mark_pg_archive_task_del(const int64_t epoch, const int64_t incarnation, const int64_t archive_round);
  int mark_pg_first_record_finish(const int64_t epoch, const int64_t incarnation, const int64_t archive_round);
  int update_max_split_log_id(const int64_t epoch, const int64_t incarnation, const int64_t round,
      const uint64_t log_id, const file_id_t ilog_id);
  int update_max_log_id(const int64_t epoch, const int64_t incarnation, const int64_t round, const uint64_t max_log_id);
  int get_current_file_info(const int64_t epoch, const int64_t incarnation, const int64_t round,
      const LogArchiveFileType type, bool& compatible, int64_t& offset, const int64_t path_len, char* file_path);
  int get_current_file_offset(const int64_t epoch, const int64_t incarnation, const int64_t round,
      const LogArchiveFileType type, int64_t& offset, bool& force_switch_flag);
  int get_current_data_file_min_log_info(const int64_t epoch, const int64_t incarnation, const int64_t round,
      uint64_t& min_log_id, int64_t& min_log_ts, bool& is_valid);
  int build_data_file_index_record(
      const int64_t epoch, const uint64_t incarnation, const uint64_t round, ObArchiveIndexFileInfo& info);
  int update_pg_archive_file_offset(const int64_t epoch, const int64_t incarnation, const int64_t round,
      const int64_t buf_size, const LogArchiveFileType type);
  int switch_archive_file(
      const int64_t epoch, const int64_t incarnation, const int64_t round, const LogArchiveFileType type);
  int set_file_force_switch(
      const int64_t epoch, const int64_t incarnation, const int64_t round, const LogArchiveFileType type);
  int set_pg_data_file_record_min_log_info(const int64_t epoch, const uint64_t incarnation, const uint64_t round,
      const uint64_t min_log_id, const int64_t min_log_ts);
  int push_send_task(ObArchiveSendTask& task, ObArchiveThreadPool& worker);
  int push_split_task(ObPGArchiveCLogTask& task, ObArchiveThreadPool& worker);
  int get_log_archive_stat(observer::PGLogArchiveStat& stat);
  int need_record_archive_key(const int64_t incarnation, const int64_t round, bool& need_record);

  void mock_init(const ObPGKey& pg_key, ObArchiveAllocator* allocator);
  int mock_push_task(ObArchiveSendTask& task, ObSpLinkQueue& queue);
  void mock_free_task_status();
  TO_STRING_KV(K(pg_been_deleted_), K(is_first_record_finish_), K(incarnation_), K(archive_round_), K(epoch_),
      K(tenant_id_), K(current_ilog_id_), K(max_log_id_), K(round_start_info_), K(start_log_id_), K(archived_log_id_),
      K(archived_log_timestamp_), K(archived_checkpoint_ts_), K(archived_clog_epoch_id_), K(archived_accum_checksum_),
      K(fetcher_max_split_log_id_), K(last_split_log_id_), K(last_split_log_submit_ts_), K(last_split_checkpoint_ts_),
      K(mandatory_), K(archive_destination_), K(pg_key_));

private:
  void destroy();
  void update_unlock_(StartArchiveHelper& helper);
  void free_task_status_();

private:
  bool pg_been_deleted_;
  bool is_first_record_finish_;
  bool has_encount_error_;
  int64_t incarnation_;
  int64_t archive_round_;
  int64_t epoch_;
  uint64_t tenant_id_;

  file_id_t current_ilog_id_;

  uint64_t max_log_id_;  // pg max log id

  ObArchiveRoundStartInfo round_start_info_;

  uint64_t start_log_id_;              // pg first log to archive in current round
  uint64_t fetcher_max_split_log_id_;  // ilog fetcher max log id
  uint64_t last_split_log_id_;
  int64_t last_split_log_submit_ts_;
  int64_t last_split_checkpoint_ts_;
  uint64_t archived_log_id_;        // archived max log id
  int64_t archived_log_timestamp_;  // archived max log ts
  int64_t archived_checkpoint_ts_;  // archived max checkpoint ts
  int64_t archived_clog_epoch_id_;
  int64_t archived_accum_checksum_;
  bool mandatory_;

  ObArchiveDestination archive_destination_;
  ObPGKey pg_key_;

  ObArchiveSendTaskStatus* send_task_queue_;
  ObArchiveCLogTaskStatus* clog_task_queue_;

  ObArchiveAllocator* allocator_;
  mutable RWLock rwlock_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObPGArchiveTask);
};

class ObPGArchiveTaskGuard final {
public:
  explicit ObPGArchiveTaskGuard(ObArchivePGMgr* pg_mgr);
  ~ObPGArchiveTaskGuard();

public:
  void set_pg_archive_task(ObPGArchiveTask* pg_archive_task);
  ObPGArchiveTask* get_pg_archive_task();

  TO_STRING_KV(KPC(pg_archive_task_));

private:
  void revert_pg_archive_task_();

private:
  ObPGArchiveTask* pg_archive_task_;
  ObArchivePGMgr* pg_mgr_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObPGArchiveTaskGuard);
};

}  // namespace archive
}  // namespace oceanbase

#endif /* OCEANBASE_ARCHIVE_OB_PG_ARCHIVE_TASK_H_ */
