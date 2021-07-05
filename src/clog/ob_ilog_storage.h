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

#ifndef OCEANBASE_CLOG_OB_ILOG_STORAGE_H_
#define OCEANBASE_CLOG_OB_ILOG_STORAGE_H_

#include "lib/task/ob_timer.h"
#include "ob_file_id_cache.h"
#include "ob_ilog_cache.h"
#include "ob_ilog_store.h"
#include "ob_log_cache.h"
#include "ob_log_define.h"
#include "ob_log_direct_reader.h"
#include "ob_log_file_pool.h"
#include "ob_log_reader_interface.h"
#include "ob_raw_entry_iterator.h"

namespace oceanbase {
namespace storage {
class ObPartitionService;
}
namespace common {
class ObILogFileStore;
}
namespace clog {
class ObCommitLogEnv;
class ObIlogAccessor {
public:
  ObIlogAccessor();
  virtual ~ObIlogAccessor();
  virtual void destroy();

public:
  int init(const char* dir_name, const char* shm_path, const int64_t server_seq, const common::ObAddr& addr,
      ObLogCache* log_cache);

  int add_partition_needed_to_file_id_cache(
      const common::ObPartitionKey& partition_key, const uint64_t last_replay_log_id);
  // NOTE: this function just only be called once when observer
  // starts.
  int fill_file_id_cache();
  // NOTE: This function doesn't support read ilog file which version is smaller
  // than 2.1.0, meanwhile, it's only used to read last ilog file.
  int get_cursor_from_ilog_file(const common::ObAddr& addr, const int64_t seq,
      const common::ObPartitionKey& partition_key, const uint64_t query_log_id, const Log2File& item,
      ObLogCursorExt& log_cursor_ext);
  // NOTE: This function doesn't support read ilog file which version is smaller
  // than 2.1.0.
  int get_cursor_batch_for_fast_recovery(const common::ObAddr& addr, const int64_t seq,
      const common::ObPartitionKey& partition_key, const uint64_t query_log_id, ObGetCursorResult& result);
  ObILogFileStore* get_log_file_store() const
  {
    return file_store_;
  }
  int query_max_ilog_from_file_id_cache(const common::ObPartitionKey& partition_key, uint64_t& ret_max_ilog_id);

  int ensure_log_continuous_in_file_id_cache(const ObPartitionKey& pkey, const uint64_t log_id);
  int check_is_clog_obsoleted(
      const ObPartitionKey& pkey, const file_id_t file_id, const offset_t offset, bool& is_obsoleted) const;
  int check_partition_ilog_can_be_purged(const ObPartitionKey& pkey, const int64_t max_decided_trans_version,
      const uint64_t item_max_log_id, const int64_t item_max_log_ts, bool& can_purge);

protected:
  int handle_last_ilog_file_(const file_id_t file_id);
  int fill_file_id_cache_(const file_id_t file_id);
  int get_index_info_block_map_(
      const file_id_t file_id, IndexInfoBlockMap& index_info_block_map, const bool update_old_version_max_file_id);
  int write_old_version_info_block_and_trailer_(
      const file_id_t file_id, const offset_t offset, ObIndexInfoBlockHandler& info_block_handler);
  int write_old_version_info_block_(
      const file_id_t file_id, const offset_t offset, ObIndexInfoBlockHandler& info_block_handler);
  int write_old_version_trailer_(const file_id_t file_id, const offset_t offset);
  bool is_new_version_ilog_file_(const file_id_t file_id) const;

protected:
  ObILogFileStore* file_store_;
  ObFileIdCache file_id_cache_;
  ObAlignedBuffer buffer_;
  // Don't need to init or destory
  ObTailCursor log_tail_;
  ObLogDirectReader direct_reader_;
  file_id_t old_version_max_file_id_;

private:
  bool inited_;
};

class ObIlogStorage : public ObIlogAccessor {
public:
  ObIlogStorage();
  ~ObIlogStorage();

public:
  int init(const char* dir_name, const char* shm_path, const int64_t server_seq, const common::ObAddr& addr,
      ObLogCache* log_cache, storage::ObPartitionService* partition_service, ObCommitLogEnv* commit_log_env);
  void destroy();
  int start();
  void stop();
  void wait();

public:
  // Return value:
  // 1) OB_SUCCESS
  // 2) OB_ERR_OUT_OF_UPPER_BOUND
  // 3) OB_CURSOR_NOT_EXIST
  // 4) OB_NEED_RETRY, query log for old version, if ilog file hasn't beed loadm, need caller retry
  int get_cursor_batch(
      const common::ObPartitionKey& partition_key, const uint64_t query_log_id, ObGetCursorResult& result);
  int get_cursor_batch_from_file(
      const common::ObPartitionKey& partition_key, const uint64_t query_log_id, ObGetCursorResult& result);
  // Return value:
  // 1) OB_SUCCESS
  // 2) OB_ERR_OUT_OF_UPPER_BOUND
  // 3) OB_CURSOR_NOT_EXIST
  // 4) OB_NEED_RETRY, query log for old version, if ilog file hasn't beed loadm, need caller retry
  int get_cursor(
      const common::ObPartitionKey& partition_key, const uint64_t query_log_id, ObLogCursorExt& log_cursor_ext);
  int submit_cursor(
      const common::ObPartitionKey& partition_key, const uint64_t log_id, const ObLogCursorExt& log_cursor_ext);
  int submit_cursor(const common::ObPartitionKey& partition_key, const uint64_t log_id,
      const ObLogCursorExt& log_cursor_ext, const common::ObMemberList& memberlist, const int64_t replica_num,
      const int64_t memberlist_version);
  // Return value
  // 1) OB_SUCCESS
  // 2) OB_PARTITION_NOT_EXIST
  int query_max_ilog_id(const common::ObPartitionKey& partition_key, uint64_t& ret_max_ilog_id);
  // Return value
  // 1) OB_SUCCESS
  // 2) OB_PARTITION_NOT_EXIST
  int query_max_flushed_ilog_id(const common::ObPartitionKey& partition_key, uint64_t& ret_max_ilog_id);
  // Return value
  // 1) OB_SUCCESS
  // 2) OB_PARTITION_NOT_EXIST
  int get_ilog_memstore_min_log_id_and_ts(
      const common::ObPartitionKey& partition_key, uint64_t& min_log_id, int64_t& min_log_ts) const;
  // Return value
  // 1) OB_SUCCESS
  // 2) OB_ENTRY_NOT_EXIST
  int get_file_id_range(file_id_t& min_file_id, file_id_t& max_file_id) const;

  int locate_by_timestamp(const common::ObPartitionKey& partition_key, const int64_t start_ts, uint64_t& target_log_id,
      int64_t& target_log_timestamp);
  int locate_ilog_file_by_log_id(
      const common::ObPartitionKey& pkey, const uint64_t start_log_id, uint64_t& end_log_id, file_id_t& ilog_id);
  int wash_ilog_cache();
  int purge_stale_file();
  int purge_stale_ilog_index();
  // for ObIlogPerFileCacheBuilder
  ObIRawIndexIterator* alloc_raw_index_iterator(
      const file_id_t start_file_id, const file_id_t end_file_id, const offset_t offset);
  void revert_raw_index_iterator(ObIRawIndexIterator* iter);
  // for ObLogExternalExecutorWithBreakpoint
  int get_index_info_block_map(const file_id_t file_id, IndexInfoBlockMap& index_info_block_map);
  int get_used_disk_space(int64_t& used_space) const;

  int get_next_ilog_file_id_from_memory(file_id_t& next_ilog_file_id) const;

private:
  class PurgeCheckFunctor;
  class ObIlogStorageTimerTask : public common::ObTimerTask {
  public:
    ObIlogStorageTimerTask() : ilog_storage_(NULL)
    {}
    ~ObIlogStorageTimerTask()
    {}

  public:
    int init(ObIlogStorage* ilog_storage);
    virtual void runTimerTask();

  private:
    void wash_ilog_cache_();
    void purge_stale_file_();
    void purge_stale_ilog_index_();

  private:
    ObIlogStorage* ilog_storage_;
  };
  static const int64_t TIMER_TASK_INTERVAL = 20L * 1000L * 1000L;

private:
  int init_next_ilog_file_id_(file_id_t& next_ilog_file_id) const;
  int get_cursor_from_memstore_(
      const common::ObPartitionKey& partition_key, const uint64_t query_log_id, ObGetCursorResult& result);
  int get_cursor_from_file_(
      const common::ObPartitionKey& partition_key, const uint64_t query_log_id, ObGetCursorResult& result);
  int get_cursor_from_ilog_file_(const common::ObPartitionKey& partition_key, const uint64_t query_log_id,
      const Log2File& log2file_item, ObGetCursorResult& result);
  int get_cursor_from_ilog_cache_(const common::ObPartitionKey& partition_key, const uint64_t query_log_id,
      const Log2File& log2file_item, ObGetCursorResult& result);
  int query_max_ilog_from_memstore_(const common::ObPartitionKey& partition_key, uint64_t& ret_max_ilog_id);
  int handle_locate_between_files_(
      const Log2File& prev_item, const Log2File& next_item, uint64_t& target_log_id, int64_t& target_log_timestamp);

  int check_ilog_format_version_(const file_id_t file_id, bool& is_new_version, offset_t& info_block_start_offset);
  int purge_stale_file_(const file_id_t file_id, const int64_t max_decided_trans_version, bool& can_purge);
  int check_modify_time_for_purge_(const file_id_t file_id, bool& can_purge);
  int get_ilog_file_mtime_(const file_id_t file_id, time_t& ilog_mtime);
  int get_min_clog_file_mtime_(time_t& min_clog_mtime);
  int do_purge_stale_ilog_file_(const file_id_t file_id);
  int do_purge_stale_ilog_index_(
      const int64_t max_decided_trans_version, const int64_t can_purge_ilog_index_min_timestamp);
  int prepare_cursor_result_for_locate_(
      const common::ObPartitionKey& partition_key, const Log2File& item, ObGetCursorResult& result);
  int search_cursor_result_for_locate_(
      const ObGetCursorResult& result, const int64_t start_ts, const ObLogCursorExt*& target_cursor) const;

private:
  bool is_inited_;
  storage::ObPartitionService* partition_service_;
  ObCommitLogEnv* commit_log_env_;
  ObIlogStore ilog_store_;
  ObIlogPerFileCacheBuilder pf_cache_builder_;
  ObIlogCache ilog_cache_;
  ObIlogStorageTimerTask task_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObIlogStorage);
};
}  // namespace clog
}  // namespace oceanbase
#endif  // OCEANBASE_CLOG_OB_CURSOR_CACHE_H_
