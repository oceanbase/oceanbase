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

#ifndef OCEANBASE_CLOG_OB_ILOG_STORE_H_
#define OCEANBASE_CLOG_OB_ILOG_STORE_H_
#include "lib/container/ob_se_array.h"
#include "lib/lock/ob_tc_rwlock.h"
#include "lib/task/ob_timer.h"
#include "common/ob_member_list.h"
#include "ob_file_id_cache.h"
#include "ob_log_common.h"
#include "ob_log_define.h"
#include "ob_ilog_memstore.h"

namespace oceanbase {
namespace storage {
class ObPartitionService;
}
namespace common {
class ObILogFileStore;
}
namespace clog {
class ObIlogMemstore;
class ObFileIdCache;
class ObLogDirectReader;
class ObILogDir;
class ObIlogFileBuilder;

class PinnedMemory {
public:
  PinnedMemory();
  ~PinnedMemory();
  int init(const int64_t size);
  void destroy();
  char* get_ptr();
  bool is_valid(const int64_t size) const;
  TO_STRING_KV(KP(pinned_memory_), K(size_));

private:
  char* pinned_memory_;
  int64_t size_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(PinnedMemory);
};

class ObIlogStore {
public:
  ObIlogStore();
  ~ObIlogStore();

public:
  int init(const file_id_t next_ilog_file_id, common::ObILogFileStore* file_store, ObFileIdCache* file_id_cache,
      ObLogDirectReader* direct_reader, storage::ObPartitionService* partition_service);
  void destroy();
  int start();
  void stop();
  void wait();

public:
  // retry inside the function
  // caller guarantees that the same partition_key is called from
  // a single thread, log_id is monotonically increasing.
  int submit_cursor(
      const common::ObPartitionKey& partition_key, const uint64_t log_id, const ObLogCursorExt& log_cursor_ext);
  int submit_cursor(const common::ObPartitionKey& partition_key, const uint64_t log_id,
      const ObLogCursorExt& log_cursor_ext, const common::ObMemberList& memberlist, const int64_t replica_num,
      const int64_t memberlist_version);
  // return value
  // 1) OB_SUCCESS, query success
  // 2) OB_ENTRY_NOT_EXIST, the log_id is not exist
  // 3) OB_ERR_OUT_OF_UPPER_BOUND, the log_id is greater than max_log_id
  //
  // support batch query
  int get_cursor_from_memstore(
      const common::ObPartitionKey& partition_key, const uint64_t log_id, ObGetCursorResult& result) const;
  // return value
  // 1) OB_SUCCESS, query success
  // 2) OB_ENTRY_NOT_EXIST, the log_id is not exist, only return this
  //    error code when file was reclaimed
  //
  // support batch query
  int get_cursor_from_ilog_file(const common::ObPartitionKey& partition_key, const uint64_t log_id,
      const Log2File& log2file_item, ObGetCursorResult& result) const;
  // return value
  // 1) OB_SUCCESS, query success
  // 2) OB_PARTITION_NOT_EXIST, partition not exist
  // query the max_log_id and max_log_ts in the partition
  int get_max_ilog_from_memstore(const common::ObPartitionKey& partition_key, uint64_t& ret_max_log_id) const;
  //
  // return value
  // 1) OB_SUCCESS, query success
  // 2) OB_PARTITION_NOT_EXIST, partition not exist
  // query the min_log_id and min_log_ts in the partition
  int get_memstore_min_log_id_and_ts(
      const common::ObPartitionKey& partition_key, uint64_t& ret_min_log_id, int64_t& ret_min_log_ts) const;

  file_id_t get_next_ilog_file_id() const
  {
    return next_ilog_file_id_;
  }
  // ilog writer worker
  void runTimerTask();

private:
  const static int64_t DEFAULT_MEMSTORE_COUNT = 16;
  const static int64_t TIMER_TASK_INTERVAL = 2 * 1000 * 1000;
  // PINNED_MEMORY_SIZE should set as ObIlogMemstore::CURSOR_SIZE_TRIGGER(32MB)
  // however, in concurrent secenarios, it will causes that the size of
  // ObIlogMemstore will exceed than ObIlogMemstore::CURSOR_SIZE_TRIGGER.
  // therefore, set PINNED_MEMORY_SIZE as double ObIlogMemstore::CURSOR_SIZE_TRIGGER
  const static int64_t PINNED_MEMORY_SIZE = 2 * ObIlogMemstore::CURSOR_SIZE_TRIGGER;
  struct FrozenMemstore;
  typedef common::ObSEArray<ObIlogMemstore*, DEFAULT_MEMSTORE_COUNT> IlogMemstoreArray;
  typedef common::ObSEArray<FrozenMemstore, DEFAULT_MEMSTORE_COUNT> FrozenMemstoreArray;

  int get_memstore_min_log_id_and_ts_unlock_(
      const common::ObPartitionKey& partition_key, uint64_t& ret_min_log_id, int64_t& ret_min_log_ts) const;
  int get_cursor_from_memstore_(
      const common::ObPartitionKey& partition_key, const uint64_t log_id, ObLogCursorExt& log_cursor_ext) const;
  int get_cursor_from_frozen_memstore_(
      const common::ObPartitionKey& partition_key, const uint64_t log_id, ObLogCursorExt& log_cursor_ext) const;
  int get_max_log_from_frozen_memstore_(const common::ObPartitionKey& partition_key, uint64_t& ret_max_log_id) const;
  int get_min_log_from_frozen_memstore_(
      const common::ObPartitionKey& partition_key, uint64_t& ret_min_log_id, int64_t& ret_min_log_ts) const;
  int update_max_flushed_ilog_id_(IndexInfoBlockMap& index_info_block_map);

  int write_file_(const file_id_t next_ilog_file_id, char* buffer, const int64_t buffer_size);

  int froze_memstore_(const ObIlogFreezeTriggerType& trigger_type);
  int raise_memstore_();
  void free_memstore_(IlogMemstoreArray& free_memstore_array);
  void free_memstore_(ObIlogMemstore* memstore);
  void free_memstore_();
  void timer_check_need_freeze_();

  // this function return the range of ObIlogMemstore which can be merged
  // end_idx is the last index of ObIlogMemstore which can be merged
  // is_ilog_not_continous_trigger means that the next ObIlogMemstore after
  // end_idx is whether trigger by OB_ILOG_NOT_CONTINOUS.
  int get_merge_range_(int64_t& end_idx, bool& is_ilog_not_continous_trigger);

  // merge all ObIlogMemstore in [0, end_idx] to merge_after_memstore
  // end_idx is the last index of frozen_memstore_array_
  // memstore_after_merge is the container to merge forzen_memstore_array_
  int merge_frozen_memstore_(int64_t& end_idx, FrozenMemstore& memstore_after_merge);
  int build_and_write_file_(ObIlogFileBuilder& builder);
  int check_need_dump_(bool& need_dump, int64_t curr_memstore_seq);

  // according to trigger_type, execute different operation
  // memstore_after_merge is the memstore after merge
  // end_idx means that the last index of frozen_memstore_array_
  // free_memstore_array_ records the memstore need to release memory
  // builder is used to write memstore_after_merge into file
  // 1) if trigger_type is OB_TIMER_TRIGGER_TYPE or OB_CLOG_SIZE_TRIGGER_TYPE,
  //    don't need apeend info_block into file_id_cache, free_memstore_array records the
  //    memstore located in [0, end_idx).
  // 2) if trigger_type is OB_ILOG_NOT_CONTINOUS_TYPE or OB_MERGE_NEED_SWITCH_FILE_TRIGGER_TYPE,
  //    need append info_block into file_id_cache and swith ilog file, free_memstore_array
  //    records the memstore located in [0, end_idx]
  int handle_different_trigger_type_(const FrozenMemstore& memstore_after_merge, const int64_t& end_idx,
      IlogMemstoreArray& free_memstore_array, ObIlogFileBuilder& builder);

  // according to trigger_type, determines whether need to do merge
  // 1. if call this function before merge, can determine whether need to do merge
  // 2. if call this function after merge, can determine where need to switch file
  //    after doing merge
  bool need_merge_frozen_memstore_array_by_trigger_type_(const ObIlogFreezeTriggerType& trigger_type) const;

  int do_merge_frozen_memstore_(const FrozenMemstoreArray& frozen_memstore_array, bool is_ilog_not_continous_trigger,
      FrozenMemstore& memstore_after_merge);

  void alloc_memstore_(ObIlogMemstore*& memstore);

private:
  class IlogWriterTask : public common::ObTimerTask {
  public:
    IlogWriterTask() : ilog_store_(NULL)
    {}
    ~IlogWriterTask()
    {}

  public:
    int init(ObIlogStore* ilog_store);
    virtual void runTimerTask();

  private:
    ObIlogStore* ilog_store_;
  };

  class MergeIlogMemstoreContainer;

  struct FrozenMemstore {
    FrozenMemstore() : trigger_type_(OB_INVALID_TRIGGER_TYPE), memstore_(NULL), seq_(-1)
    {}
    FrozenMemstore(const ObIlogFreezeTriggerType& trigger_type, ObIlogMemstore* memstore, int64_t seq)
        : trigger_type_(trigger_type), memstore_(memstore), seq_(seq)
    {}
    bool is_valid() const
    {
      return memstore_ != NULL && trigger_type_ != OB_INVALID_TRIGGER_TYPE && seq_ != -1;
    }
    int set_frozen_memstore(ObIlogFreezeTriggerType trigger_type, ObIlogMemstore* memstore, int64_t seq)
    {
      int ret = common::OB_SUCCESS;
      if (is_valid()) {
        ret = common::OB_ERR_UNEXPECTED;
        CLOG_LOG(ERROR, "want to set valid frozen memstore", K(*this), K(trigger_type), K(memstore), K(seq));
      } else {
        trigger_type_ = trigger_type;
        memstore_ = memstore;
        seq_ = seq;
      }
      return ret;
    }
    ObIlogFreezeTriggerType trigger_type_;
    ObIlogMemstore* memstore_;
    int64_t seq_;
    TO_STRING_KV(K(trigger_type_), K(memstore_), KP(memstore_), K(seq_));
  };

  class UpdateMaxFlushedIlogFunctor;
  typedef common::RWLock RWLock;
  typedef RWLock::RLockGuard RLockGuard;
  typedef RWLock::WLockGuard WLockGuard;

private:
  mutable RWLock lock_;
  bool is_inited_;
  file_id_t next_ilog_file_id_;
  common::ObILogFileStore* file_store_;
  ObFileIdCache* file_id_cache_;
  ObLogDirectReader* direct_reader_;
  storage::ObPartitionService* partition_service_;
  ObIlogMemstore* active_memstore_;
  FrozenMemstoreArray frozen_memstore_array_;
  int64_t memstore_cnt_;
  // active ObIlogMemstore's sequence number
  int64_t curr_memstore_seq_;
  // the sequence number corresponding to the last
  // memstore that has been merged
  int64_t prev_consumed_memstore_seq_;
  PinnedMemory pinned_memory_;
  IlogWriterTask task_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObIlogStore);
};
}  // namespace clog
}  // namespace oceanbase

#endif  // OCEANBASE_CLOG_OB_ILOG_STORE_H_
