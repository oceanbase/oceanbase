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

#ifndef OB_PARTITION_SCHEDULER_H_
#define OB_PARTITION_SCHEDULER_H_

#include "lib/task/ob_timer.h"
#include "lib/queue/ob_dedup_queue.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/lock/ob_mutex.h"
#include "storage/ob_i_partition_storage.h"
#include "storage/ob_i_partition_report.h"
#include "storage/compaction/ob_partition_merge_util.h"
#include "storage/ob_partition_split_task.h"

namespace oceanbase {
namespace storage {
class ObBuildIndexDag;
class ObSSTableMergeDag;
class ObWriteCheckpointDag;
}  // namespace storage
namespace storage {
class ObBloomFilterLoadTask : public common::IObDedupTask {
public:
  ObBloomFilterLoadTask(
      const uint64_t table_id, const blocksstable::MacroBlockId& macro_id, const ObITable::TableKey& table_key);
  virtual ~ObBloomFilterLoadTask();
  virtual int64_t hash() const;
  virtual bool operator==(const IObDedupTask& other) const;
  virtual int64_t get_deep_copy_size() const;
  virtual IObDedupTask* deep_copy(char* buffer, const int64_t buf_size) const;
  virtual int64_t get_abs_expired_time() const
  {
    return 0;
  }
  virtual int process();

private:
  int load_bloom_filter();
  uint64_t table_id_;
  blocksstable::MacroBlockId macro_id_;
  ObITable::TableKey table_key_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBloomFilterLoadTask);
};

class ObBloomFilterBuildTask : public common::IObDedupTask {
public:
  ObBloomFilterBuildTask(const uint64_t table_id, const blocksstable::MacroBlockId& macro_id, const int64_t prefix_len,
      const ObITable::TableKey& table_key);
  virtual ~ObBloomFilterBuildTask();
  virtual int64_t hash() const;
  virtual bool operator==(const IObDedupTask& other) const;
  virtual int64_t get_deep_copy_size() const;
  virtual IObDedupTask* deep_copy(char* buffer, const int64_t buf_size) const;
  virtual int64_t get_abs_expired_time() const
  {
    return 0;
  }
  virtual int process();

private:
  int build_bloom_filter();
  uint64_t table_id_;
  blocksstable::MacroBlockId macro_id_;
  int64_t prefix_len_;
  ObITable::TableKey table_key_;
  ObTableAccessParam access_param_;
  ObTableAccessContext access_context_;
  common::ObArenaAllocator allocator_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBloomFilterBuildTask);
};

struct ObMergeStatEntry {
  ObMergeStatEntry();
  void reset();
  int64_t frozen_version_;
  int64_t start_time_;
  int64_t finish_time_;
};

class ObMergeStatistic {
public:
  ObMergeStatistic();
  virtual ~ObMergeStatistic();
  int notify_merge_start(const int64_t frozen_version);
  int notify_merge_finish(const int64_t frozen_version);
  int get_entry(const int64_t frozen_version, ObMergeStatEntry& entry);

private:
  int search_entry(const int64_t frozen_version, ObMergeStatEntry*& pentry);
  static const int64_t MAX_KEPT_HISTORY = 16;
  obsys::CRWLock lock_;
  ObMergeStatEntry stats_[MAX_KEPT_HISTORY];

private:
  DISALLOW_COPY_AND_ASSIGN(ObMergeStatistic);
};

class ObMinorMergeHistory {
public:
  explicit ObMinorMergeHistory(const uint64_t tenant_id);
  virtual ~ObMinorMergeHistory();
  int notify_minor_merge_start(const int64_t snapshot_version);
  int notify_minor_merge_finish(const int64_t snapshot_version);

private:
  static const int64_t MAX_MINOR_HISTORY = 16;
  lib::ObMutex mutex_;
  int64_t count_;
  uint64_t tenant_id_;
  int64_t snapshot_history_[MAX_MINOR_HISTORY];

private:
  DISALLOW_COPY_AND_ASSIGN(ObMinorMergeHistory);
};

class ObFastFreezeChecker {
public:
  explicit ObFastFreezeChecker(const int64_t tenant_id);
  virtual ~ObFastFreezeChecker();
  void reset();
  OB_INLINE bool need_check() const
  {
    return enable_fast_freeze_;
  }
  int check_hotspot_need_fast_freeze(ObIPartitionGroup& pg, bool& need_fast_freeze);
  TO_STRING_KV(K_(tenant_id), K_(enable_fast_freeze));

private:
  // the minimum schedule interval of fast freeze is 2min
  static const int64_t FAST_FREEZE_INTERVAL_US = 120 * 1000 * 1000;
  ObFastFreezeChecker() = default;

private:
  int64_t tenant_id_;
  bool enable_fast_freeze_;
};

class ObClearTransTableTask : public common::ObTimerTask {
public:
  ObClearTransTableTask();
  virtual ~ObClearTransTableTask();
  virtual void runTimerTask() override;
};

struct ObMergePriorityCompare {
  bool operator()(const memtable::ObMergePriorityInfo& linfo, const memtable::ObMergePriorityInfo& rinfo) const
  {
    return linfo.compare(rinfo);
  }
};

class ObPartitionScheduler {
  friend ObClearTransTableTask;

public:
  static ObPartitionScheduler& get_instance();
  // NOT thread safe.
  // There is a timer in the ObPartitionScheduler, which will be scheduled periodically after
  int init(ObPartitionService& partition_service, share::schema::ObMultiVersionSchemaService& schema_service,
      ObIPartitionReport& report);
  // NOT Thread safe.
  // It should be invoked before the destroy of partition_service and report
  void destroy();
  // Thread safe.
  // It will schedule an async task to generate merge tasks for all partitions.
  int schedule_merge(const int64_t frozen_version);
  // Thread safe.
  // It will schedule an async task to merge the given partition. The merge task will be ignored
  // if there has been the same merge task in the queue.
  //@param partition_key: the key of the partition
  int schedule_merge(const common::ObPartitionKey& partition_key, bool& is_merged);

  // Thread safe.
  // It will schedule an async task to build bloomfilter for the given macro block. The bloomfilter build
  // task will be ignored if there has been the same build task in the queue.
  int schedule_build_bloomfilter(const uint64_t table_id, const blocksstable::MacroBlockId macro_id,
      const int64_t prefix_len, const ObITable::TableKey& table_key);
  int schedule_load_bloomfilter(
      const uint64_t table_id, const blocksstable::MacroBlockId& macro_id, const ObITable::TableKey& table_key);
  // return bloomfilter task queue size
  int64_t get_bf_queue_size() const
  {
    return bf_queue_.task_count();
  };
  // Return the current frozen version.
  int64_t get_frozen_version() const;
  int64_t get_merged_version() const;
  int64_t get_merged_schema_version(const uint64_t tenant_id) const;
  int check_index_compact_to_latest(const common::ObPartitionKey& pkey, const uint64_t index_id, bool& is_finished);
  void set_fast_retry_interval(const int64_t failure_fast_retry_interval_us);
  inline void stop_merge();
  inline void resume_merge();
  inline bool could_merge_start();
  inline int get_merge_stat(const int64_t version, ObMergeStatEntry& entry);
  int reload_config();
  ObIPartitionReport* get_partition_report()
  {
    return report_;
  }
  int schedule_pg_mini_merge(const ObPartitionKey& pg_key);
  int schedule_pg(const ObMergeType merge_type, ObIPartitionGroup& partition, const common::ObVersion& merge_version,
      bool& is_merged);
  int schedule_partition();
  int notify_minor_merge_start(const uint64_t tenant_id, const int64_t snapshot_version);
  int notify_minor_merge_finish(const uint64_t tenant_id, const int64_t snapshot_version);
  int schedule_major_merge();
  void stop();
  void wait();
  int reload_minor_merge_schedule_interval();
  int clear_unused_trans_status();
  OB_INLINE void enable_auto_checkpoint()
  {
    write_ckpt_task_.is_enable_ = true;
  }
  OB_INLINE void disable_auto_checkpoint()
  {
    write_ckpt_task_.is_enable_ = false;
  }
  int64_t get_min_schema_version(const int64_t tenant_id);

private:
  class MergeRetryTask : public common::ObTimerTask {
  public:
    MergeRetryTask();
    virtual ~MergeRetryTask();
    virtual void runTimerTask();
  };
  class WriteCheckpointTask : public common::ObTimerTask {
  public:
    WriteCheckpointTask() : is_enable_(false)
    {}
    virtual ~WriteCheckpointTask()
    {
      is_enable_ = false;
    };
    virtual void runTimerTask();

    // no concurrent set, become TRUE only during observer restart
    bool is_enable_;
  };
  class MinorMergeScanTask : public common::ObTimerTask {
  public:
    MinorMergeScanTask();
    virtual ~MinorMergeScanTask() override;
    virtual void runTimerTask() override;
  };
  class MergeCheckTask : public common::ObTimerTask {
  public:
    MergeCheckTask() = default;
    virtual ~MergeCheckTask() = default;
    virtual void runTimerTask() override;
  };
  class UpdateCacheInfoTask : public common::ObTimerTask {
  public:
    UpdateCacheInfoTask() = default;
    virtual ~UpdateCacheInfoTask() = default;
    virtual void runTimerTask() override;
  };

private:
  friend class ObBuildIndexDag;
  static const int64_t DEFAULT_FAILURE_FAST_RETRY_INTERVAL_US = 1000L * 1000L * 60L;
  static const int64_t DEFAULT_WRITE_CHECKPOINT_INTERVAL_US = 1000L * 1000L * 60L;
  static const int64_t DEFAULT_MAJOR_MERGE_RETRY_INTERVAL_US = 1000L * 1000L * 30L;
  static const int64_t DEFAULT_MINOR_MERGE_SCAN_INTERVAL_US = 1000L * 1000L * 30L;
  static const int64_t DEFAULT_CLEAR_UNUSED_TRANS_INTERVAL_US = 1000L * 1000L * 30L;
  static const int64_t DEFAULT_THREAD_DEAD_THRESHOLD = 1000L * 1000L * 3600L;
  static const int64_t BLOOM_FILTER_BUILD_THREAD_CNT = 1;
  static const int32_t MERGE_THREAD_CNT_PERCENT = 30;       // percent of merge_thread_cnt in max_cpu_thread_cnt
  static const int32_t DEFAULT_MERGE_WORK_THREAD_NUM = 10;  // the default num of merge work thread
  static const int64_t MAX_MERGE_WORK_THREAD_NUM = 64;
  static const int64_t PS_TASK_QUEUE_SIZE = 10000;
  static const int64_t PS_TASK_MAP_SIZE = 10000;
  static const int64_t DEFAULT_MERGE_CHECK_INTERVAL_US = 1000L * 1000L * 10L;
  static const int64_t DEFAULT_MIN_SCHEMA_VERSION_UPDATE_INTERVAL_US = 1000L * 1000L * 60L * 5L;  // 5 mins
  typedef common::hash::ObHashMap<uint64_t, int64_t, common::hash::NoPthreadDefendMode>
      TenantMinSSTableSchemaVersionMap;
  typedef common::hash::ObHashMap<uint64_t, int64_t, common::hash::NoPthreadDefendMode>::iterator
      TenantMinSSTableSchemaVersionMapIterator;
  ObPartitionScheduler();
  virtual ~ObPartitionScheduler();

  int alloc_merge_dag(const ObMergeType merge_type, ObSSTableMergeDag*& dag);
  int alloc_split_dag(ObSSTableSplitDag*& dag);
  int alloc_write_checkpoint_dag(ObWriteCheckpointDag*& dag);
  int schedule_merge_sstable_dag(const ObIPartitionGroup& pg, const ObMergeType merge_type,
      const common::ObVersion& frozen_version, const common::ObPartitionKey& pkey, const uint64_t index_id);
  int schedule_split_sstable_dag(const bool is_major_split, const common::ObVersion& split_version,
      const common::ObPartitionKey& src_pkey, const common::ObPartitionKey& dest_pkey,
      const common::ObPartitionKey& dest_pg_key, const uint64_t index_id);

  int schedule_all_partitions(bool& merge_finished, common::ObVersion& frozen_version);
  int check_all_partitions(bool& check_finished, common::ObVersion& frozen_version);
  int schedule_minor_merge_all();
  int sort_for_minor_merge(
      common::ObArray<memtable::ObMergePriorityInfo>& merge_priority_infos, ObIPartitionArrayGuard& partitions);
  int merge_all();
  int check_all();
  int32_t get_max_merge_thread_cnt();
  int32_t get_default_merge_thread_cnt();
  int get_all_dependent_tables(share::schema::ObSchemaGetterGuard& schema_guard, ObPGPartitionArrayGuard& partitions,
      common::hash::ObHashSet<uint64_t>& dependent_table_set);

  int schedule_partition_split(ObPGPartition& partition, const ObPartitionKey& dest_pg_key);
  int schedule_partition_split(const bool is_major_split, ObPGPartition& partition, const ObPartitionKey& dest_pg_key);
  int schedule_partition_merge(const ObMergeType merge_type, ObPGPartition& partition, ObIPartitionGroup& pg,
      const common::ObVersion& frozen_version, const int64_t trans_table_end_log_ts,
      const int64_t trans_table_timestamp, bool& is_merged);
  int check_restore_point(ObPGStorage& pg_storage);
  int check_backup_point(ObPGStorage& pg_storage);
  int check_partition(const common::ObVersion& frozen_version, ObIPartitionGroup* partition, bool& check_finished);
  int schedule_partition(const ObMergeType merge_type, ObPGPartition& partition, ObIPartitionGroup& pg,
      const common::ObVersion& frozen_version, const int64_t trans_table_end_log_ts,
      const int64_t trans_table_timestamp, const bool is_restore, bool& is_merged);
  int check_release_memtable();

  inline bool is_dependent_table(const common::hash::ObHashSet<uint64_t>& dependent_table_set, const uint64_t table_id)
  {
    return common::OB_HASH_EXIST == dependent_table_set.exist_refactored(table_id);
  }
  int check_need_merge_table(const uint64_t table_id, bool& need_merge);
  int get_all_pg_partitions(ObIPartitionArrayGuard& pg_arr_guard, ObPGPartitionArrayGuard& part_arr_guard);
  int write_checkpoint();
  int remove_split_dest_partition(ObIPartitionGroup* partition, bool& is_removed);
  int schedule_trans_table_merge_dag(
      const ObPartitionKey& pg_key, const int64_t end_log_ts, const int64_t trans_table_seq);
  int check_and_freeze_for_major_(const int64_t freeze_ts, ObIPartitionGroup& pg);
  OB_INLINE bool need_check_fast_freeze(const ObMergeType& merge_type, const ObPartitionKey& pg_key)
  {
    UNUSED(pg_key);
    return MINI_MERGE == merge_type;
  }
  int update_min_sstable_schema_info();
  int update_min_schema_version(
      TenantMinSSTableSchemaVersionMap& tmp_map, const int64_t table_id, const int64_t min_schema_version);
  int update_to_map(TenantMinSSTableSchemaVersionMap& tmp_map);
  int check_partition_exist(const ObPartitionKey& pkey, bool& exist);
  int can_schedule_partition(const ObMergeType merge_type, bool& can_schedule);

private:
  typedef common::hash::ObHashMap<uint64_t, ObMinorMergeHistory*, common::hash::NoPthreadDefendMode>
      MinorMergeHistoryMap;
  typedef common::hash::ObHashMap<uint64_t, int64_t, common::hash::NoPthreadDefendMode> TenantSnapshotMap;
  common::ObVersion frozen_version_;
  common::ObVersion merged_version_;  // the merged major version of the local server, may be not accurate after reboot
  common::ObDedupQueue queue_;
  common::ObDedupQueue bf_queue_;
  ObPartitionService* partition_service_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  ObIPartitionReport* report_;
  WriteCheckpointTask write_ckpt_task_;
  MergeRetryTask inner_task_;
  MergeRetryTask fast_major_task_;
  MinorMergeScanTask minor_scan_task_;
  MinorMergeScanTask minor_task_for_major_;
  bool is_major_scan_to_be_continued_;
  bool is_minor_scan_to_be_continued_;
  int64_t failure_fast_retry_interval_us_;
  int64_t minor_merge_schedule_interval_;
  ObMergeStatistic merge_statistic_;
  mutable obsys::CRWLock frozen_version_lock_;
  mutable lib::ObMutex timer_lock_;
  bool first_most_merged_;
  bool inited_;
  MergeCheckTask merge_check_task_;
  lib::ObMutex check_release_lock_;
  MinorMergeHistoryMap minor_merge_his_map_;
  TenantSnapshotMap tenant_snapshot_map_;
  TenantSnapshotMap current_tenant_snapshot_map_;
  bool is_stop_;
  ObClearTransTableTask clear_unused_trans_status_task_;
  ObVersion report_version_;
  lib::ObMutex min_sstable_schema_version_lock_;
  TenantMinSSTableSchemaVersionMap min_sstable_schema_version_map_;
  UpdateCacheInfoTask update_cache_info_task_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObPartitionScheduler);
};

/**
 * ---------------------------------------------------------Inline
 * Methods---------------------------------------------------------------
 */
inline void ObPartitionScheduler::stop_merge()
{
  compaction::ObPartitionMergeUtil::stop_merge();
}

inline void ObPartitionScheduler::resume_merge()
{
  if (!compaction::ObPartitionMergeUtil::could_merge_start()) {
    compaction::ObPartitionMergeUtil::resume_merge();
  }
}

inline bool ObPartitionScheduler::could_merge_start()
{
  return compaction::ObPartitionMergeUtil::could_merge_start();
}

inline int ObPartitionScheduler::get_merge_stat(const int64_t version, ObMergeStatEntry& entry)
{
  return merge_statistic_.get_entry(version, entry);
}

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OB_PARTITION_SCHEDULER_H_ */
