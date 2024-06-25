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

#ifndef STORAGE_OB_TENANT_TABLET_SCHEDULER_H_
#define STORAGE_OB_TENANT_TABLET_SCHEDULER_H_

#include "lib/queue/ob_dedup_queue.h"
#include "share/ob_ls_id.h"
#include "share/tablet/ob_tablet_info.h"
#include "storage/ob_i_store.h"
#include "storage/compaction/ob_tablet_merge_task.h"
#include "storage/compaction/ob_partition_merge_policy.h"
#include "storage/compaction/ob_tenant_medium_checker.h"
#include "storage/ddl/ob_direct_insert_sstable_ctx_new.h"
#include "lib/hash/ob_hashset.h"
#include "storage/compaction/ob_tenant_tablet_scheduler_task_mgr.h"
#include "storage/compaction/ob_compaction_schedule_iterator.h"
#include "share/compaction/ob_schedule_batch_size_mgr.h"
#include "storage/compaction/ob_compaction_schedule_util.h"

namespace oceanbase
{
namespace blocksstable
{
class MacroBlockId;
}
namespace memtable
{
class ObMemtable;
}
namespace storage
{
class ObLS;
class ObTablet;
struct ObTabletStatKey;
}

namespace compaction
{
struct ObTabletSchedulePair;

class ObFastFreezeChecker
{
public:
  ObFastFreezeChecker();
  virtual ~ObFastFreezeChecker();
  int init();
  void reset();
  OB_INLINE bool need_check() const { return enable_fast_freeze_; }
  void reload_config(const bool enable_fast_freeze);
  int check_need_fast_freeze(const storage::ObTablet &tablet, bool &need_fast_freeze);
  TO_STRING_KV(K_(enable_fast_freeze));
private:
  void check_hotspot_need_fast_freeze(
      memtable::ObMemtable &memtable,
      bool &need_fast_freeze);
  void check_tombstone_need_fast_freeze(
      const storage::ObTablet &tablet,
      const ObTableQueuingModeCfg &queuing_cfg,
      memtable::ObMemtable &memtable,
      bool &need_fast_freeze);
  void try_update_tablet_threshold(
      const storage::ObTabletStatKey &key,
      const storage::ObMtStat &mt_stat,
      const int64_t memtable_create_timestamp,
      const ObTableQueuingModeCfg &queuing_cfg,
      int64_t &adaptive_threshold);
private:
  static const int64_t FAST_FREEZE_INTERVAL_US = 300 * 1000 * 1000L;  //300s
  static const int64_t PRINT_LOG_INVERVAL = 2 * 60 * 1000 * 1000L; // 2m
  static const int64_t TOMBSTONE_DEFAULT_ROW_COUNT = 250000;
  static const int64_t EMPTY_MVCC_ROW_COUNT = 1000;
  static const int64_t EMPTY_MVCC_ROW_PERCENTAGE = 50;
  static const int64_t TOMBSTONE_MAX_ROW_COUNT = 500000;
  static const int64_t TOMBSTONE_STEP_ROW_COUNT = 50000;
  static const int64_t FAST_FREEZE_TABLET_STAT_KEY_BUCKET_NUM = OB_MAX_LS_NUM_PER_TENANT_PER_SERVER * 1024;
  common::hash::ObHashMap<ObTabletStatKey, int64_t> store_map_;
  bool enable_fast_freeze_;
};

struct ObProhibitScheduleMediumMap
{
public:

  enum class ProhibitFlag : int32_t
  {
    TRANSFER = 0,
    MEDIUM = 1,
    FLAG_MAX
  };

  static const char *ProhibitFlagStr[];
  static bool is_valid_flag(const ProhibitFlag &flag)
  {
    return flag >= ProhibitFlag::TRANSFER && flag < ProhibitFlag::FLAG_MAX;
  }
  ObProhibitScheduleMediumMap();
  ~ObProhibitScheduleMediumMap() { destroy(); }
  int init();
  void destroy();
  int clear_flag(const ObTabletID &tablet_id, const ProhibitFlag &input_flag);
  int add_flag(const ObTabletID &tablet_id, const ProhibitFlag &input_flag);
  int batch_clear_flags(const ObIArray<ObTabletID> &tablet_ids, const ProhibitFlag &input_flag);
  int batch_add_flags(const ObIArray<ObTabletID> &tablet_ids, const ProhibitFlag &input_flag);
  int64_t to_string(char *buf, const int64_t buf_len) const;
  int64_t get_transfer_flag_cnt() const;
private:
  static const int64_t PRINT_LOG_INVERVAL = 2 * 60 * 1000 * 1000L; // 2m
  static const int64_t TABLET_ID_MAP_BUCKET_NUM = OB_MAX_LS_NUM_PER_TENANT_PER_SERVER * 1024;

  int inner_batch_check_tablets_not_prohibited_(const ObIArray<ObTabletID> &tablet_ids); // hold lock outside !!
  int inner_batch_add_tablets_prohibit_flags_(const ObIArray<ObTabletID> &tablet_ids, const ProhibitFlag &input_flag); // hold lock outside !!
  int inner_clear_flag_(const ObTabletID &tablet_id, const ProhibitFlag &input_flag); // hold lock outside !!
  int64_t transfer_flag_cnt_;
  mutable obsys::ObRWLock lock_;
  common::hash::ObHashMap<ObTabletID, ProhibitFlag> tablet_id_map_; // tablet is used for transfer of medium compaction
};

struct ObTenantTabletMediumParam
{
public:
  explicit ObTenantTabletMediumParam(const int64_t &merge_version, bool is_tombstone = false)
    : merge_version_(merge_version),
      is_tombstone_(is_tombstone),
      is_leader_(false),
      could_major_merge_(false),
      enable_adaptive_compaction_(false)
    {}
  ~ObTenantTabletMediumParam() = default;
  TO_STRING_KV(K_(merge_version), K_(is_tombstone), K_(is_leader), K_(could_major_merge), K_(enable_adaptive_compaction));
public:
  const int64_t merge_version_;
  bool is_tombstone_; // tombstone scene after mini
  bool is_leader_;
  bool could_major_merge_;
  bool enable_adaptive_compaction_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTenantTabletMediumParam);
};

class ObTenantTabletScheduler
{
public:
  ObTenantTabletScheduler();
  ~ObTenantTabletScheduler();
  static int mtl_init(ObTenantTabletScheduler* &scheduler);

  int init();
  int start();
  void destroy();
  void reset();
  void stop();
  void wait() { timer_task_mgr_.wait(); }
  bool is_stop() const { return is_stop_; }
  int reload_tenant_config();
  bool enable_adaptive_compaction() const { return enable_adaptive_compaction_; }
  bool enable_adaptive_merge_schedule() const { return enable_adaptive_merge_schedule_; }
  int64_t get_error_tablet_cnt() { return ATOMIC_LOAD(&error_tablet_cnt_); }
  void clear_error_tablet_cnt() { ATOMIC_STORE(&error_tablet_cnt_, 0); }
  void update_error_tablet_cnt(const int64_t delta_cnt)
  {
    // called when check tablet checksum error
    (void)ATOMIC_AAF(&error_tablet_cnt_, delta_cnt);
  }
  OB_INLINE bool schedule_ignore_error(const int ret)
  {
    return OB_ITER_END == ret
      || OB_STATE_NOT_MATCH == ret
      || OB_LS_NOT_EXIST == ret;
  }
  // major merge status control
  void stop_major_merge();
  void resume_major_merge();
  OB_INLINE bool could_major_merge_start() const { return ATOMIC_LOAD(&major_merge_status_); }
  OB_INLINE bool is_restore() const { return ATOMIC_LOAD(&is_restore_); }
  // The transfer task sets the flag that prohibits the scheduling of medium when the log stream is src_ls of transfer
  int stop_tablets_schedule_medium(const ObIArray<ObTabletID> &tablet_ids, const ObProhibitScheduleMediumMap::ProhibitFlag &input_flag);
  int clear_tablets_prohibit_medium_flag(const ObIArray<ObTabletID> &tablet_ids, const ObProhibitScheduleMediumMap::ProhibitFlag &input_flag);
  int clear_prohibit_medium_flag(const ObTabletID &tablet_id, const ObProhibitScheduleMediumMap::ProhibitFlag &input_flag)
  {
    return prohibit_medium_map_.clear_flag(tablet_id, input_flag);
  }
  int tablet_start_schedule_medium(const ObTabletID &tablet_id, bool &tablet_could_schedule_medium);
  const ObProhibitScheduleMediumMap& get_prohibit_medium_ls_map() const {
    return prohibit_medium_map_;
  }
  int64_t get_frozen_version() const;
  int64_t get_inner_table_merged_scn() const { return ATOMIC_LOAD(&inner_table_merged_scn_); }
  int get_min_data_version(uint64_t &min_data_version);
  void set_inner_table_merged_scn(const int64_t merged_scn)
  {
    return ATOMIC_STORE(&inner_table_merged_scn_, merged_scn);
  }
  int64_t get_bf_queue_size() const { return bf_queue_.task_count(); }
  int schedule_merge(const int64_t broadcast_version);
  int update_upper_trans_version_and_gc_sstable();
  int try_update_upper_trans_version_and_gc_sstable(ObLS &ls, ObCompactionScheduleIterator &iter);
  int check_ls_compaction_finish(const share::ObLSID &ls_id);
  int schedule_all_tablets_minor();

  int gc_info();
  int set_max();

  int refresh_tenant_status();
  // Schedule an async task to build bloomfilter for the given macro block.
  // The bloomfilter build task will be ignored if a same build task exists in the queue.
  int schedule_build_bloomfilter(
      const uint64_t table_id,
      const blocksstable::MacroBlockId &macro_id,
      const int64_t prefix_len);
  static bool check_tx_table_ready(ObLS &ls, const share::SCN &check_scn);
  static int fill_minor_compaction_param(
      const ObTabletHandle &tablet_handle,
      const ObGetMergeTablesResult &result,
      const int64_t total_sstable_cnt,
      const int64_t parallel_dag_cnt,
      const int64_t create_time,
      compaction::ObTabletMergeDagParam &param);
  template <class T>
  static int schedule_tablet_minor_merge(
      ObLSHandle &ls_handle,
      ObTabletHandle &tablet_handle);
  template <class T>
  static int schedule_tablet_minor_merge(
      const ObMergeType &merge_type,
      ObLSHandle &ls_handle,
      ObTabletHandle &tablet_handle);
  static int schedule_tablet_meta_merge(
      ObLSHandle &ls_handle,
      ObTabletHandle &tablet_handle,
      bool &has_created_dag);
  static int schedule_tablet_medium_merge(
      ObLSHandle &ls_handle,
      const ObTabletID &tablet_id,
      bool &succ_create_dag);
  template <class T>
  static int schedule_merge_execute_dag(
      const compaction::ObTabletMergeDagParam &param,
      ObLSHandle &ls_handle,
      ObTabletHandle &tablet_handle,
      const ObGetMergeTablesResult &result,
      T *&dag,
      const bool add_into_scheduler = true);
  static bool check_weak_read_ts_ready(
      const int64_t &merge_version,
      ObLS &ls);
  static int schedule_merge_dag(
      const share::ObLSID &ls_id,
      const storage::ObTablet &tablet,
      const ObMergeType merge_type,
      const int64_t &merge_snapshot_version);
  static int schedule_tablet_ddl_major_merge(
      ObLSHandle &ls_handle,
      ObTabletHandle &tablet_handle);

  int get_min_dependent_schema_version(int64_t &min_schema_version);
  int prepare_ls_medium_merge(
      ObLS &ls,
      ObTenantTabletMediumParam &param,
      bool &all_ls_weak_read_ts_ready);
  int try_schedule_tablet_medium(
      ObLS &ls,
      const share::ObLSID &ls_id,
      ObTabletHandle &tablet_handle,
      const share::SCN &weak_read_ts,
      ObTenantTabletMediumParam &param,
      const bool scheduler_called,
      bool &tablet_merge_finish,
      bool &medium_clog_submitted,
      bool &succ_create_dag,
      ObTabletSchedulePair &schedule_pair,
      ObCompactionTimeGuard &time_guard);
  int try_schedule_tablet_medium_merge(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const bool is_rebuild_column_group);
  int schedule_next_round_for_leader(
    const ObIArray<compaction::ObTabletCheckInfo> &tablet_ls_infos,
    const ObIArray<compaction::ObTabletCheckInfo> &finish_tablet_ls_infos);
  OB_INLINE int64_t get_schedule_batch_size() const { return batch_size_mgr_.get_schedule_batch_size(); }
  OB_INLINE int64_t get_checker_batch_size() const { return batch_size_mgr_.get_checker_batch_size(); }
private:
  friend struct ObTenantTabletSchedulerTaskMgr;
  int schedule_next_medium_for_leader(
    ObLS &ls,
    ObTabletHandle &tablet_handle,
    const share::SCN &weak_read_ts,
    const compaction::ObMediumCompactionInfoList *medium_info_list,
    const int64_t major_merge_version);
  int schedule_all_tablets_medium();
  int schedule_ls_medium_merge(
      const int64_t merge_version,
      ObLSHandle &ls_handle,
      bool &all_ls_weak_read_ts_ready);
  OB_INLINE int schedule_tablet_medium(
    ObLS &ls,
    ObTabletHandle &tablet_handle,
    const share::SCN &weak_read_ts,
    ObTenantTabletMediumParam &param,
    const bool tablet_could_schedule_medium,
    const bool scheduler_called,
    bool &tablet_merge_finish,
    bool &medium_clog_submitted,
    bool &succ_create_dag,
    ObTabletSchedulePair &schedule_pair,
    ObCompactionTimeGuard &time_guard);
  int after_schedule_tenant_medium(
    const int64_t merge_version,
    bool all_ls_weak_read_ts_ready);
  int update_major_progress(const int64_t merge_version);
  bool get_enable_adaptive_compaction();
  int update_tablet_report_status(
    const bool tablet_merge_finish,
    ObLS &ls,
    ObTablet &tablet);
  int schedule_ls_minor_merge(ObLSHandle &ls_handle);
  OB_INLINE int schedule_tablet_minor(
    ObLSHandle &ls_handle,
    ObTabletHandle tablet_handle,
    bool &schedule_minor_flag,
    bool &need_fast_freeze_flag);
  int update_report_scn_as_ls_leader(
      ObLS &ls);

  int get_ls_tablet_medium_list(
      const share::ObLSID &ls_id,
      const ObTabletID &tablet_id,
      common::ObArenaAllocator &allocator,
      ObLSHandle &ls_handle,
      ObTabletHandle &tablet_handle,
      const compaction::ObMediumCompactionInfoList *&medium_list,
      share::SCN &weak_read_ts);
  void report_blocking_medium(
    const bool &is_leader,
    const bool &tablet_could_schedule_medium,
    const bool &could_major_merge,
    const share::ObLSID &ls_id);
  int schedule_batch_freeze_dag(
      const share::ObLSID &ls_id,
      const common::ObIArray<compaction::ObTabletSchedulePair> &tablet_ids);
public:
  static const int64_t INIT_COMPACTION_SCN = 1;
  typedef common::ObSEArray<ObGetMergeTablesResult, compaction::ObPartitionMergePolicy::OB_MINOR_PARALLEL_INFO_ARRAY_SIZE> MinorParallelResultArray;
private:
  static const int64_t BLOOM_FILTER_LOAD_BUILD_THREAD_CNT = 1;
  static const int64_t NO_MAJOR_MERGE_TYPE_CNT = 3;
  static const int64_t TX_TABLE_NO_MAJOR_MERGE_TYPE_CNT = 1;
  static const int64_t BF_TASK_QUEUE_SIZE = 10L * 1000;
  static const int64_t BF_TASK_MAP_SIZE = 10L * 1000;
  static const int64_t BF_TASK_TOTAL_LIMIT = 512L * 1024L * 1024L;
  static const int64_t BF_TASK_HOLD_LIMIT = 256L * 1024L * 1024L;
  static const int64_t BF_TASK_PAGE_SIZE = common::OB_MALLOC_MIDDLE_BLOCK_SIZE; //64K

  static constexpr ObMergeType MERGE_TYPES[] = {MINOR_MERGE, HISTORY_MINOR_MERGE, MDS_MINOR_MERGE};
  static const int64_t ADD_LOOP_EVENT_INTERVAL = 120 * 1000 * 1000L; // 120s
  static const int64_t PRINT_LOG_INVERVAL = 2 * 60 * 1000 * 1000L; // 2m
  static const int64_t WAIT_MEDIUM_CHECK_THRESHOLD = 10 * 60 * 1000 * 1000 * 1000L; // 10m
  static const int64_t REFRESH_TENANT_STATUS_INTERVAL = 30 * 1000 * 1000L; // 30s
  static const int64_t MERGE_BACTH_FREEZE_CNT = 100L;
private:
  bool is_inited_;
  bool major_merge_status_;
  bool is_stop_;
  bool is_restore_;
  bool enable_adaptive_compaction_;
  bool enable_adaptive_merge_schedule_;
  common::ObDedupQueue bf_queue_;
  mutable obsys::ObRWLock frozen_version_lock_;
  int64_t frozen_version_;
  int64_t merged_version_; // the merged major version of the local server, may be not accurate after reboot
  int64_t inner_table_merged_scn_;
  uint64_t min_data_version_;
  ObCompactionScheduleTimeGuard time_guard_;
  ObScheduleStatistics schedule_stats_;
  ObFastFreezeChecker fast_freeze_checker_;
  ObCompactionScheduleIterator minor_ls_tablet_iter_;
  ObCompactionScheduleIterator medium_ls_tablet_iter_;
  ObCompactionScheduleIterator gc_sst_tablet_iter_;
  int64_t error_tablet_cnt_; // for diagnose
  int64_t loop_cnt_;
  ObProhibitScheduleMediumMap prohibit_medium_map_;
  ObTenantTabletSchedulerTaskMgr timer_task_mgr_;
  ObScheduleBatchSizeMgr batch_size_mgr_;
};

} // namespace compaction
} // namespace oceanbase

#endif // STORAGE_OB_TENANT_TABLET_SCHEDULER_H_
