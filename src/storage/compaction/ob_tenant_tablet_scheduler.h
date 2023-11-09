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

#include "lib/task/ob_timer.h"
#include "lib/queue/ob_dedup_queue.h"
#include "share/ob_ls_id.h"
#include "storage/ob_i_store.h"
#include "storage/compaction/ob_tablet_merge_task.h"
#include "storage/compaction/ob_partition_merge_policy.h"
#include "storage/compaction/ob_storage_locality_cache.h"

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
class ObITable;
class ObTabletDDLKvMgr;
class ObLSTabletService;
class ObFastFreezeChecker
{
public:
  ObFastFreezeChecker();
  virtual ~ObFastFreezeChecker();
  void reset();
  OB_INLINE bool need_check() const { return enable_fast_freeze_; }
  void reload_config(const bool enable_fast_freeze);
  int check_need_fast_freeze(const storage::ObTablet &tablet, bool &need_fast_freeze);
  TO_STRING_KV(K_(enable_fast_freeze));
private:
  int check_hotspot_need_fast_freeze(const memtable::ObMemtable &memtable, bool &need_fast_freeze);
private:
  static const int64_t FAST_FREEZE_INTERVAL_US = 120 * 1000 * 1000;  //120s
  bool enable_fast_freeze_;
};

// record ls_id/tablet_id
class ObCompactionScheduleIterator
{
public:
  ObCompactionScheduleIterator(
    const bool is_major,
    ObLSGetMod mod = ObLSGetMod::STORAGE_MOD)
    : mod_(mod),
      is_major_(is_major),
      scan_finish_(false),
      merge_finish_(false),
      finish_cur_batch_(false),
      report_scn_flag_(false),
      ls_idx_(-1),
      tablet_idx_(0),
      schedule_tablet_cnt_(0),
      max_batch_tablet_cnt_(SCHEDULE_TABLET_BATCH_CNT),
      ls_tablet_svr_(nullptr),
      ls_ids_(),
      tablet_ids_()
  {
    ls_ids_.set_attr(ObMemAttr(MTL_ID(), "CompIter"));
    tablet_ids_.set_attr(ObMemAttr(MTL_ID(), "CompIter"));
  }
  ~ObCompactionScheduleIterator() { reset(); }
  int build_iter(const int64_t batch_tablet_cnt);
  int get_next_ls(ObLSHandle &ls_handle);
  int get_next_tablet(ObTabletHandle &tablet_handle);
  bool is_scan_finish() const { return scan_finish_; }
  bool tenant_merge_finish() const { return merge_finish_ & scan_finish_; }
  void update_merge_finish(const bool merge_finish) {
    merge_finish_ &= merge_finish;
  }
  void set_report_scn_flag() { report_scn_flag_ = true; }
  bool need_report_scn() const { return report_scn_flag_; }
  void reset();
  bool is_valid() const;
  void skip_cur_ls()
  {
    ++ls_idx_;
    tablet_idx_ = -1;
    ls_tablet_svr_ = nullptr;
    tablet_ids_.reuse();
  }
  void start_cur_batch()
  {
    schedule_tablet_cnt_ = 0;
    finish_cur_batch_ = false;
  }
  int64_t to_string(char *buf, const int64_t buf_len) const;
private:
  // virtual for unittest
  virtual int get_cur_ls_handle(ObLSHandle &ls_handle);
  virtual int get_tablet_ids();
  virtual int get_tablet_handle(const ObTabletID &tablet_id, ObTabletHandle &tablet_handle);

  static const int64_t LS_ID_ARRAY_CNT = 10;
  static const int64_t TABLET_ID_ARRAY_CNT = 2000;
  static const int64_t SCHEDULE_TABLET_BATCH_CNT = 50 * 1000L; // 5w
  static const int64_t CHECK_REPORT_SCN_INTERVAL = 5 * 60 * 1000 * 1000L; // 600s

  ObLSGetMod mod_;
  bool is_major_;
  bool scan_finish_;
  bool merge_finish_;
  bool finish_cur_batch_;
  bool report_scn_flag_;
  int64_t timeout_us_;
  int64_t ls_idx_;
  int64_t tablet_idx_;
  int64_t schedule_tablet_cnt_;
  int64_t max_batch_tablet_cnt_;
  storage::ObLSTabletService *ls_tablet_svr_;
  common::ObSEArray<share::ObLSID, LS_ID_ARRAY_CNT> ls_ids_;
  common::ObSEArray<ObTabletID, TABLET_ID_ARRAY_CNT> tablet_ids_;
};

struct ObProhibitScheduleMediumMap
{
public:
  enum ProhibitFlag
  {
    TRANSFER = 0,
    MEDIUM = 1,
    FLAG_MAX
  };
  static const char *ProhibitFlagStr[];
  static bool is_valid_flag(const ProhibitFlag flag)
  {
    return flag >= TRANSFER && flag < FLAG_MAX;
  }
  ObProhibitScheduleMediumMap();
  ~ObProhibitScheduleMediumMap() { destroy(); }
  int init();
  void destroy();
  int clear_flag(const share::ObLSID &ls_id, const ProhibitFlag &input_flag);
  int add_flag(const share::ObLSID &ls_id, const ProhibitFlag &input_flag);
  int64_t to_string(char *buf, const int64_t buf_len) const;
  int64_t get_transfer_flag_cnt() const;
private:
  int64_t transfer_flag_cnt_;
  mutable obsys::ObRWLock lock_;
  common::hash::ObHashMap<share::ObLSID, ProhibitFlag> ls_id_map_;
};

class ObTenantTabletScheduler
{
public:
  struct ObScheduleStatistics
  {
  public:
    ObScheduleStatistics() { reset(); }
    ~ObScheduleStatistics() {}
    OB_INLINE void reset()
    {
      add_weak_read_ts_event_flag_ = false;
      check_weak_read_ts_cnt_ = 0;
      start_timestamp_ = 0;
      clear_tablet_cnt();
    }
    OB_INLINE void clear_tablet_cnt()
    {
      schedule_cnt_ = 0;
      finish_cnt_ = 0;
      wait_rs_validate_cnt_ = 0;
    }
    OB_INLINE void start_merge()
    {
      add_weak_read_ts_event_flag_ = true;
      check_weak_read_ts_cnt_ = 0;
      start_timestamp_ = ObTimeUtility::fast_current_time();
      clear_tablet_cnt();
    }
    TO_STRING_KV(K_(schedule_cnt), K_(finish_cnt), K_(wait_rs_validate_cnt));
    bool add_weak_read_ts_event_flag_;
    int64_t check_weak_read_ts_cnt_;
    int64_t start_timestamp_;
    int64_t schedule_cnt_;
    int64_t finish_cnt_;
    int64_t wait_rs_validate_cnt_;
  };
public:
  ObTenantTabletScheduler();
  ~ObTenantTabletScheduler();
  static int mtl_init(ObTenantTabletScheduler* &scheduler);

  int init();
  int start();
  void destroy();
  void reset();
  void stop();
  void wait();
  bool is_stop() const { return is_stop_; }
  int reload_tenant_config();
  bool enable_adaptive_compaction() const { return enable_adaptive_compaction_; }
  int64_t get_error_tablet_cnt() { return ATOMIC_LOAD(&error_tablet_cnt_); }
  void clear_error_tablet_cnt() { ATOMIC_STORE(&error_tablet_cnt_, 0); }
  void update_error_tablet_cnt(const int64_t delta_cnt)
  {
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
  OB_INLINE bool could_major_merge_start() const { return major_merge_status_; }
  int check_tablet_could_schedule_by_status(const ObTablet &tablet, bool &could_schedule_merge);
  // The transfer task sets the flag that prohibits the scheduling of medium when the log stream is src_ls of transfer
  int stop_ls_schedule_medium(const share::ObLSID &ls_id);
  int clear_prohibit_medium_flag(const share::ObLSID &ls_id, const ObProhibitScheduleMediumMap::ProhibitFlag &input_flag)
  {
    return prohibit_medium_map_.clear_flag(ls_id, input_flag);
  }
  int ls_start_schedule_medium(const share::ObLSID &ls_id, bool &ls_could_schedule_medium);
  const ObProhibitScheduleMediumMap& get_prohibit_medium_ls_map() const {
    return prohibit_medium_map_;
  }
  int64_t get_frozen_version() const;
  int64_t get_merged_version() const { return merged_version_; }
  int64_t get_inner_table_merged_scn() const { return ATOMIC_LOAD(&inner_table_merged_scn_); }
  void set_inner_table_merged_scn(const int64_t merged_scn)
  {
    return ATOMIC_STORE(&inner_table_merged_scn_, merged_scn);
  }
  int64_t get_bf_queue_size() const { return bf_queue_.task_count(); }
  int schedule_merge(const int64_t broadcast_version);
  int update_upper_trans_version_and_gc_sstable();
  int check_ls_compaction_finish(const share::ObLSID &ls_id);
  int schedule_all_tablets_minor();

  int gc_info();
  int set_max();

  // Schedule an async task to build bloomfilter for the given macro block.
  // The bloomfilter build task will be ignored if a same build task exists in the queue.
  int schedule_build_bloomfilter(
      const uint64_t table_id,
      const blocksstable::MacroBlockId &macro_id,
      const int64_t prefix_len);
  static bool check_tx_table_ready(ObLS &ls, const share::SCN &check_scn);
  static int check_ls_state(ObLS &ls, bool &need_merge);
  static int check_ls_state_in_major(ObLS &ls, bool &need_merge);
  template <class T>
  static int schedule_tablet_minor_merge(
      ObLSHandle &ls_handle,
      ObTabletHandle &tablet_handle);
  static int schedule_tablet_meta_major_merge(
      ObLSHandle &ls_handle,
      ObTabletHandle &tablet_handle);
  template <class T>
  static int schedule_merge_execute_dag(
      const compaction::ObTabletMergeDagParam &param,
      ObLSHandle &ls_handle,
      ObTabletHandle &tablet_handle,
      const ObGetMergeTablesResult &result);
  static bool check_weak_read_ts_ready(
      const int64_t &merge_version,
      ObLS &ls);
  static int schedule_merge_dag(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const ObMergeType merge_type,
      const int64_t &merge_snapshot_version,
      const bool is_tenant_major_merge = false);
  static int schedule_tablet_ddl_major_merge(
      ObTabletHandle &tablet_handle);

  int get_min_dependent_schema_version(int64_t &min_schema_version);

private:
  int schedule_all_tablets_medium();
  int schedule_ls_medium_merge(
      int64_t &merge_version,
      ObLSHandle &ls_handle,
      bool &all_ls_weak_read_ts_ready);
  OB_INLINE int schedule_tablet_medium(
    ObLSHandle &ls_handle,
    ObTabletHandle &tablet_handle,
    const int64_t major_frozen_scn,
    const share::SCN &weak_read_ts,
    const bool could_major_merge,
    const bool enable_adaptive_compaction,
    const bool ls_could_schedule_medium,
    compaction::ObLSLocality ls_locality,
    int64_t &merge_version,
    bool &is_leader,
    bool &tablet_merge_finish);
  int schedule_ls_minor_merge(
      ObLSHandle &ls_handle);
  int try_remove_old_table(ObLS &ls);
  int restart_schedule_timer_task(
    const int64_t interval,
    const int64_t tg_id,
    common::ObTimerTask &timer_task);
  int update_report_scn_as_ls_leader(
      ObLS &ls);

private:
  class MergeLoopTask: public common::ObTimerTask
  {
  public:
    MergeLoopTask() = default;
    virtual ~MergeLoopTask() = default;
    virtual void runTimerTask();
  };
  class SSTableGCTask : public common::ObTimerTask
  {
  public:
    SSTableGCTask() { disable_timeout_check(); }
    virtual ~SSTableGCTask() = default;
    virtual void runTimerTask() override;
  };
  class MediumLoopTask : public common::ObTimerTask
  {
  public:
    MediumLoopTask() { disable_timeout_check(); }
    virtual ~MediumLoopTask() = default;
    virtual void runTimerTask() override;
  };
  class InfoPoolResizeTask : public common::ObTimerTask
  {
  public:
    InfoPoolResizeTask() = default;
    virtual ~InfoPoolResizeTask() = default;
    virtual void runTimerTask() override;
  };
public:
  static const int64_t INIT_COMPACTION_SCN = 1;
  typedef common::ObSEArray<ObGetMergeTablesResult, compaction::ObPartitionMergePolicy::OB_MINOR_PARALLEL_INFO_ARRAY_SIZE> MinorParallelResultArray;

private:
  static const int64_t BLOOM_FILTER_LOAD_BUILD_THREAD_CNT = 1;
  static const int64_t NO_MAJOR_MERGE_TYPE_CNT = 2;
  static const int64_t TX_TABLE_NO_MAJOR_MERGE_TYPE_CNT = 1;
  static const int64_t BF_TASK_QUEUE_SIZE = 10L * 1000;
  static const int64_t BF_TASK_MAP_SIZE = 10L * 1000;
  static const int64_t BF_TASK_TOTAL_LIMIT = 512L * 1024L * 1024L;
  static const int64_t BF_TASK_HOLD_LIMIT = 256L * 1024L * 1024L;
  static const int64_t BF_TASK_PAGE_SIZE = common::OB_MALLOC_MIDDLE_BLOCK_SIZE; //64K

  static constexpr ObMergeType MERGE_TYPES[] = {
      MINOR_MERGE, HISTORY_MINOR_MERGE};
  static const int64_t SSTABLE_GC_INTERVAL = 30 * 1000 * 1000L; // 30s
  static const int64_t INFO_POOL_RESIZE_INTERVAL = 30 * 1000 * 1000L; // 30s
  static const int64_t DEFAULT_COMPACTION_SCHEDULE_INTERVAL = 30 * 1000 * 1000L; // 30s
  static const int64_t DEFAULT_COMPACTION_SCHEDULE_BATCH_SIZE = 50 * 1000L; // 5w
  static const int64_t ADD_LOOP_EVENT_INTERVAL = 120 * 1000 * 1000L; // 120s
  static const int64_t WAIT_MEDIUM_CHECK_THRESHOLD = 10 * 60 * 1000 * 1000 * 1000L; // 10m // ns
  static const int64_t PRINT_LOG_INVERVAL = 2 * 60 * 1000 * 1000L; // 2m
  static const int64_t SCHEDULE_TABLET_BATCH_CNT = 50 * 1000L; // 5w
  static const int64_t CHECK_LS_LOCALITY_INTERVAL = 5 * 60 * 1000 * 1000L; // 5m
private:
  bool is_inited_;
  bool major_merge_status_;
  bool is_stop_;
  int merge_loop_tg_id_; // thread
  int medium_loop_tg_id_; // thread
  int sstable_gc_tg_id_; // thread
  int info_pool_resize_tg_id_;   // thread
  int64_t schedule_interval_;
  int64_t schedule_tablet_batch_size_;

  common::ObDedupQueue bf_queue_;
  mutable obsys::ObRWLock frozen_version_lock_;
  int64_t frozen_version_;
  int64_t merged_version_; // the merged major version of the local server, may be not accurate after reboot
  int64_t inner_table_merged_scn_;
  ObScheduleStatistics schedule_stats_;
  MergeLoopTask merge_loop_task_;
  MediumLoopTask medium_loop_task_;
  SSTableGCTask sstable_gc_task_;
  InfoPoolResizeTask info_pool_resize_task_;
  ObFastFreezeChecker fast_freeze_checker_;
  bool enable_adaptive_compaction_;
  ObCompactionScheduleIterator minor_ls_tablet_iter_;
  ObCompactionScheduleIterator medium_ls_tablet_iter_;
  int64_t error_tablet_cnt_; // for diagnose
  ObProhibitScheduleMediumMap prohibit_medium_map_;
  compaction::ObStorageLocalityCache ls_locality_cache_;
};

} // namespace storage
} // namespace oceanbase

#endif // STORAGE_OB_TENANT_TABLET_SCHEDULER_H_
