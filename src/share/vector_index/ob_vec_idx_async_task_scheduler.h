/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SHARE_VECTOR_INDEX_OB_VEC_IDX_ASYNC_TASK_SCHEDULER_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_OB_VEC_IDX_ASYNC_TASK_SCHEDULER_H_

#include "lib/task/ob_timer.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_spin_lock.h"
#include "share/ob_ls_id.h"
#include "share/vector_index/ob_plugin_vector_index_scheduler.h"
#include "share/vector_index/ob_vector_index_async_task.h"
#include "share/vector_index/ob_vector_index_i_task_executor.h"
#include "share/vector_index/ob_hybrid_vector_refresh_task.h"
#include "share/vector_index/ob_ivf_async_task_executor.h"
#include "share/vector_index/ob_vector_index_freeze_task.h"
#include "share/vector_index/ob_vector_index_merge_task.h"
#include "share/vector_index/ob_vector_mem_sync_executor.h"

namespace oceanbase
{
namespace share
{

class ObPluginVectorIndexService;

// Leader-only executor set for a single LS, owned by ObVecIdxAsyncTaskScheduler
struct ObVecIdxLeaderExecutors
{
  ObVecIdxLeaderExecutors()
    : is_inited_(false),
      need_resume_(false),
      ref_cnt_(0),
      is_removed_(false)
  {}
  bool get_need_resume() const { return ATOMIC_LOAD(&need_resume_); }
  void set_need_resume(bool val) { ATOMIC_STORE(&need_resume_, val); }
  bool test_and_clear_need_resume() { return ATOMIC_TAS(&need_resume_, false); }
  TO_STRING_KV(K_(is_inited), K_(need_resume), K_(async_task_exec), K_(ivf_task_exec),
               K_(embedding_task_exec), K_(freeze_exec), K_(merge_exec), K_(mem_sync_exec),
               K_(ref_cnt), K_(is_removed));
  bool is_inited_;
  bool need_resume_;
  int64_t ref_cnt_;
  bool is_removed_;
  ObVecAsyncTaskExecutor async_task_exec_;
  ObIvfAsyncTaskExecutor ivf_task_exec_;
  ObVecEmbeddingAsyncTaskExecutor embedding_task_exec_;
  ObVecIdxFreezeTaskExecutor freeze_exec_;
  ObVecIdxMergeTaskExecutor merge_exec_;
  ObVecMemSyncExecutor mem_sync_exec_;
};

// Follower-only executor set for a single LS, owned by ObVecIdxAsyncTaskScheduler.
struct ObVecIdxFollowerExecutors
{
  ObVecIdxFollowerExecutors()
    : is_inited_(false),
      need_resume_(false),
      ref_cnt_(0),
      is_removed_(false)
  {}
  bool get_need_resume() const { return ATOMIC_LOAD(&need_resume_); }
  void set_need_resume(bool val) { ATOMIC_STORE(&need_resume_, val); }
  bool test_and_clear_need_resume() { return ATOMIC_TAS(&need_resume_, false); }
  TO_STRING_KV(K_(is_inited), K_(need_resume), K_(ref_cnt), K_(is_removed), K_(mem_sync_exec), K_(ivf_task_exec));
  bool is_inited_;
  bool need_resume_;
  int64_t ref_cnt_;
  bool is_removed_;
  ObVecMemSyncExecutor mem_sync_exec_;
  ObIvfAsyncTaskExecutor ivf_task_exec_;
};

struct ObVecAsyncSchedDiagStat
{
  ObVecAsyncSchedDiagStat() { reset(); }
  void reset()
  {
    pop_cnt_ = 0;
    push_cnt_ = 0;
    skip_same_tablet_cnt_ = 0;
    skip_stopped_ls_cnt_ = 0;
    skip_disabled_cnt_ = 0;
    skip_ddl_conflict_cnt_ = 0;
    mem_block_cnt_ = 0;
    push_fail_cnt_ = 0;
  }
  void add(const ObVecAsyncSchedDiagStat &other)
  {
    pop_cnt_ += other.pop_cnt_;
    push_cnt_ += other.push_cnt_;
    skip_same_tablet_cnt_ += other.skip_same_tablet_cnt_;
    skip_stopped_ls_cnt_ += other.skip_stopped_ls_cnt_;
    skip_disabled_cnt_ += other.skip_disabled_cnt_;
    skip_ddl_conflict_cnt_ += other.skip_ddl_conflict_cnt_;
    mem_block_cnt_ += other.mem_block_cnt_;
    push_fail_cnt_ += other.push_fail_cnt_;
  }
  int64_t get_skip_cnt() const
  {
    return skip_same_tablet_cnt_ + skip_stopped_ls_cnt_ + skip_disabled_cnt_
        + skip_ddl_conflict_cnt_ + mem_block_cnt_;
  }
  TO_STRING_KV(K_(pop_cnt), K_(push_cnt), K_(skip_same_tablet_cnt),
               K_(skip_stopped_ls_cnt), K_(skip_disabled_cnt),
               K_(skip_ddl_conflict_cnt), K_(mem_block_cnt), K_(push_fail_cnt));

  int64_t pop_cnt_;
  int64_t push_cnt_;
  int64_t skip_same_tablet_cnt_;
  int64_t skip_stopped_ls_cnt_;
  int64_t skip_disabled_cnt_;
  int64_t skip_ddl_conflict_cnt_;
  int64_t mem_block_cnt_;
  int64_t push_fail_cnt_;
};

// Tenant-level scheduler for vector index async tasks. Uses an independent timer TG
// One instance per tenant, held by ObPluginVectorIndexService.
class ObVecIdxAsyncTaskScheduler : public common::ObTimerTask
{
public:
  ObVecIdxAsyncTaskScheduler()
    : is_inited_(false),
      is_stopped_(false),
      tenant_id_(OB_INVALID_TENANT_ID),
      schedule_tg_id_(-1),
      service_(nullptr),
      ls_executor_lock_(common::ObLatchIds::OB_VEC_IDX_ASYNC_TASK_SCHEDULER_EXECUTOR_MAP_LOCK),
      executor_allocator_(ObMemAttr(OB_SERVER_TENANT_ID, "VecIdxExecAlloc")),
      last_schedule_time_{},
      can_schedule_{},
      current_memory_config_(0),
      local_schema_version_(OB_INVALID_VERSION),
      is_tenant_cancel_done_(false),
      is_first_startup_round_done_(false),
      is_first_update_round_done_(false),
      startup_cleanup_cutoff_ts_(0),
      last_startup_cleanup_retry_ts_(0),
      startup_cleanup_total_rows_(0),
      disabled_all_(nullptr),
      disable_entries_(nullptr),
      disable_entry_count_(0),
      last_disable_list_hash_(UINT64_MAX),
      cached_can_do_work_(false),
      last_can_do_work_check_ts_(0),
      last_triggered_task_check_ts_(0),
      last_orphan_task_check_ts_(0),
      last_sched_snapshot_ts_(0),
      last_blocked_diag_ts_(0),
      last_unexpected_no_pop_diag_ts_(0),
      last_has_queued_(false),
      last_blocked_by_water_level_(false),
      last_thread_count_(0),
      last_total_queued_(0),
      last_water_level_config_hash_(UINT64_MAX),
      sched_diag_stat_()
  {}
  virtual ~ObVecIdxAsyncTaskScheduler();

  int init(uint64_t tenant_id, ObPluginVectorIndexService *service);
  int start();
  void stop();
  void destroy();

  virtual void runTimerTask() override;

  // Called by ObPluginVectorIndexLoadScheduler::safe_to_destroy to clean up
  // task contexts for a given LS before it is destroyed.
  int clear_ls_async_task_ctxs(const share::ObLSID &ls_id,
                                ObVecIndexAsyncTaskOption &task_opt,
                                const ObVecIndexTaskCtxArray &task_ctx_array);

  // Remove leader/follower executor sets for ls_id and run their destructors so that
  // ObVecITaskExecutor::ls_handle_ releases ObLS refs (SHARE_MOD). Invoked from
  // ObPluginVectorIndexLoadScheduler::stop() before LS safe_destroy.
  void remove_and_destroy_ls_executors(const share::ObLSID &ls_id);

  // Cancel all active leader tasks for the given LS. Non-blocking: sets cancel flags
  // and kills inner SQL sessions. Called from ObPluginVectorIndexLoadScheduler::inner_switch_to_follower_().
  int cancel_ls_leader_tasks(const share::ObLSID &ls_id);
  int cancel_ls_follower_tasks(const share::ObLSID &ls_id);
  bool is_task_disabled(ObVecIndexAsyncTaskType task_type, int64_t tablet_id) const
  {
    return is_task_disabled_(task_type, tablet_id);
  }

  // Mark the given LS as needing task recovery from inner table on next timer cycle.
  // Called from ObPluginVectorIndexLoadScheduler::switch_to_leader().
  void mark_ls_need_resume(const share::ObLSID &ls_id);
  // Symmetric to mark_ls_need_resume(): the new follower (this observer) was either the
  // previous leader or restarted, and may have left non-FINISH rows in the inner table
  // with exec_addr=self_addr. The next timer cycle will trigger an R1 sweep on the
  // follower load path. Called from ObPluginVectorIndexLoadScheduler::inner_switch_to_follower_().
  void mark_ls_need_resume_for_follower(const share::ObLSID &ls_id);

  static const int64_t HISTORY_SAVE_TIME_US = 7LL * 24 * 60 * 60 * 1000 * 1000; // 7 days

  TO_STRING_KV(K_(is_inited), K_(is_stopped), K_(tenant_id), K_(schedule_tg_id));

private:
  static const int64_t SCHEDULE_PERIOD_US = 1 * 1000 * 1000;         // 1s
  static const int64_t VEC_INDEX_LOAD_TIME_NORMAL_THRESHOLD = 30 * 1000 * 1000; // 30s
  static const int64_t DEFAULT_LS_EXECUTOR_MAP_SIZE = 64;
  static const int64_t DEFAULT_TABLE_ARRAY_SIZE = 200;
  static const int64_t TABLE_GENERATE_BATCH_SIZE = 200;
  static const int64_t CAN_DO_WORK_CACHE_INTERVAL_US = 30 * 1000 * 1000;       // 30s
  static const int64_t TRIGGERED_TASK_CHECK_INTERVAL_US = 5 * 1000 * 1000;     // 5s
  static const int64_t ORPHAN_TASK_CHECK_INTERVAL_US = 30 * 1000 * 1000;       // 30s
  static const int64_t ORPHAN_TASK_MIN_AGE_US = 30 * 1000 * 1000;              // 30s
  static const int64_t STARTUP_CLEANUP_BATCH_SIZE = 100L;
  static const int64_t STARTUP_CLEANUP_RETRY_INTERVAL_US = 10 * 1000 * 1000;   // 10s
  static const int64_t SCHED_DIAG_SNAPSHOT_INTERVAL_US = 60 * 1000 * 1000;     // 60s
  static const int64_t SCHED_DIAG_BLOCKED_INTERVAL_US = 10 * 1000 * 1000;      // 10s
  static const int64_t SCHED_DIAG_NO_POP_INTERVAL_US = 60 * 1000 * 1000;       // 60s

  // --- History cleanup (migrated from ObTenantVecAsyncTaskScheduler) ---
  static const int64_t HISTORY_MOVE_BATCH_SIZE = 1024L;
  static const int64_t HISTORY_MOVE_MAX_ROWS_PER_ROUND = 4096L;
  static const int64_t HISTORY_CLEAR_BATCH_SIZE = 4096L;

  void run_timer_task();
  // True if tenant is not in restore and can run vector index scheduling.
  bool check_tenant_can_schedule_();
  bool check_can_do_work();
  int check_tenant_memory();
  int check_has_vector_index(common::ObIArray<uint64_t> &vec_table_id_array);
  int check_schema_version(bool &schema_version_changed);
  int check_index_adapter_exist(ObPluginVectorIndexMgr *mgr);
  int check_need_maintenance_ls_follower(ObPluginVectorIndexMgr *mgr);
  int check_need_maintenance(ObPluginVectorIndexMgr *mgr);
  void set_need_maintenance(ObPluginVectorIndexMgr *mgr);
  int execute_adapter_maintenance_for_ls(storage::ObLS *ls,
                                         ObPluginVectorIndexMgr *mgr,
                                         const common::ObIArray<uint64_t> &vec_table_id_array,
                                         const bool is_ls_leader);
  int acquire_adapter_in_maintenance(storage::ObLS *ls,
                                     const share::ObLSID &ls_id,
                                     const int64_t table_id,
                                     const schema::ObTableSchema *table_schema,
                                     ObVecIdxSharedTableInfoMap &shared_table_info_map);
  int set_shared_table_info_in_maintenance(storage::ObLS *ls,
                                           const int64_t table_id,
                                           const schema::ObSimpleTableSchemaV2 *table_schema,
                                           ObVecIdxSharedTableInfoMap &shared_table_info_map);
  int check_and_execute_adapter_maintenance_tasks();
  int check_is_vector_index_table(
      const ObSimpleTableSchemaV2 &table_schema, bool &is_vector_index_table, bool &is_shared_index_table);
  void clean_deprecated_adapters(storage::ObLS *ls, ObPluginVectorIndexMgr *mgr, const bool is_ls_leader);
  // Cancel all async tasks associated with the given tablet_id in the task map.
  // Returns OB_SUCCESS if every ctx was cancelled, or the first non-SUCCESS tmp_ret
  // (per-ctx failures are logged but do not abort the sweep).
  int cancel_tablet_async_tasks_(const common::ObTabletID &tablet_id,
                                 ObVecIndexAsyncTaskOption &task_opt);
  // Cancel ctxs whose truncate_version_ is older than the table's current
  // truncate_version. Skips IVF_CLEAN. Used when a tablet still exists but the
  // schema has bumped truncate_version.
  int cancel_tablet_async_tasks_for_truncated_(const common::ObTabletID &tablet_id,
                                               const int64_t truncate_version,
                                               ObVecIndexAsyncTaskOption &task_opt);
  // Cancel all tasks across all LSes when tenant is being deleted.
  // Returns true when every tenant task cancel succeeded (used to gate tenant_cancel_done_).
  bool cancel_all_tenant_tasks_();
  bool can_schedule(ObVectorTaskScheduleType task_type) const { return can_schedule_[task_type]; }
  void check_can_schedule();
  void schedule_finish();

  int do_history_cleanup_();
  int move_task_to_history_table_();
  int clear_history_task_();

  // Sync ls_executor_map_ with current LS list in service, init executors for new LSes
  // and release executors for removed LSes.
  int sync_ls_executors();
  int collect_leader_executors_to_run(
      ObSEArray<std::pair<ObLSID, ObVecIdxLeaderExecutors *>, DEFAULT_LS_EXECUTOR_MAP_SIZE> &leader_executors_to_run);
  int collect_follower_executors_to_run(
      ObSEArray<std::pair<ObLSID, ObVecIdxFollowerExecutors *>, DEFAULT_LS_EXECUTOR_MAP_SIZE> &follower_executors_to_run);
  int load_leader_tasks(
      const ObSEArray<std::pair<ObLSID, ObVecIdxLeaderExecutors *>, DEFAULT_LS_EXECUTOR_MAP_SIZE> &leader_executors_to_run,
      uint64_t &task_trace_base_num);
  // One tenant-level read of manual PREPARE tasks; dispatch to leader/follower executors by tablet -> LS.
  int load_triggered_tasks_();
  int load_leader_ls_tasks(const ObLSID &ls_id, ObVecIdxLeaderExecutors &exec, bool has_ivf_index, uint64_t &task_trace_base_num);
  int load_follower_tasks(
      const ObSEArray<std::pair<ObLSID, ObVecIdxFollowerExecutors *>, DEFAULT_LS_EXECUTOR_MAP_SIZE> &follower_executors_to_run,
      uint64_t &task_trace_base_num);
  int load_follower_ls_tasks(const ObLSID &ls_id, ObVecIdxFollowerExecutors &exec, uint64_t &task_trace_base_num);
  int check_and_schedule_leader_ls_tasks(
      const ObSEArray<std::pair<ObLSID, ObVecIdxLeaderExecutors *>, DEFAULT_LS_EXECUTOR_MAP_SIZE> &leader_executors_to_run);
  int check_and_schedule_follower_ls_tasks(
      const ObSEArray<std::pair<ObLSID, ObVecIdxFollowerExecutors *>, DEFAULT_LS_EXECUTOR_MAP_SIZE> &follower_executors_to_run);
  int pop_tasks_to_work();

  int find_ls_holding_tablet_(const common::ObTabletID &tablet_id, storage::ObLSHandle &ls_handle_out);
  int resolve_leader_triggered_executor_(const int64_t task_type, ObVecIdxLeaderExecutors &exec, ObVecITaskExecutor *&out);
  int resolve_follower_triggered_executor_(const int64_t task_type, ObVecIdxFollowerExecutors &exec, ObVecITaskExecutor *&out);
  int claim_triggered_task_(const ObVecIndexTaskStatus &row, bool &claimed);
  int finish_triggered_task_(
      const ObVecIndexTaskStatus &row,
      const int ret_code,
      const int64_t expected_status);
  int check_and_finish_orphan_self_tasks_();
  // When alive is false, orphan_ret_code carries the reason:
  //   OB_TABLET_NOT_EXIST  - tablet/ls is no longer held by this observer
  //   OB_ERR_UNEXPECTED    - tablet is held but in-memory task ctx is missing
  int check_task_alive_in_memory_(const ObVecIndexTaskStatus &row, bool &alive, int &orphan_ret_code);
  int finish_orphan_self_task_(const ObVecIndexTaskStatus &row);
  // Check whether table_id has been dropped or moved to recyclebin.
  // Returns OB_SUCCESS on lookup success (dropped is set accordingly).
  // Returns non-success if schema lookup itself fails — caller should treat
  // unknown state as "not dropped" (safe default: do not finish the task).
  int check_table_dropped_(uint64_t table_id, bool &dropped);
  int handle_cancelled_task_(
      ObVecIndexAsyncTaskCtx &task_ctx,
      const share::ObLSID &ls_id,
      ObVecIndexTaskCtxArray &cancelled_ctx_array);
  void inc_leader_executor_ref_(ObVecIdxLeaderExecutors &exec);
  void dec_leader_executor_ref_(ObVecIdxLeaderExecutors &exec);
  void retire_leader_executor_(ObVecIdxLeaderExecutors *leader_exec);
  void inc_follower_executor_ref_(ObVecIdxFollowerExecutors &exec);
  void dec_follower_executor_ref_(ObVecIdxFollowerExecutors &exec);
  void retire_follower_executor_(ObVecIdxFollowerExecutors *follower_exec);
  bool is_inited_;
  bool is_stopped_;
  uint64_t tenant_id_;
  int schedule_tg_id_;
  ObPluginVectorIndexService *service_;
  mutable common::ObSpinLock ls_executor_lock_;  // protect both executor maps for concurrent access
  common::hash::ObHashMap<share::ObLSID, ObVecIdxLeaderExecutors *> ls_leader_executor_map_;
  common::hash::ObHashMap<share::ObLSID, ObVecIdxFollowerExecutors *> ls_follower_executor_map_;
  common::ObArenaAllocator executor_allocator_;
  int64_t last_schedule_time_[ObVectorTaskScheduleType::SCHEDULE_MAX];
  bool can_schedule_[ObVectorTaskScheduleType::SCHEDULE_MAX];
  int64_t current_memory_config_;
  int64_t local_schema_version_;
  // Latched true only after a full successful cancel sweep in cancel_all_tenant_tasks_()
  // for OB_TENANT_NOT_EXIST. Accessed via ATOMIC_LOAD/STORE so future non-timer callers
  // (if any) do not hit TOCTOU. Stays false while any per-ctx cancel failed so the next
  // scheduler tick retries.
  bool is_tenant_cancel_done_;
  bool is_first_startup_round_done_;
  bool is_first_update_round_done_;
  int64_t startup_cleanup_cutoff_ts_;
  int64_t last_startup_cleanup_retry_ts_;
  int64_t startup_cleanup_total_rows_;

  int cleanup_stale_tasks_on_startup_(bool &cleanup_done, const bool skip_startup_clean);

  // --- Task disable list (emergency) ---
  struct DisableEntry {
    ObVecIndexAsyncTaskType task_type_;
    int64_t tablet_id_;  // 0 means wildcard (all tablets)
  };
  static const int64_t MAX_DISABLE_ENTRIES = 128;
  // Allocated in init() via ob_malloc(ObMemAttr) for module memory tracking.
  bool *disabled_all_;                                           // wildcard disable flags
  DisableEntry *disable_entries_;                                // per-tablet disable list
  int64_t disable_entry_count_;

  void refresh_disable_list_config_();
  bool is_task_disabled_(ObVecIndexAsyncTaskType task_type, int64_t tablet_id) const;

  // --- Throttle state for slow-path operations ---
  uint64_t last_disable_list_hash_;       // hash of last parsed _vector_task_disable_list
  bool cached_can_do_work_;               // cached result of check_can_do_work()
  int64_t last_can_do_work_check_ts_;     // timestamp of last successful check_can_do_work()
  int64_t last_triggered_task_check_ts_;  // timestamp of last load_triggered_tasks_()
  int64_t last_orphan_task_check_ts_;      // timestamp of last orphan self-task check
  int64_t last_sched_snapshot_ts_;
  int64_t last_blocked_diag_ts_;
  int64_t last_unexpected_no_pop_diag_ts_;
  bool last_has_queued_;
  bool last_blocked_by_water_level_;
  int64_t last_thread_count_;
  int64_t last_total_queued_;
  uint64_t last_water_level_config_hash_;
  ObVecAsyncSchedDiagStat sched_diag_stat_;

  DISALLOW_COPY_AND_ASSIGN(ObVecIdxAsyncTaskScheduler);
};

} // namespace share
} // namespace oceanbase
#endif // OCEANBASE_SHARE_VECTOR_INDEX_OB_VEC_IDX_ASYNC_TASK_SCHEDULER_H_
