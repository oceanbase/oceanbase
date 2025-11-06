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

#ifndef OB_STORAGE_COLUMN_STORE_CO_MERGE_DAG_H_
#define OB_STORAGE_COLUMN_STORE_CO_MERGE_DAG_H_
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/compaction/ob_tablet_merge_task.h"
#include "storage/compaction/ob_partition_merger.h"
#include "storage/column_store/ob_co_merge_ctx.h"
#include "lib/lock/ob_spin_lock.h"
#include "storage/column_store/ob_column_oriented_merger.h"

namespace oceanbase
{
namespace storage
{
struct ObUpdateTableStoreParam;
}

namespace compaction
{
class ObCOMergeDagNet;
struct ObCOMergeDagParam : public ObTabletMergeDagParam
{
  ObCOMergeDagParam();
  ~ObCOMergeDagParam() {}
  virtual bool is_valid() const override;
  INHERIT_TO_STRING_KV("ObCOMergeDagParam", ObTabletMergeDagParam,
      K_(compat_mode), K_(dag_net_id), K_(range_count), K_(cg_count), K_(is_using_column_tmp_file));
  lib::Worker::CompatMode compat_mode_;
  share::ObDagId dag_net_id_;
  int64_t range_count_;
  int64_t cg_count_;
  bool is_using_column_tmp_file_;
};

class ObCOMergeDag : public ObTabletMergeDag
{
public:
  ObCOMergeDag(const share::ObDagType::ObDagTypeEnum type);
  virtual ~ObCOMergeDag() {};
  virtual int init_by_param(const share::ObIDagInitParam *param) override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;
  virtual ObBasicTabletMergeCtx *get_ctx() override { return nullptr; } // always return nullptr in co dag.
  virtual void set_dag_error_location() override;
  INHERIT_TO_STRING_KV("ObCOMergeDag", ObTabletMergeDag, K_(dag_net_id));
protected:
  share::ObDagId dag_net_id_;
};

class ObCOMergeTask: public share::ObITask
{
public:
  explicit ObCOMergeTask(const ObITaskType type)
    : ObITask(type),
      is_inited_(false),
      dag_net_(nullptr)
  {}
  ~ObCOMergeTask() {}

  int init(ObCOMergeDagNet &dag_net);
  int get_ctx_from_dag_net(ObCOMergeDagNet &dag_net, ObCOTabletMergeCtx *&ctx);
  VIRTUAL_TO_STRING_KV(K("ObCOMergeTask"), K_(is_inited), KP(this), KP_(dag), KP_(dag_net));
protected:
  bool is_inited_;
  ObCOMergeDagNet *dag_net_;
};

/*
 * Prepare DAG
 * */
class ObCOMergePrepareDag : public ObCOMergeDag
{
public:
  ObCOMergePrepareDag()
    : ObCOMergeDag(ObDagType::DAG_TYPE_CO_MERGE_PREPARE)
  {}
  ~ObCOMergePrepareDag() {}
  virtual int create_first_task() override;
  INHERIT_TO_STRING_KV("ObCOMergePrepareDag", ObCOMergeDag, KP(this));
};

class ObCOMergePrepareTask: public ObCOMergeTask
{
public:
  ObCOMergePrepareTask()
    : ObCOMergeTask(ObITask::TASK_TYPE_SSTABLE_MERGE_PREPARE)
  {}
  ~ObCOMergePrepareTask() {}
  INHERIT_TO_STRING_KV("ObCOMergePrepareTask", ObCOMergeTask, KP(this));
protected:
  virtual int process() override;
private:
  int create_schedule_dag(ObCOTabletMergeCtx &ctx);
  int schedule_minor_exec_dag(ObCOTabletMergeCtx &ctx, ObGetMergeTablesResult &result);
};

/*
 * Schedule DAG
 * */
class ObCOMergeScheduleDag : public ObCOMergeDag
{
public:
  ObCOMergeScheduleDag()
    : ObCOMergeDag(ObDagType::DAG_TYPE_CO_MERGE_SCHEDULE),
      swap_tablet_flag_(false)
  {}
  ~ObCOMergeScheduleDag() {}
  virtual int create_first_task() override;
  virtual bool ignore_warning() override
  {
    return ObTabletMergeDag::ignore_warning()
      || can_ignore_warning(dag_ret_);
  }
  static bool can_ignore_warning(const int ret) { // allowed failure
    return OB_ALLOCATE_MEMORY_FAILED == ret
      || OB_EAGAIN == ret
      || OB_SIZE_OVERFLOW == ret;
  }
  void set_swap_tablet_flag(const bool swap_tablet_flag) { swap_tablet_flag_ = swap_tablet_flag; }
  bool get_swap_tablet_flag() const { return swap_tablet_flag_; }

  INHERIT_TO_STRING_KV("ObCOMergeScheduleDag", ObCOMergeDag, K_(swap_tablet_flag));
private:
  bool swap_tablet_flag_;
};

class ObCOMergeScheduleTask: public ObCOMergeTask
{
public:
  ObCOMergeScheduleTask()
    : ObCOMergeTask(ObITask::TASK_TYPE_SSTABLE_MERGE_PREPARE)
  {}
  ~ObCOMergeScheduleTask() {}
  INHERIT_TO_STRING_KV("ObCOMergeScheduleTask", ObCOMergeTask, KP(this));
protected:
  virtual int process() override;
};

/*
 * Execute DAG
 * */
class ObCOMergeExeDag : public ObCOMergeDag
{
public:
  enum RangeMergeStatus : uint8_t
  {
    RANGE_NEED_PERSIST = 0,
    RANGE_PERSIST_FINISH
  };
  enum CGMergeStatus : uint8_t
  {
    CG_NEED_REPLAY = 0,
    CG_SKIP_REPLAY,
    CG_IS_REPLAYING,
    CG_REPLAY_FAILED,
    CG_REPLAY_FINISH,
    CG_SSTABLE_CREATED
  };
public:
  ObCOMergeExeDag();
  ~ObCOMergeExeDag();
  void reset();
  virtual int init_by_param(const share::ObIDagInitParam *param) override;
  virtual int create_first_task() override;
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int gene_compaction_info(compaction::ObTabletCompactionProgress &progress) override;
  virtual int diagnose_compaction_info(compaction::ObDiagnoseTabletCompProgress &progress) override;
  virtual int decide_retry_strategy(const int error_code, ObDagRetryStrategy &retry_status);
  virtual int inner_reset_status_for_retry() override;
  ObPartitionMergeProgress *get_merge_progress() { return merge_progress_; }
  bool check_replay_finished();
  int get_cg_merge_status(const int64_t range_idx, const int64_t cg_idx, CGMergeStatus &status);
  int set_cg_merge_status(const int64_t range_idx, const int64_t start_cg_idx, const int64_t end_cg_idx, const CGMergeStatus status);
  int finish_replay(const int64_t range_idx, const int64_t start_cg_idx, const int64_t end_cg_idx, bool &all_cg_finished);
  int try_generate_finish_task(ObITask *parent);
  int set_range_merge_status(const int64_t range_idx, const RangeMergeStatus status);
  void set_need_reduce_batch() { ATOMIC_SET(&need_reduce_batch_, true); }
  int generate_next_task(ObCOTabletMergeCtx &ctx, ObCOMergeDagNet &dag_net, ObITask *parent_task, ObITask *&next_task);
  template <typename T, typename... Args>
  int create_task(ObITask *parent, T *&task, Args&&... args);
  void delete_task(ObITask *&task);
  INHERIT_TO_STRING_KV("ObCOMergeExeDag", ObCOMergeDag,
    K_(range_count), K_(cg_count), K_(merge_batch_size));
private:
  int get_dag_net_and_ctx(ObCOMergeDagNet *&dag_net, ObCOTabletMergeCtx *&ctx);
  int prepare_replay_status(const int64_t range_count, const int64_t cg_count);
  int init_merge_batch_size(const bool is_using_column_tmp_file);
  void try_update_merge_batch_size();
  int64_t inner_get_replayed_cg_count() const;
  int64_t inner_get_persisted_range_count() const;
  int get_next_replay_cg_pair(
      const int64_t range_idx,
      ObCOTabletMergeCtx &ctx,
      int64_t &start_cg_idx,
      int64_t &end_cg_idx);
  OB_INLINE bool inner_check_replay_finished() const
  {
    bool bret = true;
    for (int64_t i = 0; bret && i < range_count_ * cg_count_; ++i) {
      if (CG_REPLAY_FINISH > cg_merge_status_[i]) {
        bret = false;
      }
    }
    return bret;
  }
  OB_INLINE bool inner_check_range_replay_finished(const int64_t range_idx) const
  {
    bool bret = true;
    for (int64_t i = 0 ; bret && i < cg_count_; ++i) {
      if (CG_REPLAY_FINISH > cg_merge_status_[range_idx * cg_count_ + i]) {
        bret = false;
      }
    }
    return bret;
  }
  OB_INLINE void inner_set_cg_merge_status(
      const int64_t range_idx,
      const int64_t start_cg_idx,
      const int64_t end_cg_idx,
      const CGMergeStatus status)
  {
    for (int64_t i = start_cg_idx; i < end_cg_idx; ++i) {
      cg_merge_status_[range_idx * cg_count_ + i] = status;
    }
  }
private:
  bool need_reduce_batch_;
  bool need_update_batch_size_;
  int64_t range_count_; // range count
  int64_t cg_count_;
  int64_t merge_batch_size_;
  RangeMergeStatus *range_status_;
  CGMergeStatus *cg_merge_status_;
  common::ObSpinLock exe_lock_;
  compaction::ObPartitionMergeProgress *merge_progress_; // TODO
};

class ObCOMergeExeTask : public ObCOMergeTask
{
public:
  explicit ObCOMergeExeTask(const ObITaskType type)
    : ObCOMergeTask(type),
      range_idx_(-1),
      allocator_("CoMergeExe")
  {}
  int init(ObCOMergeDagNet &dag_net, const int64_t range_idx);
  INHERIT_TO_STRING_KV("ObCOMergeExeTask", ObCOMergeTask, K_(range_idx));
protected:
  int64_t range_idx_;
  compaction::ObLocalArena allocator_;
};

// build and persist merge log
class ObCOMergeLogPersistTask : public ObCOMergeExeTask
{
public:
  ObCOMergeLogPersistTask()
    : ObCOMergeExeTask(ObITask::TASK_TYPE_CO_MERGE_PERSIST),
      persister_(nullptr)
  {}
  ~ObCOMergeLogPersistTask();
  void reset();
  INHERIT_TO_STRING_KV("ObCOMergeLogPersistTask", ObCOMergeExeTask, KP_(persister));
protected:
  virtual int process() override;
private:
  ObCOMergeLogPersister *persister_;
};

// read/build + replay merge log
class ObCOMergeLogReplayTask : public ObCOMergeExeTask
{
public:
  friend class ObCOMergeExeDag;
  ObCOMergeLogReplayTask()
    : ObCOMergeExeTask(ObITask::TASK_TYPE_CO_MERGE_REPLAY),
      replayer_(nullptr),
      start_cg_idx_(0),
      end_cg_idx_(0)
  {}
  ~ObCOMergeLogReplayTask();
  void reset();
  int init(ObCOMergeDagNet &dag_net,
      const int64_t range_idx,
      const int64_t start_cg_idx,
      const int64_t end_cg_idx);
  virtual int generate_next_task(ObITask *&next_task) override;
  INHERIT_TO_STRING_KV("ObCOMergeLogReplayTask", ObCOMergeExeTask, K_(start_cg_idx), K_(end_cg_idx));
protected:
  virtual int process() override;
private:
  ObCOMergeLogReplayer *replayer_;
  int64_t start_cg_idx_;
  int64_t end_cg_idx_;
};

class ObCOMergeFinishTask : public ObCOMergeTask
{
public:
  ObCOMergeFinishTask()
    : ObCOMergeTask(ObITask::TASK_TYPE_CO_MERGE_FINISH)
  {}
  ~ObCOMergeFinishTask() {}

  INHERIT_TO_STRING_KV("ObCOMergeFinishTask", ObCOMergeTask, KP(this));
protected:
  virtual int process() override;
private:
  int create_cg_sstables();
};

// PrepareDag -> [MinorDag -> ScheduleDag] -> ExeDag
class ObCOMergeDagNet : public share::ObIDagNet, public ObMergeDagHash
{
public:
  ObCOMergeDagNet();
  ~ObCOMergeDagNet();

  virtual int init_by_param(const share::ObIDagInitParam *param) override;

  virtual bool is_valid() const override;
  virtual int start_running() override;
  virtual bool operator == (const ObIDagNet &other) const override;
  virtual uint64_t hash() const override
  {
    uint64_t hash_value = ObMergeDagHash::inner_hash();
    ObDagNetType::ObDagNetTypeEnum dag_net_type = get_type();
    hash_value = common::murmurhash(
        &dag_net_type, sizeof(dag_net_type), hash_value);
    return hash_value;
  }
  virtual int fill_dag_net_key(char *buf, const int64_t buf_len) const override;
  virtual int fill_comment(char *buf, const int64_t buf_len) const override;
  virtual int clear_dag_net_ctx() override;
  void cancel_dag_net(const int error_code);
  int create_co_execute_dag(share::ObIDag &parent_dag);
  int prepare_co_merge_ctx();
  int get_compat_mode();
  int swap_tablet_after_minor();
  ObCOTabletMergeCtx *get_merge_ctx() const { return co_merge_ctx_; }
  const ObCOMergeDagParam& get_dag_param() const { return basic_param_; }
  void set_prepare_dag_running_ts() { prepare_dag_running_ts_ = ObTimeUtility::fast_current_time(); }
  int64_t get_prepare_dag_running_ts() const { return prepare_dag_running_ts_; }
  template<class T>
  int create_dag(
    T *&dag,
    share::ObIDag *parent = nullptr,
    const int64_t range_count = 0,
    const int64_t cg_count = 0,
    const bool is_using_column_tmp_file = false);
  int init_min_sstable_end_scn();
  int get_min_sstable_end_scn(SCN &min_end_scn); // return min_end_scn from ctx
  INHERIT_TO_STRING_KV("ObCOMergeDagNet", ObIDagNet, K_(is_inited), K_(basic_param), K_(min_sstable_end_scn), K_(prepare_dag_running_ts));
private:
  static const int64_t DELAY_SCHEDULE_FINISH_DAG_CG_CNT = 150;
  static const int64_t DEFAULT_MAX_RETRY_TIMES = 2;
private:
  bool is_inited_;
  ObCOMergeDagParam basic_param_;
  common::ObArenaAllocator tmp_allocator_; // TODO(@lixia.yq) temp solution, use allocator on ObIDagNet later
  ObCOTabletMergeCtx *co_merge_ctx_;
  int64_t min_sstable_end_scn_;
  int64_t prepare_dag_running_ts_;
};

template<class T>
int ObCOMergeDagNet::create_dag(
    T *&dag,
    share::ObIDag *parent/* = nullptr*/,
    const int64_t range_count/* = 0 */,
    const int64_t cg_count/* = 0*/,
    const bool is_using_column_tmp_file/* = false*/)
{
  int ret = OB_SUCCESS;
  dag = nullptr;

  // create dag and connections
  basic_param_.range_count_ = range_count;
  basic_param_.cg_count_ = cg_count;
  basic_param_.is_using_column_tmp_file_ = is_using_column_tmp_file;
  if (OB_FAIL(MTL(share::ObTenantDagScheduler*)->alloc_dag(dag))) {
    STORAGE_LOG(WARN, "fail to alloc dag", K(ret));
  } else if (OB_FAIL(dag->init_by_param(&basic_param_))) {
    STORAGE_LOG(WARN, "Fail to init prepare dag", K(ret));
  } else if (nullptr != parent && OB_FAIL(parent->add_child(*dag))) {
    STORAGE_LOG(WARN, "failed to add child", K(ret), KPC(parent), KPC(dag));
  } else if (nullptr == parent && OB_FAIL(add_dag_into_dag_net(*dag))) {
    STORAGE_LOG(WARN, "fail to add dag into dag_net", K(ret));
  } else if (OB_FAIL(dag->create_first_task())) {
    STORAGE_LOG(WARN, "failed to create first task", K(ret), KPC(dag));
  } else if (share::ObDagType::DAG_TYPE_CO_MERGE_EXECUTE == dag->get_type()) {
#ifdef ERRSIM
    dag->set_max_retry_times(30);
#else
    dag->set_max_retry_times(DEFAULT_MAX_RETRY_TIMES);
#endif
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(MTL(share::ObTenantDagScheduler*)->add_dag(dag))) {
    STORAGE_LOG(WARN, "Fail to add dag into dag_scheduler", K(ret));
  } else {
    STORAGE_LOG(INFO, "success to create and add dag", K(ret), K_(basic_param), KPC(dag),
      "dag_type", ObIDag::get_dag_type_str(dag->get_type()),  K(dag->get_indegree()));
  }
  if (OB_FAIL(ret) && nullptr != dag) {
    // will remove from dag_net & free dag in this func
    (void)MTL(share::ObTenantDagScheduler*)->free_dag(*dag);
    dag = nullptr;
  }
  return ret;
}

template <typename T, typename... Args>
int ObCOMergeExeDag::create_task(ObITask *parent, T *&task, Args&&... args)
{
  return inner_create_task(false, parent, task, args...);
}


} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COLUMN_STORE_CO_MERGE_DAG_H_
