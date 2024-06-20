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

namespace oceanbase
{
namespace storage
{
struct ObUpdateTableStoreParam;
}

namespace compaction
{
class ObCOMergeDagNet;
// [start_cg_idx_, end_cg_idx_)
struct ObCOMergeDagParam : public ObTabletMergeDagParam
{
  ObCOMergeDagParam();
  ~ObCOMergeDagParam();
  virtual bool is_valid() const override;
  INHERIT_TO_STRING_KV("ObTabletMergeDagParam", ObTabletMergeDagParam,
      K_(start_cg_idx), K_(end_cg_idx), K_(compat_mode), K_(dag_net_id));
  uint32_t start_cg_idx_;
  uint32_t end_cg_idx_;
  lib::Worker::CompatMode compat_mode_;
  share::ObDagId dag_net_id_;
};

class ObCOMergeDag : public ObTabletMergeDag
{
public:
  ObCOMergeDag(const share::ObDagType::ObDagTypeEnum type);
  virtual ~ObCOMergeDag() {};
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;
  virtual ObBasicTabletMergeCtx *get_ctx() override { return nullptr; } // always return nullptr in co dag.
  virtual void set_dag_error_location() override;
};
/*
 * Prepare DAG
 * */
class ObCOMergePrepareDag : public ObCOMergeDag
{
public:
  ObCOMergePrepareDag();
  ~ObCOMergePrepareDag();
  virtual int init_by_param(const share::ObIDagInitParam *param) override;
  virtual int create_first_task() override;

  INHERIT_TO_STRING_KV("ObTabletMergeDag", ObTabletMergeDag, K_(dag_net_id));
private:
  share::ObDagId dag_net_id_;
};

class ObCOMergePrepareTask: public share::ObITask
{
public:
  ObCOMergePrepareTask();
  ~ObCOMergePrepareTask();

  int init(ObCOMergeDagNet *dag_net);
protected:
  virtual int process() override;
private:
  int create_schedule_dag(ObCOTabletMergeCtx &ctx);
private:
  bool is_inited_;
  ObCOMergeDagNet *dag_net_;
};

class ObCOMergeScheduleDag : public ObCOMergeDag
{
public:
  ObCOMergeScheduleDag();
  ~ObCOMergeScheduleDag();
  virtual int init_by_param(const share::ObIDagInitParam *param) override;
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

  INHERIT_TO_STRING_KV("ObTabletMergeDag", ObTabletMergeDag, K_(swap_tablet_flag), K_(dag_net_id));
private:
  bool swap_tablet_flag_;
  share::ObDagId dag_net_id_;
};

class ObCOMergeScheduleTask: public share::ObITask
{
public:
  ObCOMergeScheduleTask();
  ~ObCOMergeScheduleTask();

  int init(ObCOMergeDagNet *dag_net);
protected:
  virtual int process() override;
private:
  bool is_inited_;
  ObCOMergeDagNet *dag_net_;
};

/*
 * Execute DAG
 * */
class ObCOMergeBatchExeDag : public ObCOMergeDag
{
public:
  ObCOMergeBatchExeDag();
  ~ObCOMergeBatchExeDag();
  virtual bool operator == (const ObIDag &other) const override;
  virtual int init_by_param(const share::ObIDagInitParam *param) override;
  virtual int create_first_task() override;
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int report_result() override;
  virtual int gene_compaction_info(compaction::ObTabletCompactionProgress &progress) override;
  virtual int diagnose_compaction_info(compaction::ObDiagnoseTabletCompProgress &progress) override;
  uint32_t get_start_cg_idx() const { return start_cg_idx_; }
  uint32_t get_end_cg_idx() const { return end_cg_idx_; }
  bool get_retry_create_task() const { return retry_create_task_; }
  ObCompactionTimeGuard &get_time_guard() { return time_guard_; }
  OB_INLINE void dag_time_guard_click(const uint16_t event)
  {
    time_guard_.click(event);
  }
  ObPartitionMergeProgress *get_merge_progress() { return merge_progress_; }
  int init_merge_progress();
  virtual int inner_reset_status_for_retry() override;
  int create_sstable_after_merge();

  INHERIT_TO_STRING_KV("ObTabletMergeDag", ObTabletMergeDag, K_(dag_net_id), K_(start_cg_idx),
      K_(end_cg_idx), K_(retry_create_task));
private:
  int prepare_merge_progress();
public:
  common::ObSpinLock exe_lock_; // alloc && check cg_merge_infos && init something
private:
  share::ObDagId dag_net_id_;
  uint32_t start_cg_idx_;
  uint32_t end_cg_idx_;
  bool retry_create_task_;
  bool progress_inited_; // inited = true & merge_progress_ = nullptr means init failed
  ObStorageCompactionTimeGuard time_guard_;
  compaction::ObPartitionMergeProgress *merge_progress_;
};

class ObCOMergeBatchExeTask : public share::ObITask
{
public:
  ObCOMergeBatchExeTask();
  ~ObCOMergeBatchExeTask();
  int init(
      const int64_t idx,
      ObCOTabletMergeCtx &ctx,
      ObCOMergeDagNet &dag_net);
  virtual int generate_next_task(ObITask *&next_task) override;
protected:
  virtual int process() override;
private:
  void merge_start();
private:
  bool is_inited_;
  int64_t idx_;
  compaction::ObLocalArena allocator_;
  ObCOTabletMergeCtx *ctx_;
  ObCOMergeDagNet *dag_net_;
  ObMerger *merger_;
};

class ObCOMergeBatchFinishTask : public share::ObITask
{
public:
  ObCOMergeBatchFinishTask();
  ~ObCOMergeBatchFinishTask();
  int init(
      ObCOTabletMergeCtx &ctx,
      ObCOMergeDagNet &dag_net);
protected:
  virtual int process() override;
private:
  bool is_inited_;
  ObCOTabletMergeCtx *ctx_;
  ObCOMergeDagNet *dag_net_;
};

class ObCOMergeFinishDag : public ObCOMergeDag
{
public:
  ObCOMergeFinishDag();
  ~ObCOMergeFinishDag();
  virtual int init_by_param(const share::ObIDagInitParam *param) override;
  virtual int create_first_task() override;
  virtual bool check_can_schedule() override;

  INHERIT_TO_STRING_KV("ObTabletMergeDag", ObTabletMergeDag, K_(dag_net_id));
private:
  share::ObDagId dag_net_id_;
};

class ObCOMergeFinishTask: public share::ObITask
{
public:
  ObCOMergeFinishTask();
  ~ObCOMergeFinishTask();

  int init(
      ObCOTabletMergeCtx &ctx,
      ObCOMergeDagNet &dag_net);
protected:
  virtual int process() override;
private:
  bool is_inited_;
  ObCOTabletMergeCtx *ctx_;
  ObCOMergeDagNet *dag_net_;
};

// PrepareDag -> [MinorDag -> ScheduleDag] -> BatchExeDag* -> FinishDag
class ObCOMergeDagNet : public share::ObIDagNet, public ObMergeDagHash
{
public:
  enum COMergeStatus : uint8_t {
    NOT_INIT = 0,
    INITED = 1,
    CTX_PREPARED,
    PREPARE_FINISHED
  };

  ObCOMergeDagNet();
  ~ObCOMergeDagNet();

  virtual int init_by_param(const share::ObIDagInitParam *param) override;

  virtual bool is_valid() const override;
  virtual int start_running() override;
  virtual bool operator == (const ObIDagNet &other) const override;
  virtual int64_t hash() const override
  {
    int64_t hash_value = ObMergeDagHash::inner_hash();
    ObDagNetType::ObDagNetTypeEnum dag_net_type = get_type();
    hash_value = common::murmurhash(
        &dag_net_type, sizeof(dag_net_type), hash_value);
    return hash_value;
  }
  virtual int fill_dag_net_key(char *buf, const int64_t buf_len) const override;
  virtual int fill_comment(char *buf, const int64_t buf_len) const override;
  virtual int schedule_rest_dag() override;
  virtual int clear_dag_net_ctx() override;
  virtual bool inner_check_finished() override
  {
    return ATOMIC_LOAD(&finish_added_);
  }
  virtual int deal_with_cancel() override
  {
    if (!inner_check_finished() && OB_NOT_NULL(finish_dag_)) {
      (void)MTL(share::ObTenantDagScheduler*)->free_dag(*finish_dag_);
      finish_dag_ = nullptr;
    }
    return OB_SUCCESS;
  }
  void cancel_dag_net(const int error_code);
  int create_co_execute_dags(share::ObIDag &schedule_dag);
  bool check_merge_finished();
  bool should_force_cancel();
  int dag_report_result(
      const uint32_t start_cg_idx,
      const uint32_t end_cg_idx,
      const int dag_ret);
  void update_merge_status(const COMergeStatus &status) { ATOMIC_SET(&merge_status_, status); }
  int prepare_co_merge_ctx();
  int get_compat_mode();
  int swap_tablet_after_minor();
  ObCOTabletMergeCtx *get_merge_ctx() const { return co_merge_ctx_; }
  const ObCOMergeDagParam& get_dag_param() const { return basic_param_; }
  void collect_running_info(const uint32_t start_cg_idx, const uint32_t end_cg_idx, const int64_t hash,
      const share::ObDagId &dag_id, const ObCompactionTimeGuard &time_guard);
  template<class T>
  int create_dag(
    const uint32_t start_cg_idx,
    const uint32_t end_cg_idx,
    T *&dag,
    share::ObIDag *parent = nullptr,
    const bool add_scheduler_flag = true);
  INHERIT_TO_STRING_KV("ObIDagNet", ObIDagNet, K_(is_inited), K_(merge_status), K_(finish_added),
      K_(merge_batch_size), K_(basic_param), KPC_(co_merge_ctx), KP_(finish_dag));
private:
  static const int64_t DELAY_SCHEDULE_FINISH_DAG_CG_CNT = 150;
  static const int64_t DEFAULT_MAX_RETRY_TIMES = 2;

  // call this func with lock
  int inner_create_and_add_exe_dag(
      const int64_t start_cg_idx,
      const int64_t end_cg_idx,
      const int64_t max_cg_idx,
      int64_t &allowed_schedule_dag_count,
      ObCOMergeBatchExeDag *&dag,
      common::ObIArray<ObCOMergeBatchExeDag *> &exe_dag_array,
      const bool add_scheduler_flag = true);
  int choose_merge_batch_size(const int64_t column_group_cnt);
  int inner_schedule_finish_dag(ObIDag *parent_dag = nullptr);
  void try_update_merge_batch_size(const int64_t column_group_cnt);
  int inner_create_and_schedule_dags(ObIDag *parent_dag = nullptr);
  int inner_create_row_store_dag(
      ObIDag *parent_dag,
      const int64_t &max_cg_idx,
      common::ObIArray<ObCOMergeBatchExeDag *> &exe_dag_array);
  int inner_create_column_store_dag(
      ObIDag *parent_dag,
      int64_t &allowed_schedule_dag_count,
      const int64_t &max_cg_idx,
      common::ObIArray<ObCOMergeBatchExeDag *> &exe_dag_array);
  int inner_schedule_exe_dags(
      common::ObIArray<ObCOMergeBatchExeDag *> &dag_array,
      int64_t &unscheduled_dag_idx);
  void inner_free_exe_dags(
      const int64_t start_idx,
      common::ObIArray<ObCOMergeBatchExeDag *> &dag_array,
      ObCOMergeBatchExeDag *&dag);
  void inner_free_exe_dag(ObCOMergeBatchExeDag *&dag); // lock in inner_free_exe_dags
private:
  bool is_inited_;
  bool finish_added_;
  bool batch_reduced_; // only reduce batch_size one time in a round // locked by ctx_lock_
  lib::ObMutex ctx_lock_;
  int64_t merge_batch_size_; // will decrease when meet memory allocate failed
  COMergeStatus merge_status_;
  ObCOMergeDagParam basic_param_;
  common::ObArenaAllocator tmp_allocator_; // TODO(@lixia.yq) temp solution, use allocator on ObIDagNet later
  ObCOTabletMergeCtx *co_merge_ctx_;
  ObCOMergeFinishDag *finish_dag_;
  ObStorageCompactionTimeGuard time_guard_;
};

template<class T>
int ObCOMergeDagNet::create_dag(
    const uint32_t start_cg_idx,
    const uint32_t end_cg_idx,
    T *&dag,
    share::ObIDag *parent/* = nullptr*/,
    const bool add_scheduler_flag/* = true*/)
{
  int ret = OB_SUCCESS;
  dag = nullptr;

  // create dag and connections
  if (OB_UNLIKELY(start_cg_idx > end_cg_idx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid start/end cg idx", K(ret), K(start_cg_idx), K(end_cg_idx));
  } else {
    // start/end cg idx are meaningless for DagNet
    basic_param_.start_cg_idx_ = start_cg_idx;
    basic_param_.end_cg_idx_ = end_cg_idx;
    if (OB_FAIL(MTL(share::ObTenantDagScheduler*)->alloc_dag(dag))) {
      LOG_WARN("fail to alloc dag", K(ret));
    } else if (OB_FAIL(dag->init_by_param(&basic_param_))) {
      LOG_WARN("Fail to init prepare dag", K(ret));
    } else if (nullptr != parent && OB_FAIL(parent->add_child(*dag))) {
      LOG_WARN("failed to add child", K(ret), KPC(parent), KPC(dag));
    } else if (nullptr == parent && OB_FAIL(add_dag_into_dag_net(*dag))) {
      LOG_WARN("fail to add dag into dag_net", K(ret));
    } else if (OB_FAIL(dag->create_first_task())) {
      LOG_WARN("failed to create first task", K(ret), KPC(dag));
    } else if (share::ObDagType::DAG_TYPE_CO_MERGE_BATCH_EXECUTE == dag->get_type()) {
      dag->set_max_retry_times(DEFAULT_MAX_RETRY_TIMES);
#ifdef ERRSIM
      dag->set_max_retry_times(30);
#endif
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("success to create dag", K(ret), K_(basic_param), KP(dag), K(add_scheduler_flag), KP(parent));
    }
    if (OB_FAIL(ret) || !add_scheduler_flag) {
    } else if (OB_FAIL(MTL(share::ObTenantDagScheduler*)->add_dag(dag))) {
      LOG_WARN("Fail to add dag into dag_scheduler", K(ret));
    }
  }
  if (OB_FAIL(ret) && nullptr != dag) {
    // will remove from dag_net & free dag in this func
    (void)MTL(share::ObTenantDagScheduler*)->free_dag(*dag);
    dag = nullptr;
  }
  return ret;
}


} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COLUMN_STORE_CO_MERGE_DAG_H_
