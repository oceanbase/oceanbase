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

#ifndef OBDEV_SRC_SQL_DAS_OB_DAS_REF_H_
#define OBDEV_SRC_SQL_DAS_OB_DAS_REF_H_
#include "sql/das/ob_das_task.h"
#include "sql/das/ob_das_define.h"
#include "sql/das/ob_das_factory.h"
#include "sql/das/ob_das_def_reg.h"
#include "storage/tx/ob_trans_service.h"

namespace oceanbase
{
namespace sql
{
class ObDASScanOp;
class ObDASInsertOp;
class ObRpcDasAsyncAccessCallBack;

enum ObDasAggTaskStartStatus
{
  DAS_AGG_TASK_UNSTART = 0,
  DAS_AGG_TASK_REACH_MEM_LIMIT,
  DAS_AGG_TASK_PARALLEL_EXEC,
  DAS_AGG_TASK_REMOTE_EXEC
};

enum ObDasParallelType
{
  DAS_SERIALIZATION = 0,
  DAS_STREAMING_PARALLEL,
  DAS_BLOCKING_PARALLEL
};

struct DASParallelContext
{
public:
  DASParallelContext()
    :  parallel_type_(DAS_SERIALIZATION),
       submitted_task_count_(0),
       das_dop_(0),
       tx_desc_bak_(nullptr),
       has_refreshed_tx_desc_scn_(false)
  {
  }
  ~DASParallelContext() {}
  void set_das_dop(int64_t das_dop) { das_dop_ = das_dop; }
  void set_das_parallel_type(ObDasParallelType v) { parallel_type_ = v; }
  void add_submitted_task_count(int64_t v) { submitted_task_count_ += v; }
  void reset_task_count() { submitted_task_count_ = 0; }
  ObDasParallelType get_parallel_type() { return parallel_type_; }
  int64_t get_submitted_task_count() { return submitted_task_count_; }
  int64_t get_das_dop() { return das_dop_; }
  void reuse()
  {
    submitted_task_count_ = 0;
    has_refreshed_tx_desc_scn_ = false;
  }
  void reset()
  {
    parallel_type_ = DAS_SERIALIZATION;
    submitted_task_count_ = 0;
    if (OB_NOT_NULL(tx_desc_bak_)) {
      transaction::ObTransService *txs = MTL(transaction::ObTransService*);
      txs->release_tx(*tx_desc_bak_);
      tx_desc_bak_ = NULL;
    }
    has_refreshed_tx_desc_scn_ = false;
  }
  void set_tx_desc_bak(transaction::ObTxDesc *v) { tx_desc_bak_ = v; }
  transaction::ObTxDesc *get_tx_desc_bak() { return tx_desc_bak_; }
  int deep_copy_tx_desc(ObIAllocator &alloc, transaction::ObTxDesc *src_tx_desc);
  int refresh_tx_desc_bak(ObIAllocator &alloc, transaction::ObTxDesc *src_tx_desc);
  int release_tx_desc();
  TO_STRING_KV(K_(parallel_type),
               K_(submitted_task_count),
               K_(das_dop),
               K_(tx_desc_bak),
               K_(has_refreshed_tx_desc_scn));
public:
  ObDasParallelType parallel_type_;
  int64_t submitted_task_count_;
  int64_t das_dop_;
  transaction::ObTxDesc *tx_desc_bak_;
  bool has_refreshed_tx_desc_scn_;
};

struct DASRefCountContext
{
public:
  DASRefCountContext();
  void inc_concurrency_limit_with_signal();
  void inc_concurrency_limit();
  int32_t get_max_das_task_concurrency() const { return max_das_task_concurrency_; };
  ~DASRefCountContext() {}
  int32_t get_current_concurrency() const
  {
    return ATOMIC_LOAD(&das_task_concurrency_limit_);
  }
  int dec_concurrency_limit();
  common::ObThreadCond &get_cond() { return cond_; }
  int acquire_task_execution_resource(int64_t timeout_ts);
  void interrupt_other_workers(int ret)
  {
    ObThreadCondGuard guard(cond_);
    if (err_ret_ == OB_SUCCESS) {
      err_ret_ = ret;
    }
  }
  int get_interrupted_err_code() { return ATOMIC_LOAD(&err_ret_); }
  void reuse() { need_wait_ = false; }
  void set_need_wait(bool v) { need_wait_ = v; }
  bool is_need_wait() { return need_wait_; }
  TO_STRING_KV(K(max_das_task_concurrency_), K(das_task_concurrency_limit_));
public:
  int32_t max_das_task_concurrency_;
  int32_t das_task_concurrency_limit_;
  int err_ret_;
  common::ObThreadCond cond_;
  bool need_wait_;
  bool is_inited_;
};

struct ObDasAggregatedTask
{
 public:
  ObDasAggregatedTask(common::ObIAllocator &allocator)
    : server_(),
      tasks_(),
      failed_tasks_(),
      success_tasks_(),
      start_status_(DAS_AGG_TASK_UNSTART),
      mem_used_(0),
      save_ret_(OB_SUCCESS)
  {
  }
  ~ObDasAggregatedTask() { reset(); };
  void reset();
  void reuse();
  int push_back_task(ObIDASTaskOp *das_task);
  // not thread safe
  int move_to_success_tasks(ObIDASTaskOp *das_task);
  // not thread safe
  int move_to_failed_tasks(ObIDASTaskOp *das_task);
  /**
   * get aggregated tasks.
   * @param tasks: task array to output aggregated tasks.
  */
  int get_aggregated_tasks(common::ObIArray<ObIDASTaskOp *> &tasks);

  int get_failed_tasks(common::ObSEArray<ObIDASTaskOp *, 2> &tasks);
  bool has_failed_tasks() const { return failed_tasks_.get_size() > 0; }
  bool has_unstart_tasks() const;
  bool has_not_execute_task() const
  {
    return tasks_.get_size() != 0 || failed_tasks_.get_size() != 0;
  }
  int32_t get_unstart_task_size() const;
  void set_start_status(ObDasAggTaskStartStatus status) { start_status_ = status; }
  void add_mem_used(int64_t mem_used) { mem_used_ += mem_used; }
  int64_t get_mem_used() { return mem_used_; }
  void set_save_ret(int ret) { save_ret_ = ret; }
  int get_save_ret() { return save_ret_; }
  bool execute_success()
  {
    return save_ret_ == OB_SUCCESS && (!has_not_execute_task());
  }
  bool has_parallel_submiitted()
  {
    return (start_status_ == DAS_AGG_TASK_PARALLEL_EXEC || start_status_ == DAS_AGG_TASK_REMOTE_EXEC);
  }
  ObDasAggTaskStartStatus get_start_status() { return start_status_; }
  TO_STRING_KV(K(server_),
               K(start_status_),
               K(tasks_.get_size()),
               K(failed_tasks_.get_size()),
               K(success_tasks_.get_size()),
               K(save_ret_));
  common::ObAddr server_;
  DasTaskLinkedList tasks_;
  DasTaskLinkedList failed_tasks_;
  DasTaskLinkedList success_tasks_;
  ObDasAggTaskStartStatus start_status_;
  int64_t mem_used_;
  int save_ret_;
};

struct DasRefKey
{
public:
  DasRefKey()
    : tablet_loc_(NULL),
      op_type_(ObDASOpType::DAS_OP_INVALID)
  {}
  DasRefKey(const ObDASTabletLoc *tablet_loc, ObDASOpType op_type)
    : tablet_loc_(tablet_loc),
      op_type_(op_type)
  {}
  ~DasRefKey() {}
  bool operator==(const DasRefKey &other) const;
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
  TO_STRING_KV(KP_(tablet_loc), K_(op_type));

public:
  const ObDASTabletLoc *tablet_loc_;
  ObDASOpType op_type_;
};

static const int64_t DAS_REF_TASK_LOOKUP_THRESHOLD = 1000;
static const int64_t DAS_REF_TASK_SIZE_THRESHOLD = 1000;
static const int64_t DAS_REF_MAP_BUCKET_SIZE = 5000;
typedef common::hash::ObHashMap<DasRefKey, ObIDASTaskOp *, common::hash::NoPthreadDefendMode> ObDASRefMap;
typedef common::ObObjStore<ObDasAggregatedTask *, common::ObIAllocator&> DasAggregatedTaskList;

class ObDASRef
{
public:
  explicit ObDASRef(ObEvalCtx &eval_ctx, ObExecContext &exec_ctx);
  ~ObDASRef() { reset(); }

  bool check_tasks_same_ls_and_is_local(share::ObLSID &ls_id);
  DASOpResultIter begin_result_iter();
  DASTaskIter begin_task_iter() { return batched_tasks_.begin(); }
  ObDASTaskFactory &get_das_factory() { return das_factory_; }
  void set_mem_attr(const common::ObMemAttr &memattr) { das_alloc_.set_attr(memattr); }
  void set_enable_rich_format(const bool v) { enable_rich_format_ = v; }
  ObExecContext &get_exec_ctx() { return exec_ctx_; }
  template <typename DASOp>
  bool has_das_op(const ObDASTabletLoc *tablet_loc, DASOp *&das_op);
  ObIDASTaskOp* find_das_task(const ObDASTabletLoc *tablet_loc, ObDASOpType op_type);
  int add_aggregated_task(ObIDASTaskOp *das_task, ObDASOpType op_type);
  int create_agg_task(ObDASOpType op_type, const ObDASTabletLoc *tablet_loc, ObDasAggregatedTask *&agg_task);
  int find_agg_task(const ObDASTabletLoc *tablet_loc, ObDASOpType op_type, ObDasAggregatedTask *&agg_task);
  int add_batched_task(ObIDASTaskOp *das_task);
  //创建一个DAS Task，并由das_ref持有
  template <typename DASOp>
  int prepare_das_task(const ObDASTabletLoc *tablet_loc, DASOp *&task_op);
  int create_das_task(const ObDASTabletLoc *tablet_loc,
                      ObDASOpType op_type,
                      ObIDASTaskOp *&task_op);
  bool has_task() const { return !batched_tasks_.empty(); }
  int32_t get_das_task_cnt() const { return batched_tasks_.get_size(); }
  ObDasParallelType get_parallel_type() { return das_parallel_ctx_.get_parallel_type(); }
  bool is_parallel_submit() { return get_parallel_type() != DAS_SERIALIZATION; }
  int64_t get_submitted_task_count() { return das_parallel_ctx_.get_submitted_task_count(); }
  int64_t get_das_dop() { return das_parallel_ctx_.get_das_dop(); }
  DASParallelContext &get_das_parallel_ctx() { return das_parallel_ctx_; }

  int parallel_submit_agg_task(ObDasAggregatedTask *agg_task);
  int execute_all_task();
  int execute_all_task(DasAggregatedTaskList &agg_task_list);
  bool check_agg_task_can_retry(ObDasAggregatedTask *agg_task);
  int retry_all_fail_tasks(common::ObIArray<ObIDASTaskOp *> &failed_tasks);
  int close_all_task();
  bool is_all_local_task() const;
  bool is_do_gts_opt() { return do_gts_opt_; }
  void set_do_gts_opt(bool v) { do_gts_opt_ = v; }
  void set_execute_directly(bool v) { execute_directly_ = v; }
  bool is_execute_directly() const { return execute_directly_; }
  common::ObIAllocator &get_das_alloc() { return das_alloc_; }
  int64_t get_das_mem_used() const { return das_alloc_.used() - init_mem_used_; }

  int pick_del_task_to_first();
  void print_all_das_task();
  const ObExprFrameInfo *get_expr_frame_info() const { return expr_frame_info_; }
  void set_expr_frame_info(const ObExprFrameInfo *info) { expr_frame_info_ = info; }
  ObEvalCtx &get_eval_ctx() { return eval_ctx_; };
  void reset();
  void reuse();
  void set_lookup_iter(DASOpResultIter *lookup_iter) { wild_datum_info_.lookup_iter_ = lookup_iter; }
  int wait_all_tasks();
  int allocate_async_das_cb(ObRpcDasAsyncAccessCallBack *&async_cb,
                            const common::ObSEArray<ObIDASTaskOp*, 2> &task_ops,
                            int64_t timeout_ts);
  void remove_async_das_cb(ObRpcDasAsyncAccessCallBack *das_async_cb);
  DASRefCountContext &get_das_ref_count_ctx() { return das_ref_count_ctx_; }
  void clear_task_map();
  int wait_tasks_and_process_response();
private:
  DISABLE_COPY_ASSIGN(ObDASRef);
  int create_task_map();
  int move_local_tasks_to_last();
  int wait_all_executing_tasks();
  int process_remote_task_resp();
  bool check_rcode_can_retry(int ret);
private:
  typedef common::ObObjNode<ObIDASTaskOp*> DasOpNode;
  //declare das allocator
  common::ObWrapperAllocatorWithAttr das_alloc_;
  common::ObArenaAllocator *reuse_alloc_;
  union {
    common::ObArenaAllocator reuse_alloc_buf_;
  };
  /////
  ObDASTaskFactory das_factory_;
  //一个SQL Operator可以同时产生多个das task，并由DAS批量执行
  DasTaskList batched_tasks_;
  ObExecContext &exec_ctx_;
  ObEvalCtx &eval_ctx_;
  const ObExprFrameInfo *expr_frame_info_;
  DASOpResultIter::WildDatumPtrInfo wild_datum_info_;
  DasAggregatedTaskList del_aggregated_tasks_; // we must execute all delete tasks firstly

  DasAggregatedTaskList aggregated_tasks_;
  int64_t lookup_cnt_;
  int64_t task_cnt_;
  int64_t init_mem_used_;
  ObDASRefMap task_map_;
  typedef common::ObObjStore<ObRpcDasAsyncAccessCallBack *, common::ObIAllocator&> DasAsyncCbList;
  DasAsyncCbList async_cb_list_;
  DASRefCountContext das_ref_count_ctx_;
  DASParallelContext das_parallel_ctx_;
public:
  //all flags
  union {
    uint64_t flags_;
    struct { // FARM COMPAT WHITELIST
      uint64_t execute_directly_                : 1;
      uint64_t enable_rich_format_              : 1;
      uint64_t do_gts_opt_                      : 1;
      uint64_t reserved_                        : 62;
    };
  };
};

template <typename DASOp>
OB_INLINE bool ObDASRef::has_das_op(const ObDASTabletLoc *tablet_loc, DASOp *&das_op)
{
  ObDASOpType type = static_cast<ObDASOpType>(das_reg::ObDASOpTraits<DASOp>::type_);
  bool bret = false;
  ObIDASTaskOp *das_task = find_das_task(tablet_loc, type);
  if (das_task != nullptr) {
    bret = true;
    OB_ASSERT(typeid(*das_task) == typeid(DASOp));
    das_op = static_cast<DASOp*>(das_task);
  }
  return bret;
}

template <typename DASOp>
OB_INLINE int ObDASRef::prepare_das_task(const ObDASTabletLoc *tablet_loc, DASOp *&task_op)
{
  ObDASOpType type = static_cast<ObDASOpType>(das_reg::ObDASOpTraits<DASOp>::type_);
  ObIDASTaskOp *tmp_op = nullptr;
  int ret = create_das_task(tablet_loc, type, tmp_op);
  if (OB_SUCC(ret)) {
    task_op = static_cast<DASOp*>(tmp_op);
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
#endif /* OBDEV_SRC_SQL_DAS_OB_DAS_REF_H_ */
