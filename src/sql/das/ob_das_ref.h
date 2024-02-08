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

struct ObDasAggregatedTasks
{
 public:
  ObDasAggregatedTasks(common::ObIAllocator &allocator)
      : server_(),
        high_priority_tasks_(),
        tasks_(),
        failed_tasks_(),
        success_tasks_() {}
  ~ObDasAggregatedTasks() { reset(); };
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
  int get_aggregated_tasks(common::ObSEArray<ObIDASTaskOp *, 2> &tasks);
  /**
   * get aggregated tasks.
   * @param task_groups: task array's array to output aggregated tasks.
   * @param count: how many aggregated tasks should be generated.
  */
  int get_aggregated_tasks(common::ObSEArray<common::ObSEArray<ObIDASTaskOp *, 2>, 2> &task_groups, int64_t count);
  int get_failed_tasks(common::ObSEArray<ObIDASTaskOp *, 2> &tasks);
  bool has_failed_tasks() const { return failed_tasks_.get_size() > 0; }
  bool has_unstart_tasks() const;
  bool has_unstart_high_priority_tasks() const;
  int32_t get_unstart_task_size() const;
  TO_STRING_KV(K_(server),
               K(high_priority_tasks_.get_size()),
               K(tasks_.get_size()),
               K(failed_tasks_.get_size()),
               K(success_tasks_.get_size()));
  common::ObAddr server_;
  DasTaskLinkedList high_priority_tasks_;
  DasTaskLinkedList tasks_;
  DasTaskLinkedList failed_tasks_;
  DasTaskLinkedList success_tasks_;
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

class ObDASRef
{
public:
  explicit ObDASRef(ObEvalCtx &eval_ctx, ObExecContext &exec_ctx);
  ~ObDASRef() { reset(); }

  DASOpResultIter begin_result_iter();
  DASTaskIter begin_task_iter() { return batched_tasks_.begin(); }
  ObDASTaskFactory &get_das_factory() { return das_factory_; }
  void set_mem_attr(const common::ObMemAttr &memattr) { das_alloc_.set_attr(memattr); }
  void set_enable_rich_format(const bool v) { enable_rich_format_ = v; }
  ObExecContext &get_exec_ctx() { return exec_ctx_; }
  template <typename DASOp>
  bool has_das_op(const ObDASTabletLoc *tablet_loc, DASOp *&das_op);
  ObIDASTaskOp* find_das_task(const ObDASTabletLoc *tablet_loc, ObDASOpType op_type);
  int add_aggregated_task(ObIDASTaskOp *das_task);
  int add_batched_task(ObIDASTaskOp *das_task);
  //创建一个DAS Task，并由das_ref持有
  template <typename DASOp>
  int prepare_das_task(const ObDASTabletLoc *tablet_loc, DASOp *&task_op);
  int create_das_task(const ObDASTabletLoc *tablet_loc,
                      ObDASOpType op_type,
                      ObIDASTaskOp *&task_op);
  bool has_task() const { return !batched_tasks_.empty(); }
  int32_t get_das_task_cnt() const { return batched_tasks_.get_size(); }
  int execute_all_task();
  int close_all_task();
  bool is_all_local_task() const;
  void set_execute_directly(bool v) { execute_directly_ = v; }
  bool is_execute_directly() const { return execute_directly_; }
  common::ObIAllocator &get_das_alloc() { return das_alloc_; }
  int64_t get_das_mem_used() const { return das_alloc_.used() - init_mem_used_; }

  int pick_del_task_to_first();
  void print_all_das_task();
  void set_frozen_node();
  const ObExprFrameInfo *get_expr_frame_info() const { return expr_frame_info_; }
  void set_expr_frame_info(const ObExprFrameInfo *info) { expr_frame_info_ = info; }
  ObEvalCtx &get_eval_ctx() { return eval_ctx_; };
  void reset();
  void reuse();
  void set_lookup_iter(DASOpResultIter *lookup_iter) { wild_datum_info_.lookup_iter_ = lookup_iter; }
  int32_t get_current_concurrency() const;
  void inc_concurrency_limit();
  void inc_concurrency_limit_with_signal();
  int dec_concurrency_limit();
  int32_t get_max_concurrency() const { return max_das_task_concurrency_; };
  int acquire_task_execution_resource();
  int get_aggregated_tasks_count() const { return aggregated_tasks_.get_size(); }
  int wait_all_tasks();
  int allocate_async_das_cb(ObRpcDasAsyncAccessCallBack *&async_cb,
                            const common::ObSEArray<ObIDASTaskOp*, 2> &task_ops,
                            int64_t timeout_ts);
  void remove_async_das_cb(ObRpcDasAsyncAccessCallBack *das_async_cb);
private:
  DISABLE_COPY_ASSIGN(ObDASRef);
  int create_task_map();
  int move_local_tasks_to_last();
  int wait_executing_tasks();
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
  DasOpNode *frozen_op_node_; // 初始为链表的head节点，冻结一次之后为链表的最后一个节点
  const ObExprFrameInfo *expr_frame_info_;
  DASOpResultIter::WildDatumPtrInfo wild_datum_info_;
  typedef common::ObObjStore<ObDasAggregatedTasks *, common::ObIAllocator&> DasAggregatedTaskList;
  DasAggregatedTaskList aggregated_tasks_;
  int64_t lookup_cnt_;
  int64_t task_cnt_;
  int64_t init_mem_used_;
  ObDASRefMap task_map_;
  int32_t max_das_task_concurrency_;
  int32_t das_task_concurrency_limit_;
  common::ObThreadCond cond_;
  typedef common::ObObjStore<ObRpcDasAsyncAccessCallBack *, common::ObIAllocator&> DasAsyncCbList;
  DasAsyncCbList async_cb_list_;
public:
  //all flags
  union {
    uint64_t flags_;
    struct {
      uint64_t execute_directly_                : 1;
      uint64_t enable_rich_format_              : 1;
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
