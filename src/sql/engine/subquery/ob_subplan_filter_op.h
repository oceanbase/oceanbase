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

#ifndef OCEANBASE_SUBQUERY_OB_SUBPLAN_FILTER_OP_H_
#define OCEANBASE_SUBQUERY_OB_SUBPLAN_FILTER_OP_H_

#include "sql/engine/ob_operator.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/ob_sql_define.h"

namespace oceanbase
{
namespace sql
{

class DatumRow
{
public:
  DatumRow() : elems_(NULL), cnt_(0) {}
  ~DatumRow() {}
  bool operator==(const DatumRow &other) const;
  int hash(uint64_t &hash_val, uint64_t seed=0) const;
  TO_STRING_KV(KP(elems_));
  ObDatum *elems_;
  int64_t cnt_;
};

// iterator subquery rows
class ObSubQueryIterator
{
public:
  explicit ObSubQueryIterator(ObOperator &op);
  ~ObSubQueryIterator()
  {
    if (hashmap_.created()) {
      hashmap_.destroy();
    }
    if (nullptr != mem_entity_) {
      DESTROY_CONTEXT(mem_entity_);
      mem_entity_ = NULL;
    }
  }
  enum RescanStatus
  {
    INVALID_STATUS = 0,
    SWITCH_BATCH,
    NOT_SWITCH_BATCH,
    NORMAL
  };
  void set_onetime_plan() { onetime_plan_ = true; }
  void set_init_plan() { init_plan_ = true;}

  const ExprFixedArray &get_output() const { return op_.get_spec().output_; }
  ObOperator &get_op() const { return op_; }

  int get_next_row();

  int prepare_init_plan();
  void reuse();
  int rewind(bool reset_onetime_plan = false);
  //int rescan_chlid(int64_t child_idx);
  int init_mem_entity();
  int init_hashmap(const int64_t param_num)
  {
    int64_t tenant_id = op_.get_exec_ctx().get_my_session()->get_effective_tenant_id();
    return hashmap_.create(param_num * 2,
                           ObMemAttr(tenant_id, "SqlSQIterBKT", ObCtxIds::DEFAULT_CTX_ID),
                           ObMemAttr(tenant_id, "SqlSQIterND", ObCtxIds::DEFAULT_CTX_ID));
  }
  bool has_hashmap() const { return hashmap_.created(); }
  int init_probe_row(const int64_t cnt);
  int get_arena_allocator(common::ObIAllocator *&alloc);
  //fill curr exec param into probe_row_
  int get_curr_probe_row();
  void set_iter_id(const int64_t id) { id_ = id; }
  int64_t get_iter_id() const { return id_; }
  //use curr probe_row_ to probe hashmap
  int get_refactored(common::ObDatum &out);
  //set row into hashmap
  int set_refactored(const DatumRow &row, const ObDatum &result, const int64_t deep_copy_size);
  void set_parent(const ObSubPlanFilterOp *filter) { parent_ = filter; }
  int reset_hash_map();

  bool check_can_insert(const int64_t deep_copy_size)
  {
    return deep_copy_size + memory_used_ < HASH_MAP_MEMORY_LIMIT;
  }

  ObEvalCtx &get_eval_ctx() { return eval_ctx_; }

  //for vectorized
  int get_next_batch(const int64_t max_row_cnt, const ObBatchRows *&batch_rows);
  //for vectorized end
  bool is_onetime_plan() const { return onetime_plan_; }

  void set_new_batch(bool new_batch) { is_new_batch_ = new_batch;};
  TO_STRING_KV(K(onetime_plan_), K(init_plan_), K(inited_));

  //a row cache for hash optimizer to use
  DatumRow probe_row_;
  //hard core, 1M limit for each hashmap
  const static int HASH_MAP_MEMORY_LIMIT = 1024 * 1024;
  void drain_exch();

private:

  // for das batch spf
  int alloc_das_batch_store();
  int save_das_batch_store();
  int resume_das_batch_store();
  // for das batch spf end
  ObOperator &op_;
  bool onetime_plan_;
  bool init_plan_;
  bool inited_;

  ObChunkDatumStore store_;
  ObChunkDatumStore::Iterator store_it_;

  //cache optimizer for spf, the same exec_param into queryref_expr will return directly
  common::hash::ObHashMap<DatumRow, common::ObDatum, common::hash::NoPthreadDefendMode> hashmap_;
  lib::MemoryContext mem_entity_;
  int64_t id_; // curr op_id in spf
  const ObSubPlanFilterOp *parent_; //needs to get exec_param_idxs_ from op
  int64_t memory_used_;
  ObEvalCtx &eval_ctx_;

  // for vectorized
  const ObBatchRows *iter_brs_;
  int64_t batch_size_;
  int64_t batch_row_pos_;
  bool iter_end_;
  // for vectorized end

  // for das batch spf
  bool is_new_batch_;
  uint64_t current_group_;
  common::ObArrayWrap<ObObjParam> das_batch_params_recovery_;
  // for das batch spf end
};

class ObSubPlanFilterSpec : public ObOpSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObSubPlanFilterSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type);

  DECLARE_VIRTUAL_TO_STRING;
    int init_px_batch_rescan_flags(int64_t count)
  { return enable_px_batch_rescans_.init(count); }

  //在主表的每次迭代生成的行数据对于subquery来说都是驱动其进行数据迭代的参数
  common::ObFixedArray<ObDynamicParamSetter, common::ObIAllocator> rescan_params_;
  //只计算一次subquery条件
  common::ObFixedArray<ObDynamicParamSetter, common::ObIAllocator> onetime_exprs_;
  //InitPlan idxs，InitPlan只算一次，需要存储结果
  common::ObBitSet<common::OB_DEFAULT_BITSET_SIZE, common::ModulePageAllocator> init_plan_idxs_;
  //One-Time idxs，One-Time只算一次，不用存储结果
  common::ObBitSet<common::OB_DEFAULT_BITSET_SIZE, common::ModulePageAllocator> one_time_idxs_;

  // update set (, ,) = (subquery)
  ExprFixedArray update_set_;
  common::ObFixedArray<ObFixedArray<ObExpr *, common::ObIAllocator>, common::ObIAllocator> exec_param_array_;
  bool exec_param_idxs_inited_;
  // 标记每个子查询是否可以做px batch rescan
  common::ObFixedArray<bool, common::ObIAllocator> enable_px_batch_rescans_;
  bool enable_das_group_rescan_;
  ExprFixedArray filter_exprs_;
  ExprFixedArray output_exprs_;
  common::ObFixedArray<ObDynamicParamSetter, common::ObIAllocator> left_rescan_params_;
  common::ObFixedArray<ObDynamicParamSetter, common::ObIAllocator> right_rescan_params_;
};

class ObSubPlanFilterOp : public ObOperator
{
public:
  typedef ObSubQueryIterator Iterator;

  ObSubPlanFilterOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);
  virtual ~ObSubPlanFilterOp();

  virtual int inner_open() override;
  virtual int rescan() override;
  virtual int switch_iterator() override;

  virtual int inner_get_next_row() override;
  virtual int inner_close() override;

  virtual void destroy() override;

  const common::ObIArray<Iterator *> &get_subplan_iters() const { return subplan_iters_; }
  int reset_batch_rescan_param()
  {
    rescan_batch_params_.reset();
    return common::OB_SUCCESS;
  }
  int handle_next_row();
  bool enable_px_batch_rescan() { return enable_left_px_batch_; }
  //for vectorized
  int inner_get_next_batch(const int64_t max_row_cnt);
  // for vectorized end

  int init_left_cur_row(const int64_t column_cnt, ObExecContext &ctx);
  int fill_cur_row_rescan_param();

  //for DAS batch SPF
  int fill_cur_row_das_batch_param(ObEvalCtx& eval_ctx, uint64_t current_group) const;
  int bind_das_batch_params_to_store() const;
  void get_current_group(uint64_t& current_group) const;
  bool enable_left_das_batch() const {return MY_SPEC.enable_das_group_rescan_;}
  //for DAS batch SPF end

  const ObSubPlanFilterSpec &get_spec() const
  { return static_cast<const ObSubPlanFilterSpec &>(spec_); }

public:
  ObBatchRescanCtl &get_batch_rescan_ctl() { return batch_rescan_ctl_; }
  int handle_next_batch_with_px_rescan(const int64_t op_max_batch_size);
  int handle_next_batch_with_group_rescan(const int64_t op_max_batch_size);
private:
  void set_param_null() { set_pushdown_param_null(MY_SPEC.rescan_params_); };
  void destroy_subplan_iters();
  void destroy_px_batch_rescan_status();
  void destroy_update_set_mem()
  {
    if (NULL != update_set_mem_) {
      DESTROY_CONTEXT(update_set_mem_);
      update_set_mem_ = NULL;
    }
  }

  int prepare_rescan_params(bool save);
  int prepare_onetime_exprs();
  int prepare_onetime_exprs_inner();
  int handle_update_set();
  bool continue_fetching(uint64_t left_rows_total_cnt, bool stop, bool use_group = false)
  {
    return use_group?
            (!stop && (left_rows_total_cnt < max_group_size_))
           :(!stop && (left_rows_total_cnt < PX_RESCAN_BATCH_ROW_COUNT));
  }

  // for das batch spf
  int alloc_das_batch_params(uint64_t group_size);
  int init_das_batch_params();
  int deep_copy_dynamic_obj();
  // for das batch spf end

private:
  common::ObSEArray<Iterator *, 16> subplan_iters_;
  lib::MemoryContext update_set_mem_;
  bool iter_end_;
  // for px batch rescan
  bool enable_left_px_batch_;
  // for px batch rescan end
  // for das batch rescan
  uint64_t max_group_size_; //Das batch rescan size;
  uint64_t current_group_;  //The group id in this time right iter rescan;
  common::ObArrayWrap<ObSqlArrayObj> das_batch_params_;
  // for das batch rescan end
  ObChunkDatumStore left_rows_;
  ObChunkDatumStore::Iterator left_rows_iter_;
  ObChunkDatumStore::ShadowStoredRow last_store_row_;
  bool save_last_row_;
  bool is_left_end_;
  ObBatchRescanParams rescan_batch_params_;
  int64_t left_row_idx_;
  ObBatchRescanCtl batch_rescan_ctl_;
  sql::ObTMArray<common::ObObjParam> cur_params_;
  common::ObSArray<int64_t> cur_param_idxs_;
  common::ObSArray<int64_t> cur_param_expr_idxs_;
  common::ObSEArray<Iterator*, 8> subplan_iters_to_check_;
  lib::MemoryContext last_store_row_mem_;
  ObBatchResultHolder brs_holder_;
};

class GroupParamBackupGuard
{
public:
  GroupParamBackupGuard(ObEvalCtx& eval_ctx,
                       common::ObArrayWrap<ObObjParam>& das_batch_params_recovery,
                       const common::ObFixedArray<ObDynamicParamSetter, common::ObIAllocator>& rescan_params,
                       int64_t params_count)
    : eval_ctx_(eval_ctx),
      das_batch_params_recovery_(das_batch_params_recovery),
      rescan_params_(rescan_params),
      params_count_(params_count)
  {
    save_das_batch_store();
  }
  ~GroupParamBackupGuard()
  {
    resume_das_batch_store();
  }
private:
  void save_das_batch_store();
  void resume_das_batch_store();
private:
  ObEvalCtx& eval_ctx_;
  common::ObArrayWrap<ObObjParam>& das_batch_params_recovery_;
  const common::ObFixedArray<ObDynamicParamSetter, common::ObIAllocator>& rescan_params_;
  int64_t params_count_;
};


} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_SUBQUERY_OB_SUBPLAN_FILTER_OP_H_
