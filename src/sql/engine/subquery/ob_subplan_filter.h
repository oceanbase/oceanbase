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

#ifndef OCEANBASE_SRC_SQL_ENGINE_SUBQUERY_OB_SUBPLAN_FILTER_H_
#define OCEANBASE_SRC_SQL_ENGINE_SUBQUERY_OB_SUBPLAN_FILTER_H_
#include "lib/container/ob_bit_set.h"
#include "lib/container/ob_fixed_array.h"
#include "sql/engine/ob_multi_children_phy_operator.h"
namespace oceanbase {
namespace sql {
class ObSubPlanFilter : public ObMultiChildrenPhyOperator {
  OB_UNIS_VERSION(1);
  class ObSubPlanFilterCtx;
  class ObSubPlanIterator;

public:
  explicit ObSubPlanFilter(common::ObIAllocator& alloc);
  virtual ~ObSubPlanFilter();

  void reset();
  void reuse();
  int rescan(ObExecContext& ctx) const;

  /**
   * @brief add rescan param to subplan filter
   * @param sql_expr[in], the sql expression of rescan param
   * @param param_idx[in], rescan param index
   * @return if success, return OB_SUCCESS
   */
  int add_rescan_param(ObSqlExpression* sql_expr, int64_t param_idx);
  int add_onetime_expr(ObSqlExpression* sql_expr, int64_t param_idx);

  void add_initplan_idxs(const common::ObBitSet<>& idxs)
  {
    init_plan_idxs_.add_members2(idxs);
  }

  void add_onetime_idxs(const common::ObBitSet<>& idxs)
  {
    one_time_idxs_.add_members2(idxs);
  }

  int init_rescan_param(int64_t count)
  {
    return init_array_size<>(rescan_params_, count);
  }
  int init_onetime_exprs(int64_t count)
  {
    return init_array_size<>(onetime_exprs_, count);
  }
  void set_update_set(bool update_set)
  {
    update_set_ = update_set;
  }
  bool is_update_set()
  {
    return update_set_;
  }
  virtual int open(ObExecContext& ctx) const;
  virtual int switch_iterator(ObExecContext& ctx) const override;

private:
  /**
   * @brief init operator context, will create a physical operator context (and a current row space)
   * @param ctx[in], execute context
   * @return if success, return OB_SUCCESS, otherwise, return errno
   */
  virtual int init_op_ctx(ObExecContext& ctx) const;
  /**
   * @brief called by get_next_row(), get a row from the child operator or row_store
   * @param ctx[in], execute context
   * @param row[out], ObNewRow an obj array and row_size
   */
  virtual int inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;
  /**
   * @brief open operator, not including children operators.
   * called by open.
   * Every op should implement this method.
   */
  virtual int inner_open(ObExecContext& ctx) const;
  /**
   * @brief close operator, not including children operators.
   * Every op should implement this method.
   */
  virtual int inner_close(ObExecContext& ctx) const;
  /**
   * @brief wrap the object of ObExprCtx, and reset calc_buf
   * @param exec_ctx[in], execute context
   * @param expr_ctx[out], sql expression calculate buffer context
   * @return if success, return OB_SUCCESS
   */
  virtual int wrap_expr_ctx(ObExecContext& exec_ctx, common::ObExprCtx& expr_ctx) const;
  int prepare_rescan_params(ObExecContext& ctx, const common::ObNewRow& row) const;
  int prepare_onetime_exprs(ObExecContext& ctx) const;
  virtual int64_t to_string_kv(char* buf, const int64_t buf_len) const;
  int handle_update_set(ObSubPlanFilterCtx* subplan_ctx, const common::ObNewRow*& row) const;
  int construct_array_params(ObExecContext& ctx) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSubPlanFilter);

private:
  common::ObFixedArray<std::pair<ObSqlExpression*, int64_t>, common::ObIAllocator> rescan_params_;
  common::ObFixedArray<std::pair<ObSqlExpression*, int64_t>, common::ObIAllocator> onetime_exprs_;
  common::ObBitSet<common::OB_DEFAULT_BITSET_SIZE, common::ModulePageAllocator> init_plan_idxs_;
  common::ObBitSet<common::OB_DEFAULT_BITSET_SIZE, common::ModulePageAllocator> one_time_idxs_;
  bool update_set_;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SRC_SQL_ENGINE_SUBQUERY_OB_SUBPLAN_FILTER_H_ */
