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

#ifndef OCEANBASE_SQL_OB_RECURSIVE_UNION_ALL_OPERATOR_H_
#define OCEANBASE_SQL_OB_RECURSIVE_UNION_ALL_OPERATOR_H_

#include "sql/engine/ob_phy_operator.h"
#include "sql/engine/set/ob_merge_set_operator.h"
#include "sql/engine/ob_double_children_phy_operator.h"
#include "sql/engine/sort/ob_specific_columns_sort.h"
#include "common/row/ob_row.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/allocator/ob_malloc.h"
#include "ob_fake_cte_table.h"
#include "ob_recursive_inner_data.h"

namespace oceanbase {
namespace sql {
class ObRecursiveUnionAll : public ObMergeSetOperator {
  OB_UNIS_VERSION_V(1);

protected:
  class ObRecursiveUnionAllOperatorCtx : public ObPhyOperatorCtx {
  public:
    explicit ObRecursiveUnionAllOperatorCtx(ObExecContext& ctx)
        : ObPhyOperatorCtx(ctx), inner_data_(ctx.get_allocator())
    {}
    ~ObRecursiveUnionAllOperatorCtx()
    {}
    virtual void destroy()
    {
      inner_data_.~ObRecursiveInnerData();
      ObPhyOperatorCtx::destroy_base();
    }
    void set_search_strategy(ObRecursiveInnerData::SearchStrategyType strategy)
    {
      inner_data_.set_search_strategy(strategy);
    }
    void set_cur_row()
    {
      inner_data_.cur_row_ = &get_cur_row();
    }
    int init();
    int ctx_close();

  public:
    common::ObExprCtx expr_ctx_;
    ObRecursiveInnerData inner_data_;
  };

public:
  explicit ObRecursiveUnionAll(common::ObIAllocator& alloc);
  ~ObRecursiveUnionAll();
  virtual void reset();
  virtual void reuse();
  virtual int rescan(ObExecContext& ctx) const;

  inline void set_search_strategy(ObRecursiveInnerData::SearchStrategyType strategy)
  {
    strategy_ = strategy;
  };
  inline void set_fake_cte_table(ObFakeCTETable* cte_table)
  {
    pump_operator_ = cte_table;
  };
  int set_cycle_pseudo_values(ObSqlExpression& v, ObSqlExpression& d_v);

protected:
  /**
   * @brief for specified phy operator to print it's member variable with json key-value format
   * @param buf[in] to string buffer
   * @param buf_len[in] buffer length
   * @return if success, return the length used by print string, otherwise return 0
   */
  virtual int64_t to_string_kv(char* buf, const int64_t buf_len) const;
  /**
   * @brief init operator context, will create a physical operator context (and a current row space)
   * @param ctx[in], execute context
   * @return if success, return OB_SUCCESS, otherwise, return errno
   */
  virtual int init_op_ctx(ObExecContext& ctx) const;
  /**
   * @brief create operator context, only child operator can know it's specific operator type,
   * so must be overwrited by child operator,
   * @param ctx[in], execute context
   * @param op_ctx[out], the pointer of operator context
   * @return if success, return OB_SUCCESS, otherwise, return errno
   */
  virtual int inner_create_operator_ctx(ObExecContext& ctx, ObPhyOperatorCtx*& op_ctx) const;
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
   * @brief: called by get_next_row(), get a row from the child operator or row_store
   * @param: ctx[in], execute context
   * @param: row[out], ObSqlRow an obj array and row_size
   */
  virtual int inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObRecursiveUnionAll);

public:
  common::ObFixedArray<ObSortColumn, common::ObIAllocator> search_by_col_lists_;
  common::ObFixedArray<common::ObColumnInfo, common::ObIAllocator> cycle_by_col_lists_;

protected:
  static const int32_t CMP_DIRECTION_ASC = 1;
  static const int32_t CMP_DIRECTION_DESC = -1;
  const ObFakeCTETable* pump_operator_;
  ObRecursiveInnerData::SearchStrategyType strategy_;
  ObSqlExpression cycle_value_;
  ObSqlExpression cycle_default_value_;
};

}  // end namespace sql
}  // end namespace oceanbase

#endif /* OCEANBASE_SQL_OB_SET_OPERATOR_H_ */
