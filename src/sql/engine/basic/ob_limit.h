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

#ifndef _OB_LIMIT_H
#define _OB_LIMIT_H 1
#include "sql/engine/ob_single_child_phy_operator.h"
#include "sql/engine/basic/ob_material.h"
#include "sql/engine/sort/ob_sort.h"

namespace oceanbase {
namespace sql {
class ObSqlExpression;

class ObLimit : public ObSingleChildPhyOperator {
  OB_UNIS_VERSION_V(1);

private:
  class ObLimitCtx;

public:
  explicit ObLimit(common::ObIAllocator& alloc);
  virtual ~ObLimit();

  virtual void reset();
  virtual void reuse();
  /// @param limit -1 means no limit
  int set_limit(ObSqlExpression* limit, ObSqlExpression* offset, ObSqlExpression* percent);
  int get_limit(ObExecContext& ctx, int64_t& limit, int64_t& offset, bool& is_percent_first) const;
  virtual int rescan(ObExecContext& ctx) const;

  void set_is_top_limit(const bool is_top)
  {
    is_top_limit_ = is_top;
  }
  void set_calc_found_rows(const bool found_rows)
  {
    is_calc_found_rows_ = found_rows;
  }
  void set_is_fetch_with_ties(const bool is_fetch_with_ties)
  {
    is_fetch_with_ties_ = is_fetch_with_ties;
  }
  int add_sort_columns(ObSortColumn sort_column);
  int init_sort_columns(int64_t count)
  {
    return init_array_size<>(sort_columns_, count);
  }
  int convert_limit_percent(ObExecContext& ctx, ObLimitCtx* limit_ctx) const;

private:
  bool is_valid() const;
  int get_int_value(ObExecContext& ctx, const ObSqlExpression* in_val, int64_t& out_val, bool& is_null_value) const;
  int get_double_value(ObExecContext& ctx, const ObSqlExpression* double_val, double& out_val) const;
  /**
   * @brief init operator context, will create a physical operator context (and a current row space)
   * @param ctx[in], execute context
   * @return if success, return OB_SUCCESS, otherwise, return errno
   */
  int init_op_ctx(ObExecContext& ctx) const;
  /**
   * @brief: called by get_next_row(), get a row from the child operator or row_store
   * @param: ctx[in], execute context
   * @param: row[out], ObSqlRow an obj array and row_size
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
   * @brief for specified phy operator to print it's member variable with json key-value format
   * @param buf[in] to string buffer
   * @param buf_len[in] buffer length
   * @return if success, return the length used by print string, otherwise return 0
   */
  virtual int64_t to_string_kv(char* buf, const int64_t buf_len) const;
  int add_filter(ObSqlExpression* expr);
  int deep_copy_limit_last_rows(ObLimitCtx* limit_ctx, const common::ObNewRow row) const;
  int is_row_order_by_item_value_equal(ObLimitCtx* limit_ctx, const common::ObNewRow* input_row, bool& is_equal) const;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObLimit);

private:
  // data members
  ObSqlExpression* org_limit_;
  ObSqlExpression* org_offset_;
  ObSqlExpression* org_percent_;
  bool is_calc_found_rows_;
  bool is_top_limit_;
  bool is_fetch_with_ties_;
  // data members
  common::ObFixedArray<ObSortColumn, common::ObIAllocator> sort_columns_;
};
}  // end namespace sql
}  // end namespace oceanbase

#endif /* _OB_LIMIT_H */
