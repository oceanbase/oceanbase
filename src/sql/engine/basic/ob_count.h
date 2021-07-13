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

#ifndef SRC_SQL_ENGINE_BASIC_OB_COUNT_H_
#define SRC_SQL_ENGINE_BASIC_OB_COUNT_H_
#include "sql/engine/ob_single_child_phy_operator.h"

namespace oceanbase {
namespace sql {
class ObSqlExpression;

class ObCount : public ObSingleChildPhyOperator {
  OB_UNIS_VERSION_V(1);

public:
  class ObCountCtx : ObPhyOperatorCtx {
    friend class ObCount;

  public:
    explicit ObCountCtx(ObExecContext& ctx)
        : ObPhyOperatorCtx(ctx), cur_rownum_(1), has_rownum_limit_(false), rownum_limit_value_(-1)
    {}
    virtual void destroy()
    {
      ObPhyOperatorCtx::destroy_base();
    }

  public:
    int64_t cur_rownum_;
    bool has_rownum_limit_;
    int64_t rownum_limit_value_;
  };

public:
  explicit ObCount(common::ObIAllocator& alloc);
  virtual ~ObCount();
  virtual int get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;
  virtual void reset();
  virtual void reuse();
  virtual int rescan(ObExecContext& ctx) const;
  inline void set_rownum_limit_expr(ObSqlExpression* rownum_limit_expr)
  {
    rownum_limit_expr_ = rownum_limit_expr;
  }
  int add_anti_monotone_filter_exprs(ObSqlExpression* expr);
  virtual int switch_iterator(ObExecContext& ctx) const;

private:
  bool is_valid() const;
  int get_rownum_limit_value(ObExecContext& ctx, int64_t& rownum_limit_value) const;
  int get_int_value(ObExecContext& ctx, const ObSqlExpression* in_val, int64_t& out_val) const;
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

private:
  ObSqlExpression* rownum_limit_expr_;
  common::ObDList<ObSqlExpression> anti_monotone_filter_exprs_;
};
} /* namespace sql */
} /* namespace oceanbase */

#endif /* SRC_SQL_ENGINE_BASIC_OB_COUNT_H_ */
