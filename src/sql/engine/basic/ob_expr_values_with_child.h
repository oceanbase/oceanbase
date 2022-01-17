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

#ifndef OCEANBASE_SQL_ENGINE_DML_EXPR_VALUES_WITH_CHILD_H
#define OCEANBASE_SQL_ENGINE_DML_EXPR_VALUES_WITH_CHILD_H
#include "lib/charset/ob_charset.h"
#include "sql/engine/ob_single_child_phy_operator.h"
#include "sql/engine/expr/ob_sql_expression.h"
namespace oceanbase {
namespace sql {
class ObIterExprRangeParam;
class ObExprValuesWithChild : public ObSingleChildPhyOperator {
  class ObExprValuesWithChildCtx;

public:
  explicit ObExprValuesWithChild(common::ObIAllocator& alloc);
  virtual ~ObExprValuesWithChild();

  virtual int rescan(ObExecContext& ctx) const;
  int init_value_count(int64_t value_cnt);
  int add_value(ObSqlExpression* value);
  int init_range_params(int64_t range_param_count)
  {
    UNUSED(range_param_count);
    return common::OB_SUCCESS;
  }
  int add_range_param(ObIterExprRangeParam* range_param_expr)
  {
    UNUSED(range_param_expr);
    return common::OB_SUCCESS;
  }
  int64_t get_size() const
  {
    return values_.count();
  }
  virtual int serialize(char* buf, int64_t buf_len, int64_t& pos, ObPhyOpSeriCtx& seri_ctx) const;
  virtual int64_t get_serialize_size(const ObPhyOpSeriCtx& seri_ctx) const;
  int deserialize(const char* buf, const int64_t data_len, int64_t& pos);
  int deserialize_(const char* buf, const int64_t data_len, int64_t& pos);
  int deserialize_dispatch_(const char* buf, const int64_t data_len, int64_t& pos, std::false_type)
  {
    return deserialize_(buf, data_len, pos);
  }

private:
  const static int64_t UNIS_VERSION = 1;

private:
  // function members
  /**
   * @brief init operator context, will create a physical operator context (and a current row space)
   * @param ctx[in], execute context
   * @return if success, return OB_SUCCESS, otherwise, return errno
   */
  virtual int init_op_ctx(ObExecContext& ctx) const;
  /**
   * @brief called by get_next_row(), get a row from the child operator or row_store
   * @param ctx[in], execute context
   * @param row[out], ObSqlRow an obj array and row_size
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
  /*
   * ObExprValuesWithChild's child is ObSequence for insert stmt with sequence.
   */
  int try_get_next_row(ObExecContext& ctx) const;
  /*
   * misc
   */
  virtual int64_t to_string_kv(char* buf, const int64_t buf_len) const;
  int calc_next_row(ObExecContext& ctx) const;
  int64_t get_serialize_size_(const ObPhyOpSeriCtx& seri_ctx) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprValuesWithChild);

private:
  // data members
  common::ObFixedArray<ObSqlExpression*, common::ObIAllocator> values_;
};
}  // end namespace sql
}  // end namespace oceanbase

#endif /* OCEANBASE_SQL_ENGINE_DML_EXPR_VALUES_WITH_CHILD_H */
