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

#ifndef OCEANBASE_SQL_ENGINE_DML_EXPR_VALUES_H
#define OCEANBASE_SQL_ENGINE_DML_EXPR_VALUES_H
#include "lib/charset/ob_charset.h"
#include "sql/engine/ob_no_children_phy_operator.h"
#include "sql/engine/expr/ob_sql_expression.h"
namespace oceanbase {
namespace sql {
class ObIterExprRangeParam;

class ObExprValuesInput : public ObIPhyOperatorInput {
  OB_UNIS_VERSION_V(1);

public:
  ObExprValuesInput() : partition_id_values_(0)
  {}
  virtual ~ObExprValuesInput()
  {}
  virtual int init(ObExecContext&, ObTaskInfo&, const ObPhyOperator&)
  {
    return OB_SUCCESS;
  }
  virtual void reset() override
  {}
  virtual ObPhyOperatorType get_phy_op_type() const
  {
    return PHY_EXPR_VALUES;
  }

public:
  int64_t partition_id_values_;
};

class ObExprValues : public ObNoChildrenPhyOperator {
  class ObExprValuesCtx;

public:
  explicit ObExprValues(common::ObIAllocator& alloc);
  virtual ~ObExprValues();

  virtual int rescan(ObExecContext& ctx) const;
  virtual int switch_iterator(ObExecContext& ctx) const;
  int init_value_count(int64_t value_cnt);
  int add_value(ObSqlExpression* value);
  int init_range_params(int64_t range_param_count);
  int add_range_param(ObIterExprRangeParam* range_param_expr);
  int64_t get_size() const
  {
    return values_.count();
  }
  virtual int create_operator_input(ObExecContext& ctx) const override;
  virtual int serialize(char* buf, int64_t buf_len, int64_t& pos, ObPhyOpSeriCtx& seri_ctx) const;
  virtual int64_t get_serialize_size(const ObPhyOpSeriCtx& seri_ctx) const;
  int64_t get_serialize_size_(const ObPhyOpSeriCtx& seri_ctx) const;
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
  virtual int64_t to_string_kv(char* buf, const int64_t buf_len) const;
  int calc_next_row(ObExecContext& ctx) const;
  int calc_next_row_by_range_param(ObExprValuesCtx& value_ctx, const common::ObNewRow*& row) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprValues);

private:
  // data members
  common::ObFixedArray<ObSqlExpression*, common::ObIAllocator> values_;
  common::ObFixedArray<ObIterExprRangeParam*, common::ObIAllocator> range_params_;
};
}  // end namespace sql
}  // end namespace oceanbase

#endif /* OCEANBASE_SQL_ENGINE_DML_EXPR_VALUES_H */
