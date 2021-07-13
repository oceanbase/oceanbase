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

#ifndef OCEANBASE_SRC_SQL_ENGINE_AGGREGATE_OB_SCALAR_AGGREGATE_H_
#define OCEANBASE_SRC_SQL_ENGINE_AGGREGATE_OB_SCALAR_AGGREGATE_H_

#include "common/row/ob_row_store.h"
#include "sql/engine/aggregate/ob_groupby.h"
#include "sql/engine/aggregate/ob_aggregate_function.h"
namespace oceanbase {
namespace common {
class ObNewRow;
}  // namespace common

namespace sql {
class ObScalarAggregate : public ObGroupBy {
protected:
  class ObScalarAggregateCtx;

public:
  explicit ObScalarAggregate(common::ObIAllocator& alloc);
  virtual ~ObScalarAggregate();
  virtual void reset();
  virtual void reuse();
  virtual int rescan(ObExecContext& ctx) const;
  virtual int switch_iterator(ObExecContext& ctx) const override;

protected:
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
   * @brief create operator context, only child operator can know it's specific operator type,
   * so must be overwrited by child operator,
   * @param ctx[in], execute context
   * @param op_ctx[out], the pointer of operator context
   * @return if success, return OB_SUCCESS, otherwise, return errno
   */
  virtual int inner_create_operator_ctx(ObExecContext& ctx, ObPhyOperatorCtx*& op_ctx) const;
  /**
   * @brief for specified phy operator to print it's member variable with json key-value format
   * @param buf[in] to string buffer
   * @param buf_len[in] buffer length
   * @return if success, return the length used by print string, otherwise return 0
   */
  virtual int64_t to_string_kv(char* buf, const int64_t buf_len) const;
  virtual int add_group_column_idx(int64_t column_idx, common::ObCollationType cs_type);

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObScalarAggregate);
};
}  // namespace sql
}  // namespace oceanbase

#endif /* OCEANBASE_SRC_SQL_ENGINE_AGGREGATE_OB_SCALAR_AGGREGATE_H_ */
