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

#ifndef OCEANBASE_SQL_ENGINE_DML_OB_VALUES_H_
#define OCEANBASE_SQL_ENGINE_DML_OB_VALUES_H_
#include "common/row/ob_row_store.h"
#include "sql/engine/ob_no_children_phy_operator.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase {
namespace sql {
class ObValues : public ObNoChildrenPhyOperator {
  OB_UNIS_VERSION(1);
  class ObValuesCtx;

public:
  explicit ObValues(common::ObIAllocator& alloc) : ObNoChildrenPhyOperator(alloc), row_store_(alloc)
  {}
  ~ObValues()
  {}

  void reset();
  void reuse();
  int add_row(const common::ObNewRow& row);

  int rescan(ObExecContext& ctx) const;
  int set_row_store(const common::ObRowStore& row_store);

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
  DISALLOW_COPY_AND_ASSIGN(ObValues);

private:
  common::ObRowStore row_store_;
};
}  // namespace sql
}  // namespace oceanbase

#endif /* OCEANBASE_SQL_ENGINE_DML_OB_VALUES_H_ */
