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

#ifndef OCEANBASE_SRC_SQL_ENGINE_SUBQUERY_OB_UNPIVOT_H_
#define OCEANBASE_SRC_SQL_ENGINE_SUBQUERY_OB_UNPIVOT_H_
#include "sql/engine/ob_single_child_phy_operator.h"
#include "sql/resolver/dml/ob_dml_stmt.h"

namespace oceanbase {
namespace sql {
class ObUnpivot : public ObSingleChildPhyOperator {
  OB_UNIS_VERSION(1);

private:
  class ObUnpivotCtx;

public:
  explicit ObUnpivot(common::ObIAllocator& alloc);
  virtual ~ObUnpivot();
  virtual void reset();
  virtual int rescan(ObExecContext& ctx) const;

  int add_output_index(int64_t index);
  int init_output_index(int64_t count)
  {
    return init_array_size<>(output_indexs_, count);
  }

private:
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

public:
  common::ObFixedArray<int64_t, common::ObIAllocator> output_indexs_;
  ObUnpivotInfo unpivot_info_;

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObUnpivot);
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SRC_SQL_ENGINE_SUBQUERY_OB_UNPIVOT_H_ */
