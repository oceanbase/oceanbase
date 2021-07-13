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

#ifndef OCEANBASE_SQL_ENGINE_MERGE_UNION_H_
#define OCEANBASE_SQL_ENGINE_MERGE_UNION_H_

#include "sql/engine/set/ob_merge_set_operator.h"

namespace oceanbase {
namespace common {
class ObNewRow;
}
namespace sql {
class ObMergeUnion : public ObMergeSetOperator {
private:
  class ObMergeUnionCtx;

public:
  explicit ObMergeUnion(common::ObIAllocator& alloc);
  virtual ~ObMergeUnion();
  virtual void reset();
  virtual void reuse();
  virtual int rescan(ObExecContext& ctx) const;

  virtual void set_distinct(bool is_distinct);

private:
  int get_first_row(ObExecContext& ctx, ObMergeUnionCtx& union_ctx, const common::ObNewRow*& row) const;
  /**
   * @brief get next row expected the same row in the same group
   * @param ctx[in], execute context
   * @param row[out], output row
   * @return if success, return OB_SUCCESS, otherwise, return errno
   */
  int distinct_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;
  /**
   * @brief the function to get next row without distinct attribution
   * @param ctx[in], execute context
   * @param row[out], output row
   * @return if success, return OB_SUCCESS, otherwise, return errno
   */
  int all_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;
  /**
   * @brief create operator context, only child operator can know it's specific operator type,
   * so must be overwrited by child operator,
   * @param ctx[in], execute context
   * @param op_ctx[out], the pointer of operator context
   * @return if success, return OB_SUCCESS, otherwise, return errno
   */
  virtual int inner_create_operator_ctx(ObExecContext& ctx, ObPhyOperatorCtx*& op_ctx) const;
  virtual int init_op_ctx(ObExecContext& ctx) const;
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

  typedef int (ObMergeUnion::*GetNextRowFunc)(ObExecContext& ctx, const common::ObNewRow*& row) const;

private:
  GetNextRowFunc get_next_row_func_;
  DISALLOW_COPY_AND_ASSIGN(ObMergeUnion);
};
}  // end namespace sql
}  // end namespace oceanbase

#endif /* OCEANBASE_SQL_ENGINE_MERGE_UNION_H_ */
