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

#ifndef OCEANBASE_SQL_ENGINE_MERGE_INTERSECT_H_
#define OCEANBASE_SQL_ENGINE_MERGE_INTERSECT_H_

#include "sql/engine/set/ob_merge_set_operator.h"
#include "common/row/ob_row.h"

namespace oceanbase {
namespace sql {
class ObMergeIntersect : public ObMergeSetOperator {
private:
  class ObMergeIntersectCtx;

public:
  explicit ObMergeIntersect(common::ObIAllocator& alloc);
  virtual ~ObMergeIntersect();
  virtual int rescan(ObExecContext& ctx) const;
  virtual void reset();
  virtual void reuse();

  virtual void set_distinct(bool is_distinct);

private:
  int distinct_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;
  int all_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;
  /**
   * @brief create operator context, only child operator can know it's specific operator type,
   * so must be overwrited by child operator,
   * @param ctx[in], execute context
   * @param op_ctx[out], the pointer of operator context
   * @return if success, return OB_SUCCESS, otherwise, return errno
   */
  virtual int inner_create_operator_ctx(ObExecContext& ctx, ObPhyOperatorCtx*& op_ctx) const;
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

private:
  typedef int (ObMergeIntersect::*GetNextRowFunc)(ObExecContext& ctx, const common::ObNewRow*& row) const;
  GetNextRowFunc get_next_row_func_;
  DISALLOW_COPY_AND_ASSIGN(ObMergeIntersect);
};
}  // end namespace sql
}  // end namespace oceanbase

#endif /* OCEANBASE_SQL_ENGINE_MERGE_INTERSECT_H_ */
