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

#ifndef _OB_MERGE_GROUPBY_H
#define _OB_MERGE_GROUPBY_H
#include "sql/engine/aggregate/ob_groupby.h"
namespace oceanbase {
namespace sql {
class ObMergeGroupBy : public ObGroupBy {
protected:
  class ObMergeGroupByCtx;

public:
  explicit ObMergeGroupBy(common::ObIAllocator& alloc);
  virtual ~ObMergeGroupBy();
  virtual int rescan(ObExecContext& ctx) const;

private:
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
  int rollup_and_calc_results(const int64_t group_id, ObMergeGroupByCtx* groupby_ctx) const;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObMergeGroupBy);
};
}  // end namespace sql
}  // end namespace oceanbase
#endif /* _OB_MERGE_GROUPBY_H */
