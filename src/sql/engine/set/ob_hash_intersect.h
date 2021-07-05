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

#ifndef OB_HASH_INTERSECT_H
#define OB_HASH_INTERSECT_H

#include "sql/engine/set/ob_hash_set_operator.h"

namespace oceanbase {
namespace sql {
class ObHashIntersect : public ObHashSetOperator {
private:
  class ObHashIntersectCtx;

public:
  explicit ObHashIntersect(common::ObIAllocator& alloc);

  virtual ~ObHashIntersect();

private:
  int build_hash_table_by_part(ObExecContext& ctx, ObHashIntersectCtx* intersect_ctx) const;
  int get_next_group_part(ObExecContext& ctx, ObHashIntersectCtx* intersect_ctx) const;
  virtual int inner_create_operator_ctx(ObExecContext& ctx, ObPhyOperatorCtx*& op_ctx) const;
  virtual int inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;
  virtual int inner_open(ObExecContext& ctx) const;
  virtual int inner_close(ObExecContext& ctx) const;

  DISALLOW_COPY_AND_ASSIGN(ObHashIntersect);
};
}  // namespace sql
}  // end namespace oceanbase

#endif  // OB_HASH_INTERSECT_H
