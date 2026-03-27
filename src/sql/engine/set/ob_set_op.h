/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_BASIC_OB_SET_OB_SET_OP_H_
#define OCEANBASE_BASIC_OB_SET_OB_SET_OP_H_

#include "sql/engine/ob_operator.h"
#include "sql/engine/sort/ob_sort_basic_info.h"

namespace oceanbase
{
namespace sql
{

class ObSetSpec : public ObOpSpec
{
OB_UNIS_VERSION_V(1);
public:
  ObSetSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type);

  INHERIT_TO_STRING_KV("op_spec", ObOpSpec, K_(is_distinct), K_(sort_collations));
  bool is_distinct_;
  ExprFixedArray set_exprs_;
  ObSortCollations sort_collations_;
  ObSortFuncs sort_cmp_funs_;
};

/**
 * MergeSet和HashSet没有什么公用部分，所以这里暂不弄一个ObSetOp
 **/
// class ObSetOp : public ObOperator
// {
// public:
//   ObSetOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
//     : ObOperator(exec_ctx, spec, input)
//   {}
//   virtual ~ObSetOp() = 0;
// };

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_BASIC_OB_SET_OB_SET_OP_H_
