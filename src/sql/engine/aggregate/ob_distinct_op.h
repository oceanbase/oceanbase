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

#ifndef OCEANBASE_SRC_SQL_ENGINE_AGGREGATE_OB_DISTINCT_OP_H_
#define OCEANBASE_SRC_SQL_ENGINE_AGGREGATE_OB_DISTINCT_OP_H_

#include "sql/engine/ob_operator.h"
#include "share/datum/ob_datum_funcs.h"

namespace oceanbase
{
namespace sql
{

class ObDistinctSpec : public ObOpSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObDistinctSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type);

  INHERIT_TO_STRING_KV("op_spec", ObOpSpec,
                       K_(distinct_exprs), K_(is_block_mode), K_(cmp_funcs));

  // data members
  common::ObFixedArray<ObExpr*, common::ObIAllocator> distinct_exprs_;
  common::ObCmpFuncs cmp_funcs_;
  bool is_block_mode_;
  bool by_pass_enabled_;
};

/**
 * Distinct对于Hash和Merge来说没有啥公用的，所以暂时不实现一个DistinctOp基类
 **/

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SRC_SQL_ENGINE_AGGREGATE_OB_DISTINCT_OP_H_ */
