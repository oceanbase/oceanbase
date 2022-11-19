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

#ifndef OCEANBASE_BASIC_OB_TOPK_OP_H_
#define OCEANBASE_BASIC_OB_TOPK_OP_H_

#include "sql/engine/ob_operator.h"

namespace oceanbase
{
namespace sql
{

class ObTopKSpec : public ObOpSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObTopKSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type);

  bool is_valid() const;

  INHERIT_TO_STRING_KV("op_spec", ObOpSpec, K_(minimum_row_count), K_(topk_precision),
                        KPC_(org_limit), KPC_(org_offset));

  int64_t minimum_row_count_;
  int64_t topk_precision_;
  ObExpr *org_limit_;
  ObExpr *org_offset_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTopKSpec);
};

class ObTopKOp : public ObOperator
{
public:
  ObTopKOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);

  virtual int inner_open() override;
  virtual int inner_rescan() override;

  virtual int inner_get_next_row() override;

  virtual void destroy() override { ObOperator::destroy(); }

private:
  // 根据child_以及limit/offset设置topk_final_count_
  // 只会在rescan后或者第一次get_next_row()后被调用
  int get_topk_final_count();

  DISALLOW_COPY_AND_ASSIGN(ObTopKOp);

  int64_t topk_final_count_;
  int64_t output_count_;
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_BASIC_OB_TOPK_OP_H_
