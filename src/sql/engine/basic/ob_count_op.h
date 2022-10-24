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

#ifndef OCEANBASE_BASIC_OB_COUNT_OP_H_
#define OCEANBASE_BASIC_OB_COUNT_OP_H_

#include "sql/engine/ob_operator.h"

namespace oceanbase
{
namespace sql
{

class ObCountSpec : public ObOpSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObCountSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type);
  INHERIT_TO_STRING_KV("op_spec", ObOpSpec,
                       K_(rownum_limit), K_(anti_monotone_filters));

  ObExpr *rownum_limit_;
  ExprFixedArray anti_monotone_filters_;
};

class ObCountOp : public ObOperator
{
public:
  ObCountOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);

  virtual int inner_open() override;
  virtual int inner_rescan() override;
  virtual int inner_switch_iterator() override;

  // Increment %cur_rownum_ in get_next_row()
  virtual int get_next_row() override;

  virtual int inner_get_next_row() override;

  virtual void destroy() override { ObOperator::destroy(); }

  int64_t get_cur_rownum() const { return cur_rownum_; }
private:
  // reset default value of %cur_rownum_ && %rownum_limit_
  void reset_default();
  int get_rownum_limit();

private:
  int64_t cur_rownum_;
  int64_t rownum_limit_;
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_BASIC_OB_COUNT_OP_H_
