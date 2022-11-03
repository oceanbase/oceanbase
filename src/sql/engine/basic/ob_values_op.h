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

#ifndef OCEANBASE_BASIC_OB_VALUES_OP_H_
#define OCEANBASE_BASIC_OB_VALUES_OP_H_


#include "common/row/ob_row_store.h"
#include "sql/engine/ob_operator.h"

namespace oceanbase
{
namespace sql
{

//
// Values operator is only use in explain statement to hold the explain result.
//
class ObValuesSpec : public ObOpSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObValuesSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type);

  common::ObRowStore row_store_;
};

class ObValuesOp : public ObOperator
{
public:
  ObValuesOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);

  virtual int inner_open() override;
  virtual int inner_rescan() override;

  virtual int inner_get_next_row() override;

  virtual void destroy() override { ObOperator::destroy(); }

private:
  common::ObNewRow cur_row_;
  common::ObRowStore::Iterator row_store_it_;
};

} // end namespace sql
} // end namespace oceanbase
#endif // OCEANBASE_BASIC_OB_VALUES_OP_H_
