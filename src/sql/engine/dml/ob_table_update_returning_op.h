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

#ifndef OCEANBASE_DML_OB_TABLE_UPDATE_RETURNING_OP_H_
#define OCEANBASE_DML_OB_TABLE_UPDATE_RETURNING_OP_H_

#include "sql/engine/dml/ob_table_update_op.h"

namespace oceanbase {
namespace sql {

class ObTableUpdateReturningOpInput : public ObTableUpdateOpInput {
  OB_UNIS_VERSION_V(1);

public:
  using ObTableUpdateOpInput::ObTableUpdateOpInput;

  virtual int init(ObTaskInfo& task_info) override
  {
    return ObTableUpdateOpInput::init(task_info);
  }
};

class ObTableUpdateReturningSpec : public ObTableUpdateSpec {
  OB_UNIS_VERSION_V(1);

public:
  using ObTableUpdateSpec::ObTableUpdateSpec;
};

class ObTableUpdateReturningOp : public ObTableUpdateOp {
public:
  using ObTableUpdateOp::ObTableUpdateOp;

  int get_next_row() override;

  // Do update all rows in open() phase.
  // We keep row update in get_next_row(), only initialize partition key here.
  int do_table_update() override;
};

}  // end namespace sql
}  // end namespace oceanbase

#endif  // OCEANBASE_DML_OB_TABLE_UPDATE_RETURNING_OP_H_
