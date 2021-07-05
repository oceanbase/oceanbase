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

#ifndef OCEANBASE_DML_OB_TABLE_INSERT_RETURNING_OP_H_
#define OCEANBASE_DML_OB_TABLE_INSERT_RETURNING_OP_H_

#include "sql/engine/dml/ob_table_insert_op.h"

namespace oceanbase {
namespace sql {

class ObTableInsertReturningOpInput : public ObTableInsertOpInput {
  OB_UNIS_VERSION_V(1);

public:
  using ObTableInsertOpInput::ObTableInsertOpInput;
};

class ObTableInsertReturningSpec : public ObTableInsertSpec {
  OB_UNIS_VERSION_V(1);

public:
  using ObTableInsertSpec::ObTableInsertSpec;
};

class ObTableInsertReturningOp : public ObTableInsertOp {
public:
  using ObTableInsertOp::ObTableInsertOp;

  virtual int inner_open() override;
  virtual int get_next_row() override;
  virtual int inner_close() override;

private:
  common::ObNewRow new_row_;
};

}  // end namespace sql
}  // end namespace oceanbase
#endif  // OCEANBASE_DML_OB_TABLE_INSERT_RETURNING_OP_H_
