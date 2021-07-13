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

#ifndef OCEANBASE_SQL_OB_ALTER_BASELINE_STMT_H_
#define OCEANBASE_SQL_OB_ALTER_BASELINE_STMT_H_

#include "sql/resolver/ddl/ob_ddl_stmt.h"

namespace oceanbase {
namespace sql {
class ObAlterBaselineStmt : public ObDDLStmt {
public:
  ObAlterBaselineStmt() : ObDDLStmt(stmt::T_ALTER_BASELINE), alter_baseline_arg_()
  {}

  virtual obrpc::ObDDLArg& get_ddl_arg()
  {
    return alter_baseline_arg_;
  }
  TO_STRING_KV(K_(alter_baseline_arg));

public:
  obrpc::ObAlterPlanBaselineArg alter_baseline_arg_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObAlterBaselineStmt);
};
}  // namespace sql
}  // namespace oceanbase

#endif
