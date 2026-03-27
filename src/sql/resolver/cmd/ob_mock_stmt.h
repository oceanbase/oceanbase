/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_MOCK_STMT_H_
#define OB_MOCK_STMT_H_

#include "sql/resolver/cmd/ob_cmd_stmt.h"

namespace oceanbase
{
namespace sql
{
class ObMockStmt : public ObCMDStmt
{
public:
  ObMockStmt() : ObCMDStmt(stmt::T_NONE)
  {
  }
  virtual ~ObMockStmt() {}

  int add_stmt(stmt::StmtType stmt_type) {
    return stmt_types_.push_back(stmt_type);
  }

  const ObIArray<stmt::StmtType> &get_stmt_type_list() const {
    return stmt_types_;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObMockStmt);
  ObSEArray<stmt::StmtType, 16> stmt_types_;
};
}
}

#endif
