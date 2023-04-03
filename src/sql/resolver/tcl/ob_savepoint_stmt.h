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

#ifndef OCEANBASE_SQL_RESOLVER_TCL_OB_SAVEPOINT_STMT_
#define OCEANBASE_SQL_RESOLVER_TCL_OB_SAVEPOINT_STMT_

#include "sql/resolver/tcl/ob_tcl_stmt.h"

namespace oceanbase
{
namespace sql
{

class ObSavePointStmt : public ObTCLStmt
{
public:
  explicit ObSavePointStmt(stmt::StmtType type)
    : ObTCLStmt(type),
      sp_name_()
  {}
  virtual ~ObSavePointStmt()
  {}
  int set_sp_name(const char *str_value, int64_t str_len);
  inline const common::ObString &get_sp_name() const { return sp_name_; }
private:
  common::ObString sp_name_;
  DISALLOW_COPY_AND_ASSIGN(ObSavePointStmt);
};

class ObCreateSavePointStmt : public ObSavePointStmt
{
public:
  explicit ObCreateSavePointStmt()
    : ObSavePointStmt(stmt::T_CREATE_SAVEPOINT)
  {}
  virtual ~ObCreateSavePointStmt()
  {}

private:
  DISALLOW_COPY_AND_ASSIGN(ObCreateSavePointStmt);
};

class ObRollbackSavePointStmt : public ObSavePointStmt
{
public:
  explicit ObRollbackSavePointStmt()
    : ObSavePointStmt(stmt::T_ROLLBACK_SAVEPOINT)
  {}
  virtual ~ObRollbackSavePointStmt()
  {}

private:
  DISALLOW_COPY_AND_ASSIGN(ObRollbackSavePointStmt);
};

class ObReleaseSavePointStmt : public ObSavePointStmt
{
public:
  explicit ObReleaseSavePointStmt()
    : ObSavePointStmt(stmt::T_RELEASE_SAVEPOINT)
  {}
  virtual ~ObReleaseSavePointStmt()
  {}

private:
  DISALLOW_COPY_AND_ASSIGN(ObReleaseSavePointStmt);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_RESOLVER_TCL_OB_SAVEPOINT_STMT_

