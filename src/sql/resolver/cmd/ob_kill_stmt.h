/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_KILL_STMT_H_
#define OB_KILL_STMT_H_

#include "sql/resolver/cmd/ob_cmd_stmt.h"
namespace oceanbase
{
namespace sql
{
class ObKillStmt : public ObCMDStmt
{
public:
  ObKillStmt():ObCMDStmt(stmt::T_KILL),
               is_query_(false),
               is_alter_system_kill_(false),
               value_expr_(NULL)
  {
  }
  virtual ~ObKillStmt()
  {
  }

  inline void set_is_query(bool is_query) { is_query_ = is_query; }
  inline void set_is_alter_system_kill(bool is_alter_system_kill) { is_alter_system_kill_ = is_alter_system_kill; }
  inline void set_value_expr(ObRawExpr *value_expr) { value_expr_ = value_expr; }
  inline ObRawExpr *get_value_expr() const { return value_expr_; }
  inline bool is_query() const { return is_query_; }
  inline bool is_alter_system_kill() const { return is_alter_system_kill_; }
private:
  bool is_query_;
  bool is_alter_system_kill_;
  ObRawExpr *value_expr_;
  DISALLOW_COPY_AND_ASSIGN(ObKillStmt);
};


} // sql
} // oceanbase
#endif
