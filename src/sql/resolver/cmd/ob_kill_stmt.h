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
