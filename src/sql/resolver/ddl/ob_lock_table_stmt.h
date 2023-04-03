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

#ifndef OCEANBASE_SQL_RESOLVER_DML_OB_LOCK_TABLE_STMT_
#define OCEANBASE_SQL_RESOLVER_DML_OB_LOCK_TABLE_STMT_
#include "sql/resolver/ddl/ob_ddl_stmt.h"
#include "sql/resolver/ob_cmd.h"

namespace oceanbase
{
namespace sql
{

class ObLockTableStmt : public ObStmt, public ObICmd
{
public:
  explicit ObLockTableStmt()
    : ObStmt(stmt::T_LOCK_TABLE),
      lock_mode_(0),
      table_id_(0)
  {}
  virtual ~ObLockTableStmt()
  {}
  virtual int get_cmd_type() const { return get_stmt_type(); }

  void set_lock_mode(const int64_t lock_mode) { lock_mode_ = lock_mode; }
  void set_table_id(const uint64_t table_id) { table_id_ = table_id; }
  int64_t get_lock_mode() const { return lock_mode_; }
  uint64_t get_table_id() const { return table_id_; }
private:
  int64_t lock_mode_;
  uint64_t table_id_;
  DISALLOW_COPY_AND_ASSIGN(ObLockTableStmt);
};


} // namespace sql
} // namespace oceanbase

#endif
