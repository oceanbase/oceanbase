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
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/resolver/ob_cmd.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace sql
{

class ObLockTableStmt : public ObDMLStmt, public ObICmd
{
public:
  explicit ObLockTableStmt()
    : ObDMLStmt(stmt::T_LOCK_TABLE),
      lock_mode_(0),
      wait_lock_seconds_(-1)
  {}
  virtual ~ObLockTableStmt()
  {}
  virtual int get_cmd_type() const { return get_stmt_type(); }

  void set_lock_mode(const int64_t lock_mode) { lock_mode_ = lock_mode; }
  void set_wait_lock_seconds(const int64_t wait_lock_seconds) { wait_lock_seconds_ = wait_lock_seconds; }
  int64_t get_lock_mode() const { return lock_mode_; }
  int64_t get_wait_lock_seconds() const { return wait_lock_seconds_; }
private:
  int64_t lock_mode_;
  int64_t wait_lock_seconds_;
  DISALLOW_COPY_AND_ASSIGN(ObLockTableStmt);
};
} // namespace sql
} // namespace oceanbase

#endif
