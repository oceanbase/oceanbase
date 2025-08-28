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
#include "lib/container/ob_se_array.h"

namespace oceanbase
{
namespace sql
{

struct ObMySQLLockNode
{
public:
  ObMySQLLockNode() : table_item_(NULL), lock_mode_(0) {}
  ~ObMySQLLockNode() { table_item_ = NULL; }
  bool is_valid() const
  {
    return (NULL != table_item_
            && 0 != lock_mode_);
  }
  TO_STRING_KV(KPC_(table_item), K_(lock_mode));
public:
  TableItem *table_item_;
  int64_t lock_mode_;
};

class ObLockTableStmt : public ObDMLStmt, public ObICmd
{
public:
  enum {
        INVALID_STMT_TYPE =       0,
        ORACLE_LOCK_TABLE_STMT =  1,
        MYSQL_LOCK_TABLE_STMT =   2,
        MYSQL_UNLOCK_TABLE_STMT = 3
  };
  explicit ObLockTableStmt()
    : ObDMLStmt(stmt::T_LOCK_TABLE),
      lock_stmt_type_(0),
      lock_mode_(0),
      wait_lock_seconds_(-1),
      mysql_lock_list_()
  {}
  virtual ~ObLockTableStmt()
  {}
  virtual int get_cmd_type() const { return get_stmt_type(); }

  void set_lock_mode(const int64_t lock_mode) { lock_mode_ = lock_mode; }
  void set_wait_lock_seconds(const int64_t wait_lock_seconds) { wait_lock_seconds_ = wait_lock_seconds; }
  void set_lock_stmt_type(const int64_t stmt_type) { lock_stmt_type_ = stmt_type; }
  int64_t get_lock_mode() const { return lock_mode_; }
  int64_t get_wait_lock_seconds() const { return wait_lock_seconds_; }
  virtual int check_is_simple_lock_stmt(bool &is_valid) const override { 
    is_valid = true;
    return common::OB_SUCCESS;  
  };
  int64_t get_lock_stmt_type() const { return lock_stmt_type_; }
  const ObIArray<ObMySQLLockNode> &get_mysql_lock_list() const { return mysql_lock_list_; }
  int add_mysql_lock_node(const ObMySQLLockNode &node);
private:
  // for oracle lock
  int64_t lock_stmt_type_;
  int64_t lock_mode_;
  int64_t wait_lock_seconds_;

  // for mysql lock
  common::ObSEArray<ObMySQLLockNode, 2> mysql_lock_list_;
  DISALLOW_COPY_AND_ASSIGN(ObLockTableStmt);
};
} // namespace sql
} // namespace oceanbase

#endif
