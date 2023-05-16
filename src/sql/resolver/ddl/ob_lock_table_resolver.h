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

#ifndef OCEANBASE_SQL_RESOLVER_DML_OB_LOCK_TABLE_RESOLVER_
#define OCEANBASE_SQL_RESOLVER_DML_OB_LOCK_TABLE_RESOLVER_

#include "sql/resolver/ddl/ob_lock_table_stmt.h"
#include "sql/resolver/dml/ob_dml_resolver.h"

namespace oceanbase
{
namespace sql
{

class ObLockTableStmt;

// NOTE: yanyuan.cxf LOCK TABLE is dml at oracle, but it does not have
// SQL plan, so we treat it as ddl operator.
class ObLockTableResolver : public ObDMLResolver
{
public:
  static const int64_t TABLE_LIST = 0;
  static const int64_t LOCK_MODE = 1;
  static const int64_t WAIT = 2;
public:
  explicit ObLockTableResolver(ObResolverParams &params)
    : ObDMLResolver(params)
    {}
  virtual ~ObLockTableResolver()
    {}
  virtual int resolve(const ParseNode &parse_tree);
  inline ObLockTableStmt *get_lock_table_stmt() { return static_cast<ObLockTableStmt*>(stmt_); }
private:
  int resolve_mysql_mode(const ParseNode &parse_tree);
  int resolve_oracle_mode(const ParseNode &parse_tree);
  int resolve_table_list(const ParseNode &table_list);
  int resolve_lock_mode(const ParseNode &parse_tree);
  int resolve_wait_lock(const ParseNode &parse_tree);

  DISALLOW_COPY_AND_ASSIGN(ObLockTableResolver);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_RESOLVER_DML_OB_LOCK_TABLE_RESOLVER_
