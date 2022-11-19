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

#ifndef OCEANBASE_SQL_RESOLVER_EMPTY_QUERY_STMT_H_
#define OCEANBASE_SQL_RESOLVER_EMPTY_QUERY_STMT_H_

#include "sql/resolver/ob_stmt.h"
#include "sql/resolver/cmd/ob_cmd_stmt.h"
namespace oceanbase
{
namespace sql
{
class ObEmptyQueryStmt : public ObCMDStmt
{
public:
  ObEmptyQueryStmt() : ObCMDStmt(stmt::T_EMPTY_QUERY)
  {
  }

  virtual ~ObEmptyQueryStmt()
  {
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObEmptyQueryStmt);
};

} // sql
} // oceanbase
#endif /*OCEANBASE_SQL_RESOLVER_EMPTY_QUERY_STMT_H_*/
