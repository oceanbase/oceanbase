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

#ifndef OCEANBASE_SQL_OB_CATALOG_STMT_H_
#define OCEANBASE_SQL_OB_CATALOG_STMT_H_

#include "share/ob_rpc_struct.h"
#include "sql/resolver/ddl/ob_ddl_stmt.h"
namespace oceanbase
{
namespace sql
{
class ObCatalogStmt : public ObDDLStmt
{
public:
  ObCatalogStmt();
  virtual ~ObCatalogStmt();
  virtual obrpc::ObCatalogDDLArg &get_ddl_arg() { return catalog_arg_; }

  TO_STRING_KV(K_(catalog_arg));
private:
  obrpc::ObCatalogDDLArg catalog_arg_;
  DISALLOW_COPY_AND_ASSIGN(ObCatalogStmt);
};

}//namespace sql
}//namespace oceanbase
#endif //OCEANBASE_SQL_OB_CATALOG_STMT_H_
