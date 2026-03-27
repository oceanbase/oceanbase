/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
