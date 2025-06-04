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

#ifndef OCEANBASE_ROOTSERVER_OB_CATALOG_DDL_OPERATOR_H_
#define OCEANBASE_ROOTSERVER_OB_CATALOG_DDL_OPERATOR_H_

#include "share/schema/ob_schema_service.h"
#include "share/schema/ob_catalog_sql_service.h"
#include "share/ob_rpc_struct.h"

namespace oceanbase
{
// namespace share
// {
// class ObCatalogSqlService;
// }

namespace rootserver
{
class ObCatalogDDLOperator
{
public:
  ObCatalogDDLOperator(share::schema::ObMultiVersionSchemaService &schema_service,
                       common::ObMySQLProxy &sql_proxy)
    : schema_service_(schema_service),
      sql_proxy_(sql_proxy)
  {}
  virtual ~ObCatalogDDLOperator() {}

  int handle_catalog_function(share::schema::ObCatalogSchema &schema,
                              common::ObMySQLTransaction &trans,
                              const share::schema::ObSchemaOperationType ddl_type,
                              const common::ObString &ddl_stmt_str,
                              bool if_exist,
                              bool if_not_exist,
                              share::schema::ObSchemaGetterGuard &schema_guard,
                              uint64_t user_id);
  int grant_or_revoke_after_ddl(ObCatalogSchema &schema,
                                ObMySQLTransaction &trans,
                                const share::schema::ObSchemaOperationType ddl_type,
                                ObSchemaGetterGuard &schema_guard,
                                uint64_t user_id);
  int grant_revoke_catalog(const ObCatalogPrivSortKey &catalog_priv_key,
                           const ObPrivSet priv_set,
                           const bool grant,
                           const common::ObString &ddl_stmt_str,
                           common::ObMySQLTransaction &trans);
private:
  share::schema::ObMultiVersionSchemaService &schema_service_;
  common::ObMySQLProxy &sql_proxy_;
};

}//end namespace rootserver
}//end namespace oceanbase
#endif //OCEANBASE_ROOTSERVER_OB_CATALOG_DDL_OPERATOR_H_