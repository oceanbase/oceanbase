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

#ifndef OCEANBASE_ROOTSERVER_OB_OBJPRIV_MYSQL_DDL_OPERATOR_H_
#define OCEANBASE_ROOTSERVER_OB_OBJPRIV_MYSQL_DDL_OPERATOR_H_

#include "share/schema/ob_schema_service.h"
#include "share/schema/ob_priv_sql_service.h"
#include "share/ob_rpc_struct.h"

namespace oceanbase
{
namespace rootserver
{
class ObObjPrivMysqlDDLOperator
{
public:
  ObObjPrivMysqlDDLOperator(share::schema::ObMultiVersionSchemaService &schema_service,
                      common::ObMySQLProxy &sql_proxy)
    : schema_service_(schema_service),
      sql_proxy_(sql_proxy)
  {}
  virtual ~ObObjPrivMysqlDDLOperator() {}
  int grant_object(const ObObjMysqlPrivSortKey &object_priv_key,
                   const ObPrivSet priv_set,
                   common::ObMySQLTransaction &trans,
                   const uint64_t option,
                   const bool gen_ddl_stmt,
                   const common::ObString &grantor = "",
                   const common::ObString &grantor_host = "");
  int revoke_object(const share::schema::ObObjMysqlPrivSortKey &object_priv_key,
                    const ObPrivSet priv_set,
                    common::ObMySQLTransaction &trans,
                    const bool report_error = true,
                    const bool gen_ddl_stmt = true,
                    const common::ObString &grantor = "",
                    const common::ObString &grantor_host = "");
  static int drop_obj_mysql_privs(const uint64_t tenant_id,
                            const ObString &obj_name,
                            const uint64_t obj_type,
                            common::ObMySQLTransaction &trans,
                            share::schema::ObMultiVersionSchemaService &schema_service,
                            share::schema::ObSchemaGetterGuard &schema_guard);
private:
  int drop_obj_mysql_privs(const uint64_t tenant_id,
                           const ObString& obj_name,
                           const uint64_t obj_ypte,
                           common::ObMySQLTransaction &trans);
  share::schema::ObMultiVersionSchemaService &schema_service_;
  common::ObMySQLProxy &sql_proxy_;
};

}
}
#endif //OCEANBASE_ROOTSERVER_OB_OBJPRIV_MYSQL_DDL_OPERATOR_H_