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

#ifndef OCEANBASE_ROOTSERVER_MVIEW_OB_MVIEW_ALTER_SERVICE_H_
#define OCEANBASE_ROOTSERVER_MVIEW_OB_MVIEW_ALTER_SERVICE_H_

#include "lib/ob_define.h"
#include "share/schema/ob_table_schema.h"
#include "src/share/ob_rpc_struct.h"
#include "rootserver/ob_ddl_service.h"

namespace oceanbase
{
namespace rootserver
{
class ObMviewAlterService
{
public:
  static int alter_mview_or_mlog_in_trans(obrpc::ObAlterTableArg &alter_table_arg,
                                          obrpc::ObAlterTableRes &res,
                                          ObSchemaGetterGuard &schema_guard,
                                          share::schema::ObMultiVersionSchemaService *schema_service,
                                          common::ObMySQLProxy *sql_proxy,
                                          const uint64_t tenant_data_version);
  static int update_mlog_in_modify_column(const share::schema::ObTableSchema &new_table_schema,
                                          share::schema::ObSchemaGetterGuard &schema_guard,
                                          ObDDLOperator &ddl_operator,
                                          common::ObMySQLTransaction &trans);
  static int update_mview_in_modify_column(const share::schema::ObTableSchema &new_table_schema,
                                           share::schema::ObSchemaGetterGuard &schema_guard,
                                           ObDDLOperator &ddl_operator,
                                           common::ObMySQLTransaction &trans);

private:
  static int alter_mview_attributes(const uint64_t tenant_id,
                                    const ObTableSchema *orig_table_schema,
                                    obrpc::ObAlterTableArg &alter_table_arg,
                                    ObDDLOperator &ddl_operator, ObSchemaGetterGuard &schema_guard,
                                    ObDDLSQLTransaction &trans);
  static int alter_mlog_attributes(const uint64_t tenant_id, const ObTableSchema *orig_table_schema,
                                   obrpc::ObAlterTableArg &alter_table_arg,
                                   ObDDLOperator &ddl_operator, ObSchemaGetterGuard &schema_guard,
                                   ObDDLSQLTransaction &trans);
  static int update_mview_with_new_table(const uint64_t mv_id,
                                         const share::schema::ObTableSchema &new_table_schema,
                                         share::schema::ObSchemaGetterGuard &schema_guard,
                                         ObDDLOperator &ddl_operator,
                                         common::ObMySQLTransaction &trans);
  static int rebuild_mv_schema_with_new_table(share::schema::ObSchemaGetterGuard &schema_guard,
                                              const share::schema::ObTableSchema &orig_mv_schema,
                                              const share::schema::ObTableSchema &new_table_schema,
                                              share::schema::ObTableSchema &mv_schema);
};

} // namespace rootserver
} // namespace oceanbase
#endif
