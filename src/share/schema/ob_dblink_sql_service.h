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

#ifndef OCEANBASE_SHARE_SCHEMA_OB_DBLINK_SQL_SERVICE_H_
#define OCEANBASE_SHARE_SCHEMA_OB_DBLINK_SQL_SERVICE_H_

#include "ob_ddl_sql_service.h"
#include "share/ob_dml_sql_splicer.h"
#include "sql/dblink/ob_dblink_utils.h"

namespace oceanbase
{
namespace common
{
class ObString;
class ObISQLClient;
}
namespace share
{
namespace schema
{
class ObDbLinkInfo;

class ObDbLinkSqlService : public ObDDLSqlService
{
public:
  ObDbLinkSqlService(ObSchemaService &schema_service)
    : ObDDLSqlService(schema_service), dblink_proxy_(nullptr) {}
  virtual ~ObDbLinkSqlService() {}
public:
  int insert_dblink(const ObDbLinkBaseInfo &dblink_info,
                    const int64_t is_deleted,
                    common::ObISQLClient &sql_client,
                    const common::ObString *ddl_stmt_str);
  int delete_dblink(const uint64_t tenant_id,
                    const uint64_t dblink_id,
                    common::ObISQLClient &sql_client);
  void set_dblink_proxy(ObDbLinkProxy *dblink_proxy) { dblink_proxy_ = dblink_proxy; }
  int get_link_table_schema(const ObDbLinkSchema *dblink_schema,
                            const common::ObString &database_name,
                            const common::ObString &table_name,
                            common::ObIAllocator &allocator,
                            ObTableSchema *&table_schema,
                            sql::ObSQLSessionInfo *session_info,
                            const ObString &dblink_name,
                            bool is_reverse_link,
                            uint64_t *current_scn,
                            bool &is_under_oracle12c);
  template<typename T>
  int fetch_link_table_info(common::sqlclient::dblink_param_ctx &param_ctx,
                            sql::DblinkGetConnType conn_type,
                            const common::ObString &database_name,
                            const common::ObString &table_name,
                            ObIAllocator &alloctor,
                            T *&table_schema,
                            sql::ObSQLSessionInfo *session_info,
                            const ObString &dblink_name,
                            sql::ObReverseLink *reverse_link,
                            uint64_t *current_scn,
                            bool &is_under_oracle12c);
  template<typename T>
  int generate_link_table_schema(const common::sqlclient::dblink_param_ctx &param_ctx,
                                 sql::DblinkGetConnType conn_type,
                                 const ObString &database_name,
                                 const ObString &table_name,
                                 ObIAllocator &allocator,
                                 T *&table_schema,
                                 const sql::ObSQLSessionInfo *session_info,
                                 common::sqlclient::ObISQLConnection *dblink_conn,
                                 const common::sqlclient::ObMySQLResult *col_meta_result);
  int fetch_link_current_scn(const common::sqlclient::dblink_param_ctx &param_ctx,
                             sql::DblinkGetConnType conn_type,
                             ObIAllocator &allocator,
                             common::sqlclient::ObISQLConnection *dblink_conn,
                             sql::ObReverseLink *reverse_link,
                             uint64_t &current_scn);
  int try_mock_link_table_column(ObTableSchema &table_schema);

  static int convert_idenfitier_charset(ObIAllocator &alloc,
                                        const ObString &in,
                                        const sql::ObSQLSessionInfo *session_info,
                                        ObString &out);

private:
  int add_pk_columns(const uint64_t tenant_id,
                     const uint64_t dblink_id,
                     ObDMLSqlSplicer &dml);
  int add_normal_columns(const ObDbLinkBaseInfo &dblink_info,
                         ObDMLSqlSplicer &dml);
  int add_history_columns(const ObDbLinkBaseInfo &dblink_info,
                          int64_t is_deleted,
                          ObDMLSqlSplicer &dml);

  common::ObDbLinkProxy *dblink_proxy_;
  DISALLOW_COPY_AND_ASSIGN(ObDbLinkSqlService);
};


} //end of namespace schema
} //end of namespace share
} //end of namespace oceanbase

#endif //OCEANBASE_SHARE_SCHEMA_OB_DBLINK_SQL_SERVICE_H_
