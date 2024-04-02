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

#ifndef OCEANBASE_ROOTSERVER_OB_DDL_SERVICE_H_
#define OCEANBASE_ROOTSERVER_OB_DDL_SERVICE_H_ 1

#include "lib/utility/ob_macro_utils.h"
#include "share/schema/ob_priv_type.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace obrpc
{
class ObAccountArg;
class ObSetPasswdArg;
}
namespace common
{
class ObSqlString;
class ObString;
}
namespace rootserver
{

class ObDDLSqlGenerator
{
public:
  ObDDLSqlGenerator() {}

  static const char* ora_obj_priv_names[];
  static int raw_privs_to_name_ora(const share::ObRawObjPrivArray &obj_priv_array, 
                                   common::ObSqlString &priv_str);
  static int gen_create_user_sql(const obrpc::ObAccountArg &account,
                                 const common::ObString &password,
                                 common::ObSqlString &sql_string);
  static int gen_alter_role_sql(const obrpc::ObAccountArg &account,
                                const common::ObString &password,
                                common::ObSqlString &sql_string);
  static int append_ssl_info_sql(const share::schema::ObSSLType &ssl_type,
                                 const common::ObString &ssl_cipher,
                                 const common::ObString &x509_issuer,
                                 const common::ObString &x509_subject,
                                 common::ObSqlString &sql_string);
  static int gen_set_passwd_sql(const obrpc::ObAccountArg &account,
                                const common::ObString &passwd,
                                common::ObSqlString &sql_string);
  static int gen_set_max_connections_sql(const obrpc::ObAccountArg &account,
                                        const uint64_t max_connections_per_hour,
                                        const uint64_t max_user_connections,
                                        common::ObSqlString &sql_string);

  static int gen_alter_user_require_sql(const obrpc::ObAccountArg &account,
                                        const obrpc::ObSetPasswdArg &arg,
                                        common::ObSqlString &sql_string);
  static int gen_rename_user_sql(const obrpc::ObAccountArg &old_account,
                                 const obrpc::ObAccountArg &new_account,
                                 common::ObSqlString &sql_string);
  static int gen_lock_user_sql(const obrpc::ObAccountArg &account,
                               const bool locked,
                               common::ObSqlString &sql_string);
  static int gen_drop_user_sql(const obrpc::ObAccountArg &account,
                               common::ObSqlString &sql_string);
  static int gen_revoke_all_sql(const obrpc::ObAccountArg &account,
                                common::ObSqlString &sql_string);
  static int gen_user_priv_sql(const obrpc::ObAccountArg &account,
                               const share::schema::ObNeedPriv &need_priv,
                               const bool is_grant,
                               common::ObSqlString &sql_string);
  static int gen_db_priv_sql(const obrpc::ObAccountArg &account,
                             const share::schema::ObNeedPriv &need_priv,
                             const bool is_grant,
                             common::ObSqlString &sql_string);
  static int gen_table_priv_sql(const obrpc::ObAccountArg &account,
                                const share::schema::ObNeedPriv &need_priv,
                                const bool is_grant,
                                common::ObSqlString &sql_string);
  static int gen_table_priv_sql_ora(const obrpc::ObAccountArg &account,
                                    const share::schema::ObTablePrivSortKey &table_priv_key,
                                    const bool revoke_all_flag,
                                    const share::ObRawObjPrivArray &obj_priv_array,
                                    const bool is_grant,
                                    common::ObSqlString &sql_string);
  static int gen_routine_priv_sql(const obrpc::ObAccountArg &account,
                                  const share::schema::ObNeedPriv &need_priv,
                                  const bool is_grant,
                                  common::ObSqlString &sql_string);
  static int gen_audit_stmt_sql(const common::ObString &username,
                                const share::schema::ObSAuditModifyType modify_type,
                                const share::schema::ObSAuditSchema &audit_schema,
                                const share::schema::ObSAuditOperByType by_type,
                                const share::schema::ObSAuditOperWhenType when_type,
                                common::ObSqlString &sql_string);
  static int gen_audit_object_sql(const common::ObString &schema_name,
                                  const common::ObString &object_name,
                                  const share::schema::ObSAuditModifyType modify_type,
                                  const share::schema::ObSAuditSchema &audit_schema,
                                  const share::schema::ObSAuditOperByType by_type,
                                  const share::schema::ObSAuditOperWhenType when_type,
                                  common::ObSqlString &sql_string);
  static int gen_column_priv_sql(const obrpc::ObAccountArg &account,
                                 const share::schema::ObNeedPriv &need_priv,
                                 const bool is_grant,
                                 ObSqlString &sql_string);
  
private:
  static int get_priv_name(const int64_t priv, const char *&name);
  static int priv_to_name(const ObPrivSet priv, common::ObSqlString &priv_str);
  static char *adjust_ddl_format_str(char *ori_format_str);
  DISALLOW_COPY_AND_ASSIGN(ObDDLSqlGenerator);
};


}
}


#endif //OCEANBASE_ROOTSERVER_OB_DDL_SERVICE_H_
