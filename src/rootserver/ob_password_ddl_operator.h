/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_ROOTSERVER_OB_PASSWORD_DDL_OPERATOR_H_
#define OCEANBASE_ROOTSERVER_OB_PASSWORD_DDL_OPERATOR_H_

#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_service.h"

namespace oceanbase
{
namespace rootserver
{

// Applies password-related changes to user schema records.
class ObPasswordDDLOperator {
public:
  explicit ObPasswordDDLOperator(share::schema::ObMultiVersionSchemaService &schema_service);
  ~ObPasswordDDLOperator();
  int set_passwd(const uint64_t tenant_id,
                 const uint64_t user_id,
                 const common::ObString &passwd,
                 const common::ObString *ddl_stmt_str,
                 common::ObMySQLTransaction &trans,
                 const common::ObString &plugin,
                 const bool retain_current_password,
                 const bool discard_old_password,
                 const share::schema::ObPasswordExpireOption password_expire_option,
                 int64_t password_lifetime_us,
                 const int64_t password_last_changed);

private:
  int calc_old_password_info(const bool discard_old_password,
                             const bool retain_current_password,
                             const common::ObString &plugin,
                             const share::schema::ObUserInfo *user_info,
                             common::ObString &old_password,
                             int64_t &old_password_start_time);

  share::schema::ObMultiVersionSchemaService &schema_service_;
  DISALLOW_COPY_AND_ASSIGN(ObPasswordDDLOperator);
};

} // end namespace rootserver
} // end namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_PASSWORD_DDL_OPERATOR_H_
