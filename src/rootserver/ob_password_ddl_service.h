/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_ROOTSERVER_OB_PASSWORD_DDL_SERVICE_H_
#define OCEANBASE_ROOTSERVER_OB_PASSWORD_DDL_SERVICE_H_

#include "rootserver/ob_ddl_service.h"

namespace oceanbase
{
namespace rootserver
{

// Orchestrates password-related DDL and delegates shared operations to ObDDLService.
class ObPasswordDDLService {
public:
  explicit ObPasswordDDLService(ObDDLService *ddl_service);
  ~ObPasswordDDLService();
  int set_passwd(const obrpc::ObSetPasswdArg &arg);
  int set_passwd_in_trans(const uint64_t tenant_id,
                          const uint64_t user_id,
                          const common::ObString &new_passwd,
                          const common::ObString *ddl_stmt_str,
                          share::schema::ObSchemaGetterGuard &schema_guard,
                          const common::ObString &plugin,
                          const bool retain_current_password,
                          const bool discard_old_password,
                          const share::schema::ObPasswordExpireOption password_expire_option,
                          const int64_t password_lifetime_us,
                          const int64_t password_last_changed);

private:
  ObDDLService *ddl_service_;  // Not owned.
  DISALLOW_COPY_AND_ASSIGN(ObPasswordDDLService);
};

} // end namespace rootserver
} // end namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_PASSWORD_DDL_SERVICE_H_
