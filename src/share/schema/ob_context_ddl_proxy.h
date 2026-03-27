/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef __OB_SHARE_SCHEMA_CONTEXT_DDL_PROXY_H__
#define __OB_SHARE_SCHEMA_CONTEXT_DDL_PROXY_H__

#include "lib/utility/ob_macro_utils.h"
#include "lib/container/ob_bit_set.h"

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
class ObMySQLTransaction;
}
namespace share
{
namespace schema
{
class ObSchemaGetterGuard;
class ObContextSchema;
class ObMultiVersionSchemaService;
}

class ObContextDDLProxy
{
public:
  ObContextDDLProxy(share::schema::ObMultiVersionSchemaService &schema_service);
  virtual ~ObContextDDLProxy();
  int create_context(share::schema::ObContextSchema &ctx_schema,
                      common::ObMySQLTransaction &trans,
                      share::schema::ObSchemaGetterGuard &schema_guard,
                      const bool or_replace,
                      const bool obj_exist,
                      const share::schema::ObContextSchema *old_schema,
                      bool &need_clean,
                      const common::ObString *ddl_stmt_str);
  int inner_create_context(share::schema::ObContextSchema &ctx_schema,
                            common::ObMySQLTransaction &trans,
                            share::schema::ObSchemaGetterGuard &schema_guard,
                            const common::ObString *ddl_stmt_str);
  int drop_context(share::schema::ObContextSchema &ctx_schema,
                    common::ObMySQLTransaction &trans,
                    share::schema::ObSchemaGetterGuard &schema_guard,
                    const share::schema::ObContextSchema *old_schema,
                    bool &need_clean,
                    const common::ObString *ddl_stmt_str);
  int create_or_replace_context(schema::ObContextSchema &ctx_schema,
                                common::ObMySQLTransaction &trans,
                                share::schema::ObSchemaGetterGuard &schema_guard,
                                const bool obj_exist,
                                const schema::ObContextSchema *old_schema,
                                bool &need_clean,
                                const ObString *ddl_stmt_str);
  int inner_alter_context(share::schema::ObContextSchema &ctx_schema,
                            common::ObMySQLTransaction &trans,
                            share::schema::ObSchemaGetterGuard &schema_guard,
                            const ObString *ddl_stmt_str);
private:
  /* functions */
  /* variables */
  DISALLOW_COPY_AND_ASSIGN(ObContextDDLProxy);
  share::schema::ObMultiVersionSchemaService &schema_service_;
};
}
}
#endif /* __OB_SHARE_SCHEMA_CONTEXT_DDL_PROXY_H__ */
//// end of header file