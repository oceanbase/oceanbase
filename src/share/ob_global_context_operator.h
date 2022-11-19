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

#ifndef __OB_SHARE_SCHEMA_GLOBAL_CONTEXT_OPERATOR_H__
#define __OB_SHARE_SCHEMA_GLOBAL_CONTEXT_OPERATOR_H__

#include "lib/utility/ob_macro_utils.h"
#include "lib/container/ob_bit_set.h"

namespace oceanbase
{
namespace common
{
class ObISQLClient;
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

class ObGlobalContextOperator
{
public:
  ObGlobalContextOperator();
  virtual ~ObGlobalContextOperator();
  int clean_global_context(const common::ObIArray<uint64_t> &tenant_ids,
                           common::ObISQLClient &sql_proxy,
                           share::schema::ObMultiVersionSchemaService &schema_service);
  int delete_global_context(const uint64_t tenant_id,
                            const uint64_t context_id,
                            const ObString &attribute,
                            const ObString &client_id,
                            ObISQLClient &sql_proxy);
  int delete_global_contexts_by_id(const uint64_t tenant_id,
                                  const uint64_t context_id,
                                  ObISQLClient &sql_proxy);
  int delete_global_contexts_by_ids(const uint64_t tenant_id,
                                    const common::ObIArray<uint64_t> &context_ids,
                                    ObISQLClient &sql_proxy);
  int insert_update_context(const uint64_t tenant_id,
                            const uint64_t context_id,
                            const ObString &context_name,
                            const ObString &attribute,
                            const ObString &client_id,
                            const ObString &username,
                            const ObString &value,
                            ObISQLClient &sql_proxy);
  int read_global_context(const uint64_t tenant_id,
                          const uint64_t context_id,
                          const ObString &attribute,
                          const ObString &client_id,
                          const ObString &username, 
                          ObString &value,
                          bool &exist,
                          ObISQLClient &sql_proxy,
                          common::ObIAllocator &alloc);
  
private:
  /* functions */
  /* variables */
  DISALLOW_COPY_AND_ASSIGN(ObGlobalContextOperator);
};
}
}
#endif /* __OB_SHARE_SCHEMA_GLOBAL_CONTEXT_OPERATOR_H__ */
//// end of header file