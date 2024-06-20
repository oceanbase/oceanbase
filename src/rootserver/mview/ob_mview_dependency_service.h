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

#ifndef OCEANBASE_ROOTSERVER_MVIEW_OB_MVIEW_DEPENDENCY_SERVICE_H_
#define OCEANBASE_ROOTSERVER_MVIEW_OB_MVIEW_DEPENDENCY_SERVICE_H_

#include "lib/ob_define.h"
#include "lib/container/ob_array.h"
#include "share/schema/ob_table_schema.h"

namespace oceanbase
{
namespace common
{
  class ObMySQLTransaction;
}
namespace share
{
namespace schema
{
  class ObSchemaGetterGuard;
  class ObMultiVersionSchemaService;
  class ObDependencyInfo;
}
}
namespace rootserver
{
class ObMViewDependencyService
{
public:
  ObMViewDependencyService(share::schema::ObMultiVersionSchemaService &schema_service);
  ~ObMViewDependencyService();
  int update_mview_dep_infos(common::ObMySQLTransaction &trans,
                             share::schema::ObSchemaGetterGuard &schema_guard,
                             const uint64_t tenant_id,
                             const uint64_t mview_table_id,
                             const common::ObIArray<share::schema::ObDependencyInfo> &dep_infos);
  int remove_mview_dep_infos(common::ObMySQLTransaction &trans,
                             share::schema::ObSchemaGetterGuard &schema_guard,
                             const uint64_t tenant_id,
                             const uint64_t mview_table_id);
  int update_mview_reference_table_status(
      common::ObMySQLTransaction &trans,
      share::schema::ObSchemaGetterGuard &schema_guard,
      const uint64_t tenant_id,
      const ObIArray<uint64_t> &ref_table_ids,
      enum share::schema::ObTableReferencedByMVFlag table_flags);
private:
  share::schema::ObMultiVersionSchemaService &schema_service_;
};
} // end of sql
} // end of oceanbase
#endif