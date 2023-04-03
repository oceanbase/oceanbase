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

#ifndef OCEANBASE_SHARE_OB_MOCK_SCHEMA_FETCHER_H_
#define OCEANBASE_SHARE_OB_MOCK_SCHEMA_FETCHER_H_

#include "share/schema/ob_schema_cache.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class MockObSchemaFetcher : public ObSchemaFetcher
{
public:
  MOCK_METHOD2(init, int(ObSchemaService *, common::ObISQLClient *));
  MOCK_METHOD5(fetch_schema, int(ObSchemaType, uint64_t, int64_t,
      common::ObIAllocator &, ObSchema *&));
  MOCK_METHOD4(fetch_tenant_schema, int(uint64_t, int64_t,
      common::ObIAllocator &, ObTenantSchema *&));
  MOCK_METHOD4(fetch_user_info, int(uint64_t, int64_t,
      common::ObIAllocator &, ObUserInfo *&));
  MOCK_METHOD4(fetch_database_schema, int(uint64_t, int64_t,
      common::ObIAllocator &, ObDatabaseSchema *&));
  MOCK_METHOD4(fetch_tablegroup_schema, int(uint64_t, int64_t,
      common::ObIAllocator &, ObTablegroupSchema *&));
  MOCK_METHOD4(fetch_table_schema, int(uint64_t, int64_t,
       common::ObIAllocator &, ObTableSchema *&));
  virtual ~MockObSchemaFetcher() {}
};
}//end namespace schema
}//end namespace share
}//end namespace oceanbase

#endif
