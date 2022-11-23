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

#ifndef MOCK_OB_MULTI_VERSION_SCHEMA_SERVICE_H_
#define MOCK_OB_MULTI_VERSION_SCHEMA_SERVICE_H_

#include <gmock/gmock.h>
#define private public
#include "share/schema/ob_multi_version_schema_service.h"
#include "mock_schema_fetcher.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/cache/ob_kv_storecache.h"
#include "share/ob_define.h"
namespace oceanbase
{
namespace share
{
namespace schema
{
class MockObMultiVersionSchemaService : public ObMultiVersionSchemaService
{
public:
  MockObMultiVersionSchemaService() {}
  virtual ~MockObMultiVersionSchemaService() {}
  MOCK_METHOD2(check_table_exist,
      int(const uint64_t table_id, bool &exist));
  //MOCK_METHOD(check_inner_stat, bool());
  int init();
  int get_schema_guard(ObSchemaGetterGuard &guard, int64_t snapshot_version = common::OB_INVALID_VERSION);
  int add_table_schema(ObTableSchema &table_schema, int64_t schema_version);
  int add_database_schema(ObDatabaseSchema &database_schema, int64_t schema_version);
protected:
  MockObSchemaFetcher mock_schema_fetcher_;
};
}
}
}
#include "mock_ob_multi_version_schema_service.ipp"



#endif /* MOCK_OB_MULTI_VERSION_SCHEMA_SERVICE_H_ */
