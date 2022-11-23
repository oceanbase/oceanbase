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

#ifndef MOCK_MV_SCHEMA_SERVICE_H_
#define MOCK_MV_SCHEMA_SERVICE_H_

#include <gmock/gmock.h>
#include "share/schema/ob_multi_version_schema_service.h"
#include "sql/resolver/ob_stmt.h"

namespace oceanbase
{
namespace storage
{

class MockMultiVersionSchemaService : public share::schema::ObMultiVersionSchemaService
{
public:
  MockMultiVersionSchemaService()
  {
    schema_manager_ = new share::schema::ObSchemaManager();
  }
  virtual ~MockMultiVersionSchemaService()
  {
    delete schema_manager_;
  }
  int parse_from_file(const char *path);
  virtual const share::schema::ObSchemaManager *get_user_schema_manager(const int64_t version);
private:
  void do_create_table(common::ObArenaAllocator &allocator,
                       share::schema::ObSchemaManager *schema_mgr,
                       const char *query_str,
                       uint64_t table_id);
  void do_resolve(common::ObArenaAllocator &allocator,
                  share::schema::ObSchemaManager *schema_mgr,
                  const char *query_str,
                  sql::ObStmt *&stmt);
  share::schema::ObSchemaManager *schema_manager_;
};
}// namespace storage
}// namespace oceanbase

#endif /* MOCK_OB_PARTITION_REPORT_H_ */
