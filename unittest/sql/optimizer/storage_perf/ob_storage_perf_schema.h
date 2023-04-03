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

#ifndef TOOLS_STORAGE_PERF_TEST_SCHEMA_H_
#define TOOLS_STORAGE_PERF_TEST_SCHEMA_H_
#include <fstream>
#include <iterator>

#include "sql/parser/ob_parser.h"
#include "sql/resolver/ob_resolver.h"
#include "lib/allocator/page_arena.h"
#include "lib/json/ob_json_print_utils.h"
#include "sql/resolver/ddl/ob_create_table_stmt.h"
#include "sql/resolver/ddl/ob_create_index_stmt.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/system_variable/ob_system_variable.h"
#include "sql/parser/parse_node.h"
#include "storage/ob_partition_storage.h"
#include "storage/mockcontainer/ob_restore_schema.h"

namespace oceanbase
{
using namespace sql;
using namespace share::schema;

namespace storageperf
{

class MySchemaService : public oceanbase::share::schema::ObMultiVersionSchemaService
{
public:
  int init(const char *file_name);
  int add_schema(const char *file_name);
  void get_schema_guard(ObSchemaGetterGuard *&schema_guard) {schema_guard = schema_guard_;}
private:
  ObRestoreSchema restore_schema_;
  ObSchemaGetterGuard *schema_guard_;
};

}//end storageperf
}//end oceanbase

#endif //TOOLS_STORAGE_PERF_SCHEMA_H_
