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

#ifndef OB_RESTORE_SCHEMA_
#define OB_RESTORE_SCHEMA_
#include "share/schema/ob_schema_getter_guard.h"
#include "sql/parser/parse_node.h"
#include "../../share/schema/mock_schema_service.h"
namespace oceanbase
{
using namespace common;
#if 0
namespace share
{
namespace schema
{
class MockSchemaService;
}
}
#endif
using namespace share;
using namespace share::schema;
namespace sql
{
class ObStmt;
class ObCreateIndexStmt;
struct ObResolverParams;
class ObRestoreSchema
{
public:
  ObRestoreSchema();
  virtual ~ObRestoreSchema() = default;
  int parse_from_file(const char *filename,
                      share::schema::ObSchemaGetterGuard *&schema_guard);
  share::schema::ObSchemaGetterGuard &get_schema_guard() { return schema_guard_; }
  //share::schema::ObSchemaManager *get_schema_manager();
  int init();

public:
  static const int64_t RESTORE_SCHEMA_VERSION = 1;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObRestoreSchema);
protected:
  // function members
  int do_parse_line(common::ObArenaAllocator &allocator, const char *query);
  int do_create_table(ObStmt *stmt);
  int do_create_index(ObStmt *stmt);
  int gen_columns(ObCreateIndexStmt &stmt,
                  share::schema::ObTableSchema &index_schema);
  int do_resolve_single_stmt(ParseNode *node,
                             ObResolverParams &ctx);
  int add_database_schema(ObDatabaseSchema &database_schema);
  int add_table_schema(ObTableSchema &table_schema);
public:
  // data members
  //share::schema::ObSchemaManager schema_manager_;
  share::schema::MockSchemaService *schema_service_;
  share::schema::ObSchemaGetterGuard schema_guard_;
  //int64_t schema_version_;
  uint64_t table_id_;
  uint64_t tenant_id_;
  uint64_t database_id_;
};
}
}
#endif
