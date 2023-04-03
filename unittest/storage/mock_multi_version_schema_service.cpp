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

#include "sql/resolver/ob_schema_checker.h"
#include "sql/resolver/ob_stmt.h"
#include "sql/resolver/ob_stmt_resolver.h"
#include "sql/resolver/ob_resolver.h"
#include "sql/resolver/ddl/ob_create_table_stmt.h"
#include "sql/parser/ob_parser.h"
#include "lib/oblog/ob_log_module.h"
#include <gmock/gmock.h>
#include <fstream>
#include "./mock_multi_version_schema_service.h"

using namespace oceanbase;
using namespace share;
using namespace sql;

#define OK(value) ASSERT_EQ(OB_SUCCESS, (value))


namespace oceanbase
{
namespace storage
{

void MockMultiVersionSchemaService::do_resolve(ObArenaAllocator &allocator,
                                               schema::ObSchemaManager *schema_mgr,
                                               const char *query_str, ObStmt *&stmt)
{
  ObParser parser(allocator);
  ObString query = ObString::make_string(query_str);
  ParseResult parse_result;
  OK(parser.parse(query, parse_result));
  //    _LOG_DEBUG("%s", SJ(ObParserResultPrintWrapper(*parse_result.result_tree_)));
  parser.free_result(parse_result);
  ObSchemaChecker schema_checker;
  schema_checker.init(*schema_mgr);
  ObResolverParams resolver_ctx;
  resolver_ctx.allocator_  = &allocator;
  resolver_ctx.schema_checker_ = &schema_checker;
  OK(resolver_ctx.create_query_ctx());
  ObResolver resolver(resolver_ctx);
  OK(resolver.resolve(ObResolver::IS_NOT_PREPARED_STMT, *parse_result.result_tree_, stmt));
  //_LOG_DEBUG("%s", SJ(*stmt));   // segv
}

void MockMultiVersionSchemaService::do_create_table(common::ObArenaAllocator &allocator,
                                                    share::schema::ObSchemaManager *schema_mgr,
                                                    const char *query_str,
                                                    uint64_t table_id)
{
  ObStmt *stmt = NULL;
  do_resolve(allocator, schema_mgr, query_str, stmt);
  // add the created table schema
  ObCreateTableStmt *create_table_stmt = dynamic_cast<ObCreateTableStmt *>(stmt);
  OB_ASSERT(NULL != create_table_stmt);
  /*
  schema::ObTableSchema table_schema;
  table_schema.set_tenant_id(0);
  table_schema.set_database_id(0);
  table_schema.set_tablegroup_id(0);
  table_schema.set_table_name(create_table_stmt->get_table_name());
  table_schema.set_table_id(table_id);
  int64_t N = create_table_stmt->get_column_count();
  for (int64_t i = 0; i < N; ++i) {
    create_table_stmt->get_column_schema(i).set_column_id(i + 1);
    OK(table_schema.add_column(create_table_stmt->get_column_schema(i)));
  }
  table_schema.set_max_used_column_id(N);
  */
  create_table_stmt->get_create_table_arg().schema_.set_block_size(16384);
  create_table_stmt->get_create_table_arg().schema_.set_table_id(table_id);
  OK(schema_mgr->add_new_table_schema(create_table_stmt->get_create_table_arg().schema_));
}

int MockMultiVersionSchemaService::parse_from_file(const char *path)
{
  int ret = OB_SUCCESS;
  /**
   *  1. get schema manager
   */
  ObArenaAllocator allocator(ObModIds::TEST);

  if (OB_SUCCESS != (ret = schema_manager_->init())) {
    return ret;
  }

  // create schema
  std::ifstream if_schema(path);
  //ASSERT_TRUE(if_schema.is_open());
  std::string line;
  uint64_t tid = 1;

  while (std::getline(if_schema, line)) {
    do_create_table(allocator, schema_manager_, line.c_str(), tid++);
  }
  return ret;
}

const share::schema::ObSchemaManager *MockMultiVersionSchemaService::get_user_schema_manager(
    const int64_t version)
{
  UNUSED(version);
  return schema_manager_;
}

}
}

