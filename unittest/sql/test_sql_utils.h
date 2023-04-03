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

#ifndef TEST_SQL_UTILS_H_
#define TEST_SQL_UTILS_H_

#undef private
#undef protected
#include <fstream>
#include <dirent.h>
#include <getopt.h>
#include <gtest/gtest.h>
#define private public
#define protected public
#include "lib/allocator/page_arena.h"
#include "lib/allocator/ob_fifo_allocator.h"
#include "lib/utility/ob_test_util.h"
#include "lib/json/ob_json_print_utils.h"  // for SJ
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "sql/parser/ob_parser.h"
#include "sql/resolver/ob_resolver.h"
#include "sql/resolver/ob_schema_checker.h"
#include "sql/resolver/ddl/ob_create_table_stmt.h"
#include "sql/resolver/ddl/ob_drop_table_stmt.h"
#include "sql/resolver/ddl/ob_create_index_stmt.h"
#include "sql/resolver/ddl/ob_create_database_stmt.h"
#include "sql/resolver/ddl/ob_use_database_stmt.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/resolver/dcl/ob_create_user_stmt.h"
#include "sql/optimizer/ob_log_plan_factory.h"
#include "sql/executor/ob_task_executor_ctx.h"
#include "sql/ob_sql_context.h"
#include "sql/engine/ob_exec_context.h"
#include "../share/schema/mock_schema_service.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::obrpc;
using namespace oceanbase::sql;

#define CSJ(x) (static_cast<const char *>(SJ((x))))
namespace test
{
typedef common::ParamStore ParamStore;
static const int32_t FILE_PATH_LEN = 512;
static const int32_t MAX_FILE_NUM = 128;
struct CmdLineParam
{
  CmdLineParam()
  {
    file_count = 0;
    test_input_from_cmd = false;
    print_schema_detail_info = false;
    record_test_result = false;
    sql_mode = DEFAULT_MYSQL_MODE;
  }
  char file_names[MAX_FILE_NUM][FILE_PATH_LEN];
  int32_t file_count;
  bool test_input_from_cmd;
  bool print_schema_detail_info;
  bool record_test_result;
  ObSQLMode sql_mode;
  std::vector<const char*> file_names_vector;
};
extern CmdLineParam clp;


extern bool comparisonFunc(const char *c1, const char *c2);
extern void load_sql_file(const char* file_name);
extern void load_all_sql_files(const char* directory_name);
extern void print_help_msg (const char* exe_name);
extern void parse_cmd_line_param(int argc, char *argv[], CmdLineParam &clp);

inline bool casesame_cstr(const char *a, const char *b)
{
  size_t len1 = strlen(a);
  size_t len2 = strlen(b);
  return (len1 == len2) && (strncasecmp(a, b, len1) == 0);
}

enum ParserResultFormat
{
  TREE_FORMAT,
  JSON_FORMAT
};

class TestSqlUtils
{
public:
  TestSqlUtils();
  virtual ~TestSqlUtils(){}
  virtual void init();
  virtual void destroy();
public:
  static const int64_t MAX_SCHEMA_FILE_PATH = 128 - 1;
// function members
  void load_schema_from_file(const char *file_path);
  void do_load_sql(const char *query_str, ObStmt *&stmt, bool is_print,  ParserResultFormat format = TREE_FORMAT, int64_t expect_error = OB_SUCCESS, int64_t case_line = 0);
  void do_resolve(const char* query, ObStmt *&stmt, bool is_print = false,
                  ParserResultFormat format = JSON_FORMAT, int64_t expect_error = OB_SUCCESS,
                  bool parameterized = true, bool need_replace_param_expr = true, int64_t case_line = 0);
  int create_system_table();
  void create_system_db();
  void do_create_table(const char *query_str);
  void do_create_table(ObStmt *&stmt);
  void do_drop_table(ObStmt *&stmt);
  void do_create_index(ObStmt *&stmt);
  void do_create_database(ObStmt *&stmt);
  void do_use_database(ObStmt *&stmt);
  void do_create_user(ObStmt *&stmt);
  void generate_index_schema(ObCreateIndexStmt &stmt);
  void generate_index_column_schema(ObCreateIndexStmt &stmt, ObTableSchema &index_schema);
  int get_hidden_column_value(ObResolverParams &resolver_ctx, ParamStore &params);
  void is_equal_content(const char* tmp_file, const char* result_file);
  uint64_t get_next_table_id(const uint64_t user_tenant_id);
  int add_table_schema(ObTableSchema &table_schema);
  int add_database_schema(ObDatabaseSchema &database_schema);
  int drop_table_schema(const ObTableSchema &table_schema);
  int parse_row_from_json(const ObString &json_str, ObString &table_name, ObIArray<ObSEArray<ObObj, 3> > &row_array);
  int parse_json_array(json::Value &value, ObIArray<ObSEArray<ObObj, 3> > &row_array);
  ObSqlSchemaGuard &get_sql_schema_guard() { return sql_schema_guard_; }
  ObSchemaGetterGuard &get_schema_guard() { return schema_guard_; }
public:
  //table id
  hash::ObHashMap<uint64_t,uint64_t> next_user_table_id_map_;
  //user_id
  uint64_t sys_user_id_;
  uint64_t next_user_id_;
  //database_id
  uint64_t sys_database_id_;
  uint64_t next_user_database_id_;
  //tenant_id
  uint64_t sys_tenant_id_;
  ObSEArray<ObObj, 16> sys_view_bigint_param_list_;
  int64_t schema_version_;
  char schema_file_path_[MAX_SCHEMA_FILE_PATH + 1];
  ObSQLSessionInfo session_info_;
  ObSqlSchemaGuard sql_schema_guard_;
  share::schema::ObSchemaGetterGuard schema_guard_;
  MockSchemaService *schema_service_;
  ObArenaAllocator allocator_;
  ObRawExprFactory expr_factory_;
  ObStmtFactory stmt_factory_;
  ObLogPlanFactory log_plan_factory_;
  ObSqlCtx sql_ctx_;
  ObExecContext exec_ctx_;
  ParamStore param_list_;
private:
  DISALLOW_COPY_AND_ASSIGN(TestSqlUtils);
};
}
#endif
