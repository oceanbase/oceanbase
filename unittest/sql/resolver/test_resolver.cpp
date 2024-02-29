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

#include <iterator>
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/ob_sql_init.h"
#include "sql/printer/ob_select_stmt_printer.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/dml/ob_insert_stmt.h"
#include "sql/resolver/dml/ob_update_stmt.h"
#include "sql/resolver/dml/ob_delete_stmt.h"
#include "sql/resolver/tcl/ob_start_trans_stmt.h"
#include "sql/resolver/tcl/ob_end_trans_stmt.h"
#include "sql/resolver/dcl/ob_create_user_stmt.h"
#include "sql/resolver/dcl/ob_grant_stmt.h"
#include "sql/resolver/dcl/ob_revoke_stmt.h"
#include "sql/resolver/dcl/ob_drop_user_stmt.h"
#include "sql/resolver/dcl/ob_rename_user_stmt.h"
#include "sql/resolver/dcl/ob_set_password_stmt.h"
#include "sql/ob_sql_define.h"
#include "lib/string/ob_sql_string.h"
#include "../test_sql_utils.h"


#include "sql/optimizer/ob_join_order.h"

using namespace oceanbase::obrpc;
namespace test
{
const char* SQL_DIR = "sql";
const char* RESULT_DIR ="result";
class TestResolver: public TestSqlUtils, public ::testing::Test
{
public:
  TestResolver();
  virtual ~TestResolver(){}
  virtual void SetUp();
  virtual void TearDown();
protected:
  void do_equal_test();
  void do_stmt_copy_test();
  void do_padding_test();
  void do_expand_view_test();
  void do_stmt_tostring_test();
  void do_join_order_test();
  void input_test_from_cmd();
  uint64_t get_next_table_id(const uint64_t user_tenant_id);
  bool is_show_sql(const ParseNode &node) const;
  static void get_index_name(std::string &str, const ObTableSchema &idx_schema);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestResolver);
};

TestResolver::TestResolver()
{
  memcpy(schema_file_path_, "./test_resolver.schema", sizeof("./test_resolver.schema"));
}
void TestResolver::SetUp()
{
  init();
}

void TestResolver::TearDown()
{
  destroy();
}

bool TestResolver::is_show_sql(const ParseNode &node) const
{
  bool ret = false;
  switch( node.type_){
    case T_SHOW_TABLES:
    case T_SHOW_DATABASES:
    case T_SHOW_VARIABLES:
    case T_SHOW_COLUMNS:
    case T_SHOW_SCHEMA:
    case T_SHOW_CREATE_TABLE:
    case T_SHOW_CREATE_VIEW:
    case T_SHOW_TABLE_STATUS:
    case T_SHOW_PARAMETERS:
    //case T_SHOW_INDEXES:
    case T_SHOW_PROCESSLIST:
    case T_SHOW_SERVER_STATUS:
    case T_SHOW_WARNINGS:
    case T_SHOW_RESTORE_PREVIEW:
    case T_SHOW_SEQUENCES:
    case T_SHOW_GRANTS:{
      ret = true;
      break;
    }
    default: {
      ret = false;
    }
  }
  return ret;
}

TEST_F(TestResolver, basic_test)
{
  //for rongxuan.test input sql in command line
  if (clp.test_input_from_cmd){
      input_test_from_cmd();
      exit(0);
  }

  const char *postfix[] = {"test","tmp","result"};
  int64_t sql_postfix_len = strlen(postfix[0]);
  int64_t tmp_postfix_len = strlen(postfix[1]);
  int64_t result_postfix_len = strlen(postfix[2]);
  char file_name[3][FILE_PATH_LEN];
  //construct the file name ./sql/test_resolver_xxx.test  sql file
  //construct the file name ./result/test_resolver_xxx.tmp  tmp result file
  //construct the file name ./result/test_resolver_xxx.test  correct result file (now is empty)
  //  int ret = 0;
  for(int32_t i = 0; i < clp.file_count; ++i){
    if (i > 0) {
      destroy();
      init();
    }
    int64_t sql_file_len = strlen(clp.file_names_vector[i]);
    snprintf(file_name[0],
             strlen(SQL_DIR) + sql_file_len + sql_postfix_len + 4,
             "./%s/%s%s",
             SQL_DIR,
             //clp.file_names[i],
             clp.file_names_vector[i],
             postfix[0]);
    snprintf(file_name[1],
             strlen(RESULT_DIR) + sql_file_len + tmp_postfix_len + 4,
             "./%s/%s%s",
             RESULT_DIR,
             //clp.file_names[i],
             clp.file_names_vector[i],
             postfix[1]);
    snprintf(file_name[2],
             strlen(RESULT_DIR) + sql_file_len + result_postfix_len + 4,
             "./%s/%s%s",
             RESULT_DIR,
            // clp.file_names[i],
             clp.file_names_vector[i],
             postfix[2]);
    _OB_LOG(INFO, "%s\t%s\t%s\t%s",clp.file_names_vector[i],file_name[0], file_name[1], file_name[2]);

    std::ifstream if_sql(file_name[0]);
    if (!if_sql.is_open()){
      _OB_LOG(ERROR,"file %s not exist!", file_name[0]);
      continue;
    } else {
      fprintf(stdout, "CASE FILE: %s\n", file_name[0]);
    }
    ASSERT_TRUE(if_sql.is_open());
    ObStmt *stmt = NULL;
    std::ofstream of_tmp(file_name[1]);
    ASSERT_TRUE(of_tmp.is_open()) << file_name[1];
    if (!of_tmp.is_open()) {
      _OB_LOG(ERROR,"file %s not exist!", file_name[1]);
      continue;
    }
    std::string line;
    int64_t case_id = 0;
    bool is_print = false;
    int64_t expect_error = 0;
    char *w;
    char *p;
    int64_t case_line = 0;
    while (std::getline(if_sql, line)) {
      ++case_line;
      if (line.size() <= 0) continue;
      if (line.at(0) == '#') continue;
      if (strncmp(line.c_str(), "--error", strlen("--error")) == 0) {
        p = const_cast<char*>(line.c_str());
        w = strsep(&p, " ");
        expect_error = atol(p);
        continue;
      }
      if (line.at(0) == '\r' || line.at(0) == '\n' ) continue;
      stmt = NULL;
      of_tmp << "***************   Case "<< ++case_id << "   ***************" << std::endl;
      of_tmp << line << std::endl;
      _OB_LOG(INFO, "case %ld: query str %s", case_id, line.c_str());
      //fprintf(stdout, "case %ld: %s\n", case_id, line.c_str());
      ASSERT_NO_FATAL_FAILURE(do_load_sql(line.c_str(),stmt, is_print, TREE_FORMAT, expect_error, case_line));
      if (expect_error == 0) {
        //ASSERT_FALSE(HasFatalFailure());
        if (NULL != stmt) {
          of_tmp << CSJ(*stmt) << std::endl;
        }
      } else {
        expect_error = 0;
      }
      allocator_.reset();
      stmt_factory_.destory();
      expr_factory_.destory();
    }
    of_tmp.close();
    if_sql.close();
    _OB_LOG(INFO, "test %s finished!, total %ld case", clp.file_names_vector[i], case_id);
    fprintf(stdout, "FINISHED %s TOTAL %ld\n", clp.file_names_vector[i], case_id);
    // verify result
    // ObSqlString cmd;
    // cmd.assign_fmt("diff -u %s %s", file_name[1], file_name[2]);
    // ret = system(cmd.ptr());
    // EXPECT_EQ(0, ret) << cmd.ptr() << std::endl;
    //std::ifstream if_result(file_name[2]);
    //ASSERT_TRUE(if_result.is_open());
    //std::istream_iterator<std::string> it_result(if_result);
    //std::ifstream if_tmp(file_name[1]);
    //ASSERT_TRUE(if_tmp.is_open());
    //std::istream_iterator<std::string> it_tmp(if_tmp);
    //ASSERT_TRUE(std::equal(it_result, std::istream_iterator<std::string>(), it_tmp));
    //if_result.close();
    //if_tmp.close();
    ASSERT_NO_FATAL_FAILURE(is_equal_content(file_name[1], file_name[2]));
    //    ret = 0;
    UNUSED(w);
  }
}

TEST_F(TestResolver, join_order)
{
  do_join_order_test();
}

void TestResolver::do_equal_test()
{
  //just for equals test
//  const char* query_sqls[3] = {
//      "select c1,c2 from rongxuan.t1 where c1 > 0 group by c1 having count(c1) > 0 order by c2 desc limit 10",
//      "(select c5, c7 from rongxuan.test) union ( select t1.c1, t2.c2 from rongxuan.t1 join rongxuan.t2 on t1.c1 = t2.c1)",
//      "select t1.c1,t2.c2,test.c3 from rongxuan.t1 join rongxuan.t2 on t1.c1 =t2.c1 join rongxuan.test on t2.c1 = test.c1"
//
//  };
//
//  bool is_print = false;
//  uint32_t size = sizeof(query_sqls)/sizeof(const char*);
//  for(uint32_t i = 0; i < size; ++i) {
//    ObStmt *stmt1 = NULL;
//    ObStmt *stmt2 = NULL;
//    do_resolve(query_sqls[i], stmt1, is_print, JSON_FORMAT, OB_SUCCESS, false);
//    OB_ASSERT(stmt1 && (stmt::T_SELECT == stmt1->get_stmt_type()));
//    ObSelectStmt *select_stmt1 = static_cast<ObSelectStmt *>(stmt1);
//    do_resolve(query_sqls[i], stmt2, is_print, JSON_FORMAT, OB_SUCCESS, false);
//    OB_ASSERT(stmt2 && (stmt::T_SELECT == stmt2->get_stmt_type()));
//    ObSelectStmt *select_stmt2 = static_cast<ObSelectStmt *>(stmt2);
//    //bool result = select_stmt1->equals(*select_stmt2);
//    //_OB_LOG(INFO, "Equal Test for %s, ret = %d", query_sqls[i], result);
//    //OB_ASSERT(result);
//    stmt_factory_.destory();
//    expr_factory_.destory();
//  }
}

void TestResolver::do_padding_test()
{
  ObStmt *stmt = NULL;
  const char* query = "update char_t set c2 = 'aa', c3 = concat(c2, '4')";
  do_resolve(query, stmt, true, JSON_FORMAT, OB_SUCCESS, false);
  OB_LOG(INFO, "START TO BUILD EXPR FOR BINARY COLUMN");
  const char* query2 = "update char_t set c6 = 'aa', c7 = concat(c6, '4')";
  do_resolve(query2, stmt, true, JSON_FORMAT, OB_SUCCESS, false);
  OB_LOG(INFO, "START TO BUILD EXPR FOR char COLUMN");

  ObObj obj;
  obj.set_varchar("PAD_CHAR_TO_FULL_LENGTH");
  ASSERT_TRUE(OB_SUCCESS == session_info_.update_sys_variable_by_name(OB_SV_SQL_MODE, obj));
  do_resolve(query, stmt, true, JSON_FORMAT, OB_SUCCESS, false);
}

void TestResolver::do_join_order_test()
{
  const char* input_file = "test_skyline.sql";
  const char* result_file = "test_skyline.result";
  const char* tmp_file =  "test_skyline.tmp";
  std::ifstream if_sql(input_file);
  if (!if_sql.is_open()){
    _OB_LOG(ERROR,"file %s not exist!", input_file);
    exit(1);
  } else {
    fprintf(stdout, "CASE FILE: %s\n", input_file);
  }
  ASSERT_TRUE(if_sql.is_open());
  std::ofstream of_tmp(tmp_file);
  if (!of_tmp.is_open()) {
    _OB_LOG(ERROR, "file %s not exist!", tmp_file);
  } else {
    fprintf(stdout, "Temp RESULT FILE :%s\n", tmp_file);
  }
  ASSERT_TRUE(of_tmp.is_open());

  std::string line;
  int64_t case_id = 0;
  while (std::getline(if_sql, line)) {
    if (line.size() <= 0) continue;
    if (line.at(0) == '#') continue;
    if (line.at(0) == '\r' || line.at(0) == '\n' ) continue;
    of_tmp << "***************   Case "<< ++case_id << "   ***************\n" << std::endl;
    of_tmp << line << "\n\n";

    ObStmt *stmt = NULL;
    do_resolve(line.c_str(), stmt ,true, JSON_FORMAT, OB_SUCCESS, false);

    ObDMLStmt *dml_stmt = static_cast<ObDMLStmt*>(stmt);
    ASSERT_TRUE(NULL != dml_stmt);

    ObIArray<TableItem *> &table_items = dml_stmt->get_table_items();
    for (int64_t i = 0; i < table_items.count(); ++i) {
      const uint64_t table_id = table_items.at(i)->table_id_;
      const uint64_t ref_id = table_items.at(i)->ref_id_;
      const ObTableSchema *table_schema = NULL;
      OK(schema_guard_.get_table_schema(ref_id, table_schema));
      ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
      int ret = table_schema->get_simple_index_infos(simple_index_infos);
      ASSERT_EQ(ret, OB_SUCCESS);
      of_tmp << "table [" << table_schema->get_table_name() << "]" << std::endl;
      for (int i = 0; i <= simple_index_infos.count(); ++i) {
        const ObTableSchema *idx_schema = NULL;
        ObArray<ObRawExpr*> index_keys;
        ObArray<ObRawExpr*> ordering;
        int64_t match_prefix_count = 0;

        if (i == simple_index_infos.count()) {
          idx_schema = table_schema;
        } else {
          OK(schema_guard_.get_table_schema(simple_index_infos.at(i).table_id_, idx_schema));
        }
        ret = ObOptimizerUtil::generate_rowkey_exprs(dml_stmt,
                                                     expr_factory_,
                                                     table_id,
                                                     *idx_schema,
                                                     index_keys,
                                                     ordering);
        ASSERT_EQ(ret, OB_SUCCESS);
        std::string idx_name_with_column;
        get_index_name(idx_name_with_column, *idx_schema);
        ASSERT_EQ(ret, OB_SUCCESS);
        of_tmp <<"\t" << idx_name_with_column << "\n";

        ObSEArray<OrderItem, 4> index_ordering;
        for (int64_t i = 0; OB_SUCC(ret) && i < index_keys.count(); ++i) {
          OrderItem order_item(index_keys.at(i), default_asc_direction());
          if (OB_FAIL(index_ordering.push_back(order_item))) {
            LOG_WARN("failed to push back order item", K(ret));
          }
        }

        bool group_match = false;
        ObSelectStmt *select_stmt = static_cast<ObSelectStmt*>(dml_stmt);
        ret = ObOptimizerUtil::is_group_by_match(index_keys, select_stmt,
                                             match_prefix_count, group_match);
        ASSERT_EQ(ret, OB_SUCCESS);
        of_tmp << "\tis_group_math:" << group_match << "\t\t" << "match count:" << match_prefix_count << "\n";

        bool distinct_match = false;
        match_prefix_count = 0;
        ret = ObOptimizerUtil::is_distinct_match(index_keys, select_stmt,
                                             match_prefix_count, distinct_match);
        of_tmp << "\tis_distinct_match:" << distinct_match << "\t" << "match count:" <<match_prefix_count << "\n";

        bool orderby_match = false;
        match_prefix_count = 0;
        int64_t order_size = dml_stmt->get_order_item_size();
        SQL_OPT_LOG(WARN, "order size", K(order_size));
        ret = ObOptimizerUtil::is_order_by_match(index_ordering, select_stmt,
                                                 match_prefix_count, orderby_match);
        of_tmp << "\tis_orderby_match:" << orderby_match << "\t" << "match_count:" <<match_prefix_count <<"\n";

        bool set_match = false;
        match_prefix_count = 0;
        if (select_stmt->is_parent_set_distinct()) {
          ret = ObOptimizerUtil::is_set_match(index_keys, select_stmt,
                                          match_prefix_count, set_match);
        }
        of_tmp << "\tis_set_match:" << set_match << "\t" << "match_count:" << match_prefix_count << "\n";

        of_tmp << std::endl;
        SQL_OPT_LOG(WARN, "sort match prefix", K(group_match), K(match_prefix_count),
                    K(table_id), K(simple_index_infos.at(i).table_id_), K(idx_schema->get_table_name_str()));

      }
      of_tmp << "----------------------------------" << std::endl;
    }
    of_tmp << "\n***************   Case "<< case_id << "(end)***************\n" << std::endl;
  }

  std::string cmd;
  cmd.append("diff -u ").append(tmp_file).append(" ").append(result_file);
  ASSERT_EQ(OB_SUCCESS, system(cmd.c_str()));
  std::cout << cmd << std::endl;

  of_tmp.close();
  if_sql.close();
}

void TestResolver::get_index_name(std::string &str, const ObTableSchema &idx_schema)
{
  const ObRowkeyInfo &info = idx_schema.get_rowkey_info();
  ObString idx_name;
  if (idx_schema.is_index_table()) {
    idx_schema.get_index_name(idx_name);
  } else {
    idx_name = idx_schema.get_table_name_str();
  }
  str.append(idx_name.ptr()).append(" <");
  for (int64_t i = 0; i < info.get_size(); ++i) {
    uint64_t column_id = info.get_column(i)->column_id_;
    str.append(idx_schema.get_column_schema(column_id)->get_column_name());
    if (i != info.get_size() -1) {
      str.append(", ");
    }
  }
  str.append(">");
}

void TestResolver::do_stmt_copy_test()
{
  ObStmt *stmt = NULL;
  ObSelectStmt *select_stmt = NULL;
  const char* query = NULL;

  query = "select /*+ index(t1 idx1) */ t1.c1,t1.c2 from rongxuan.t1, rongxuan.t2 partition(p0)"
          " where t1.c1 > 0 and t1.c1 = t1.c2 and t1.c2 = t2.c3 "
          " group by t1.c1 having count(t1.c1) > 0 order by t1.c2 desc limit 10";
  _OB_LOG(INFO, "Copy Test for %s", query);
  do_resolve(query, stmt, false, JSON_FORMAT, OB_SUCCESS, false);
  OB_ASSERT(stmt && (stmt::T_SELECT == stmt->get_stmt_type()));
  select_stmt = static_cast<ObSelectStmt *>(stmt);

  ObSelectStmt dest_stmt;

  //test SemiInfo
  SemiInfo semi1;
  semi1.join_type_ = RIGHT_OUTER_JOIN;
  semi1.left_tables_.add_member(1);
  semi1.left_tables_.add_member(2);
  ASSERT_EQ(OB_SUCCESS, select_stmt->add_semi_info(&semi1));

  // test ObStmt deep_copy
  ASSERT_EQ(OB_SUCCESS, dest_stmt.assign(*select_stmt));
  //ASSERT_TRUE(select_stmt->equals(dest_stmt));

  // test part expr
  //const ObTableSchema *table_schema = schema_mgr_->get_table_schema(sys_tenant_id_, "rongxuan", "t2", false);
  const ObTableSchema *table_schema = NULL;
  OK(schema_guard_.get_table_schema(sys_tenant_id_, "rongxuan", "t2", false,table_schema));
  ASSERT_TRUE(table_schema);
  ASSERT_TRUE(NULL != dest_stmt.get_part_expr(table_schema->get_table_id(), table_schema->get_table_id()));

  //test ObStmtHint
  ASSERT_EQ(OB_SUCCESS, dest_stmt.check_and_convert_hint(session_info_));
  const ObStmtHint &hint = dest_stmt.get_stmt_hint();
  ASSERT_TRUE(NULL != hint.get_index_hint(select_stmt->get_table_item(0)->table_id_));
  // test partition table
  TableItem *part_table = select_stmt->get_table_item_by_id(table_schema->get_table_id());
  ASSERT_TRUE(NULL != part_table);
  ASSERT_TRUE(!part_table->part_ids_.empty());
  // test joined table
  query = "select t1.c1,t1.c2, test.c3 from (t1 join t2 on t1.c1 =t2.c1) join (t3 join test on t3.c3 = test.c1) on t1.c2 = test.c2";
  do_resolve(query, stmt, false, JSON_FORMAT, OB_SUCCESS, false);
  ASSERT_TRUE(stmt && (stmt::T_SELECT == stmt->get_stmt_type()));
  select_stmt = static_cast<ObSelectStmt *>(stmt);
  JoinedTable *joined_table = select_stmt->get_joined_table(OB_INVALID_ID - 1);
  ASSERT_TRUE(NULL != joined_table);

  const ObSelectStmt *const_select = select_stmt;
  const JoinedTable *const_joined_table = const_select->get_joined_table(OB_INVALID_ID - 1);
  ASSERT_TRUE(NULL != const_joined_table);
  ASSERT_TRUE(joined_table->same_as(*const_joined_table));

  ASSERT_EQ(OB_SUCCESS, dest_stmt.assign(*select_stmt));

  // test not strict
  ObObj obj;
  obj.set_varchar("");
  ASSERT_EQ(OB_SUCCESS, session_info_.update_sys_variable_by_name(OB_SV_SQL_MODE, obj));
  query = "insert into t4(c1) values (1)";
  do_resolve(query, stmt, false, JSON_FORMAT, OB_SUCCESS, false);
}

void TestResolver::do_expand_view_test() {
  static const char* test_file = "./sql/expand_view.test";
  static const char* tmp_file = "./result/expand_view.tmp";
  static const char* result_file = "./result/expand_view.result";

  _OB_LOG(INFO, "%s\t%s\t%s", test_file, tmp_file, result_file);

  std::ifstream if_sql(test_file);
  ASSERT_TRUE(if_sql.is_open());
  std::ofstream of_tmp(tmp_file);
  ASSERT_TRUE(of_tmp.is_open());
  ObStmt *stmt = NULL;

  std::string line;
  int64_t case_id = 0;
  bool is_print = false;
  int64_t expect_error = 0;
  char *w;
  char *p;
  while (std::getline(if_sql, line)) {
    if (line.size() <= 0) continue;
    if (line.at(0) == '#') continue;
    if (strncmp(line.c_str(), "--error", strlen("--error")) == 0) {
      p = const_cast<char*>(line.c_str());
      w = strsep(&p, " ");
      expect_error = atol(p);
      continue;
    }
    UNUSED(w);
    if (line.at(0) == '\r' || line.at(0) == '\n' ) continue;

    // @nijia.nj 为了方便加case，这里硬编码，任何一条语句执行之前都跑一次‘drop view if exists v’
    do_load_sql("drop view if exists v", stmt, is_print, TREE_FORMAT, expect_error);
    allocator_.reset();
    stmt_factory_.destory();
    expr_factory_.destory();
    stmt = NULL;
    do_load_sql(line.c_str(), stmt, is_print, TREE_FORMAT, expect_error);
    ASSERT_TRUE(OB_SUCCESS == expect_error);
    if (stmt::T_CREATE_TABLE == stmt->get_stmt_type()) {
      const ObTableSchema &schema = static_cast<ObCreateTableStmt*>(stmt)->get_create_table_arg().schema_;
      if (schema.is_view_table()) {
        of_tmp << "***************   Case "<< ++case_id << "   ***************" << std::endl;
        _OB_LOG(INFO, "case %ld: query str %s", case_id, line.c_str());
        const char *view_definition = schema.get_view_schema().get_view_definition();
        of_tmp << "before : " << line << std::endl;
        of_tmp << "after : create view " << schema.get_table_name() << " as "<< view_definition << std::endl;
      }
    }
    allocator_.reset();
    stmt_factory_.destory();
    expr_factory_.destory();
  }
  of_tmp.close();
  if_sql.close();
  _OB_LOG(INFO, "test expand_view finished!, total %ld case", case_id);
  // verify result
  // ObSqlString cmd;
  // cmd.assign_fmt("diff -u %s %s", tmp_file, result_file);
  // system(cmd.ptr());
  is_equal_content(tmp_file, result_file);
}

void TestResolver::do_stmt_tostring_test() {
  static const char* test_file = "./sql/stmt_tostring.test";
  static const char* tmp_file = "./result/stmt_tostring.tmp";
  static const char* result_file = "./result/stmt_tostring.result";

  _OB_LOG(INFO, "%s\t%s\t%s", test_file, tmp_file, result_file);

  std::ifstream if_sql(test_file);
  ASSERT_TRUE(if_sql.is_open());
  std::ofstream of_tmp(tmp_file);
  ASSERT_TRUE(of_tmp.is_open());
  ObStmt *stmt = NULL;

  std::string line;
  int64_t case_id = 0;
  bool is_print = false;
  int64_t expect_error = 0;
  char *w;
  char *p;
  while (std::getline(if_sql, line)) {
    if (line.size() <= 0) continue;
    if (line.at(0) == '#') continue;
    if (strncmp(line.c_str(), "--error", strlen("--error")) == 0) {
      p = const_cast<char*>(line.c_str());
      w = strsep(&p, " ");
      expect_error = atol(p);
      continue;
    }
    UNUSED(w);
    if (line.at(0) == '\r' || line.at(0) == '\n' ) continue;

    stmt = NULL;
    do_load_sql(line.c_str(), stmt, is_print, TREE_FORMAT, expect_error);
    ASSERT_TRUE(OB_SUCCESS == expect_error);
    ASSERT_TRUE(stmt::T_SELECT == stmt->get_stmt_type());
    char buffer[OB_MAX_SQL_LENGTH];
    memset(buffer, '\0', OB_MAX_SQL_LENGTH);
    int64_t pos = 0;
    ObSelectStmtPrinter stmt_printer(buffer, OB_MAX_SQL_LENGTH, &pos,
       static_cast<ObSelectStmt*>(stmt), ObObjPrintParams());
    stmt_printer.do_print();
    buffer[pos] = '\0';
    of_tmp << "***************   Case "<< ++case_id << "   ***************" << std::endl;
    _OB_LOG(INFO, "case %ld: query str %s", case_id, line.c_str());
    of_tmp << "before : " << line << std::endl;
    of_tmp << "after : " << buffer << std::endl;
    allocator_.reset();
    stmt_factory_.destory();
    expr_factory_.destory();
  }
  of_tmp.close();
  if_sql.close();
  _OB_LOG(INFO, "test stmt to sqlstring finished!, total %ld case", case_id);
  // verify result
  // ObSqlString cmd;
  // cmd.assign_fmt("diff -u %s %s", tmp_file, result_file);
  // system(cmd.ptr());
  is_equal_content(tmp_file, result_file);
}

void TestResolver::input_test_from_cmd(){
  std::ifstream if_schema(schema_file_path_);
  ASSERT_TRUE(if_schema.is_open());
  std::string line;
  std::string schema_sql;
  while (std::getline(if_schema, line)){
     schema_sql += "|\t";
     schema_sql += line;
     schema_sql += '\n';
  }
  if_schema.close();
  ObStmt *stmt = NULL;
  bool is_print = true;
  const char * line_separator = "-------------------------------------------------------------";
  while(true){
    std::cout << line_separator << std::endl;
    std::cout << "|\t SQL in test_resolver.schema" << std::endl;
    std::cout << line_separator << std::endl;
    std::cout << schema_sql;
    std::cout << line_separator << std::endl;
    std::string sql;
    std::cout << "Please Input SQL: \n>";
    if (getline(std::cin, sql)){
      std::cout << line_separator << std::endl;
      stmt = NULL;
      std::cout << "SQL=>" << sql << std::endl;
      std::cout << line_separator << std::endl;
      do_load_sql(sql.c_str(), stmt, is_print, TREE_FORMAT);
      //      std::cout << CSJ(*stmt) << std::endl;
    }
    allocator_.reset();
    stmt_factory_.destory();
    expr_factory_.destory();
  }
  //test_resolver->TearDown();
}
}//end of namespace test

int main(int argc, char **argv)
{
  test::clp.test_input_from_cmd = false;
  test::clp.print_schema_detail_info = false;
  test::clp.record_test_result =false;
  //argc = 1;
  system("rm -rf test_resolver.log");
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_resolver.log", true);
  ::testing::InitGoogleTest(&argc,argv);
  test::parse_cmd_line_param(argc, argv, test::clp);
  init_sql_factories();
  std::cout<<"clp:record_test_result:::"<<test::clp.record_test_result<<std::endl;
  return RUN_ALL_TESTS();
}
