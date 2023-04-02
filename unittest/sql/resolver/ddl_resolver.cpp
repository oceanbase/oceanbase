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

#include "sql/ob_sql_init.h"
#include <iterator>
#include <sys/time.h>
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

using namespace oceanbase::obrpc;
namespace test
{
const char* SQL_DIR = "sql";
const char* RESULT_DIR ="result";
struct SqlAndError {
  std::string sql;
  int64_t expect_error;
};
class TestResolver: public TestSqlUtils, public ::testing::Test
{
public:
  TestResolver();
  virtual ~TestResolver(){}
  virtual void SetUp();
  virtual void TearDown();
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


TEST_F(TestResolver, basic_test)
{
  //for rongxuan.test input sql in command line
  const char *postfix[] = {"test","tmp","result"};
  int64_t sql_postfix_len = strlen(postfix[0]);
  int64_t tmp_postfix_len = strlen(postfix[1]);
  int64_t result_postfix_len = strlen(postfix[2]);
  char file_name[3][FILE_PATH_LEN];
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
      _OB_LOG_RET(ERROR,OB_ERR_SYS, "file %s not exist!", file_name[0]);
      continue;
    }
    ASSERT_TRUE(if_sql.is_open());
    ObStmt *stmt = NULL;
    //std::ofstream of_tmp(file_name[1]);
    //ASSERT_TRUE(of_tmp.is_open()) << file_name[1];
    //if (!of_tmp.is_open()) {
    //  _OB_LOG(ERROR,"file %s not exist!", file_name[1]);
    //  continue;
    //}
    std::string line;
    std::vector<SqlAndError> sql_vector;
    bool is_print = false;
    int64_t expect_error = 0;
    int64_t use_time = 0;
    char *w;
    char *p;
    timeval start, end;
    while (std::getline(if_sql, line)) {
      if (line.size() <= 0) continue;
      if (line.at(0) == '#') continue;
      if (strncmp(line.c_str(), "--error", strlen("--error")) == 0) {
        p = const_cast<char*>(line.c_str());
        w = strsep(&p, " ");
        expect_error = atol(p);
        continue;
      }
      if (line.at(0) == '\r' || line.at(0) == '\n' ) continue;
      //stmt = NULL;
      SqlAndError sql_error;
      sql_error.sql = line;
      sql_error.expect_error = expect_error;
      sql_vector.push_back(sql_error);
      expect_error = 0;
    }
    UNUSED(w);
    if_sql.close();
    const char * file_name = clp.file_names_vector[i];
    int64_t length = strlen(file_name);
    std::string name(file_name + 14, file_name + length - 1);
    gettimeofday(&start, NULL);
    std::vector<SqlAndError>::iterator iter=sql_vector.begin();
    for (; iter != sql_vector.end(); iter++) {
      stmt = NULL;
      do_load_sql(iter->sql.c_str(),stmt, is_print, TREE_FORMAT, iter->expect_error);
    }
    gettimeofday(&end, NULL);
    use_time = (long int)(end.tv_sec - start.tv_sec) * 1000000 + (long int)(end.tv_usec - start.tv_usec);;
    std::cout<< name << " use time :" << use_time << std::endl;
  }
}
}
int main(int argc, char **argv)
{
  test::clp.test_input_from_cmd = false;
  test::clp.print_schema_detail_info = false;
  test::clp.record_test_result =false;
  //argc = 1;
  ::testing::InitGoogleTest(&argc,argv);
  test::parse_cmd_line_param(argc, argv, test::clp);
  OB_LOGGER.set_log_level("DEBUG");
  OB_LOGGER.set_file_name("ddl_resolver.log", true);
  init_sql_factories();
  std::cout<<"clp:record_test_result:::"<<test::clp.record_test_result<<std::endl;
  return RUN_ALL_TESTS();
}
