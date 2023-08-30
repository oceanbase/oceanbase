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

#define USING_LOG_PREFIX RS
#include <gtest/gtest.h>
#include "lib/utility/ob_test_util.h"
#include "rootserver/ob_rs_job_table_operator.h"
#include "../share/schema/db_initializer.h"
#include "lib/mysqlclient/ob_mysql_connection.h"

using namespace oceanbase::rootserver;
using namespace oceanbase::common;
using namespace oceanbase::share;
class TestRsJobTableOperator: public ::testing::Test
{
public:
  TestRsJobTableOperator();
  virtual ~TestRsJobTableOperator();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestRsJobTableOperator);
protected:
  // function members
protected:
  // data members
  schema::DBInitializer db_initer_;
};

TestRsJobTableOperator::TestRsJobTableOperator()
{
}

TestRsJobTableOperator::~TestRsJobTableOperator()
{
}

void TestRsJobTableOperator::SetUp()
{
  ASSERT_EQ(OB_SUCCESS, db_initer_.init());
  ASSERT_EQ(OB_SUCCESS, db_initer_.create_system_table(false));
  ASSERT_EQ(OB_SUCCESS, db_initer_.create_virtual_table(
                ObInnerTableSchema::all_virtual_partition_info_schema));

  LOG_INFO("DB INITER FINISHED");
}

void TestRsJobTableOperator::TearDown()
{
}

TEST_F(TestRsJobTableOperator, test_init)
{
  ObRsJobTableOperator op;
  ObAddr rs_addr;
  rs_addr.set_ip_addr("127.0.0.1", 8888);
  ASSERT_EQ(OB_SUCCESS, op.init(&db_initer_.get_sql_proxy(), rs_addr));
  ASSERT_EQ(OB_INIT_TWICE, op.init(&db_initer_.get_sql_proxy(), rs_addr));
}

TEST_F(TestRsJobTableOperator, test_api)
{
  int ret = OB_SUCCESS;
  ObAddr rs_addr;
  rs_addr.set_ip_addr("127.0.0.1", 8888);
  ASSERT_EQ(OB_SUCCESS, THE_RS_JOB_TABLE.init(&db_initer_.get_sql_proxy(), rs_addr));

  // create
  oceanbase::common::ObMySQLTransaction trans;
  ASSERT_EQ(OB_SUCCESS, trans.start(&db_initer_.get_sql_proxy()));
  int64_t job_id1 = RS_JOB_CREATE(ALTER_TENANT_LOCALITY, trans, "tenant_id", 1010, "tenant_name", "tt1");
  ASSERT_EQ(1, job_id1);
  LOG_INFO("created job id", K(job_id1));
  int64_t job_id2 = RS_JOB_CREATE(ALTER_TABLE_LOCALITY, trans, "table_id", 1011, "table_name", "t1");
  LOG_INFO("created job id", K(job_id2));
  ASSERT_EQ(2, job_id2);
  ASSERT_EQ(OB_SUCCESS, trans.end(true));

  // get
  ObRsJobInfo job_info;
  ret = RS_JOB_GET(job_id1, job_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  LOG_INFO("get job", K(job_info));
  ASSERT_EQ(job_id1, job_info.job_id_);
  ASSERT_EQ(0, job_info.progress_);
  ASSERT_EQ(1010, job_info.tenant_id_);
  ASSERT_EQ(0, job_info.tenant_name_.compare("tt1"));
  ASSERT_EQ(JOB_TYPE_ALTER_TENANT_LOCALITY, job_info.job_type_);
  ASSERT_EQ(JOB_STATUS_INPROGRESS, job_info.job_status_);
  int64_t not_exist_job_id = 100;
  ret = RS_JOB_GET(not_exist_job_id, job_info);
  ASSERT_NE(OB_SUCCESS, ret);

  // update progress 
  ASSERT_EQ(OB_SUCCESS, trans.start(&db_initer_.get_sql_proxy()));
  ret = RS_JOB_UPDATE_PROGRESS(job_id1, 50, trans);

  ASSERT_EQ(OB_SUCCESS, trans.end(true));
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = RS_JOB_GET(job_id1, job_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(50, job_info.progress_);
  ASSERT_EQ(OB_SUCCESS, trans.start(&db_initer_.get_sql_proxy()));
  ret = RS_JOB_UPDATE_PROGRESS(job_id2, 101, trans);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = RS_JOB_UPDATE_PROGRESS(job_id2, 50, trans);
  ASSERT_EQ(OB_SUCCESS, ret);

  // update
  ret = RS_JOB_UPDATE(job_id1, trans, "extra_info", "slflskdjfoiw");
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, trans.end(true));
  ret = RS_JOB_GET(job_id1, job_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, job_info.extra_info_.compare("slflskdjfoiw"));

  // find job
  ASSERT_EQ(OB_SUCCESS, trans.start(&db_initer_.get_sql_proxy()));
  ret = RS_JOB_FIND(ALTER_TENANT_LOCALITY, job_info, trans);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(job_id1, job_info.job_id_);
  ASSERT_EQ(50, job_info.progress_);

  // complete job
  ret = RS_JOB_COMPLETE(job_id1, 0, trans);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, trans.end(true));
  ret = RS_JOB_GET(job_id1, job_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(100, job_info.progress_);
  ASSERT_EQ(0, job_info.return_code_);
  ASSERT_EQ(JOB_STATUS_SUCCESS, job_info.job_status_);

  ASSERT_EQ(OB_SUCCESS, trans.start(&db_initer_.get_sql_proxy()));
  ret = RS_JOB_COMPLETE(job_id2, 1234, trans);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, trans.end(true));
  ret = RS_JOB_GET(job_id2, job_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(50, job_info.progress_);
  ASSERT_EQ(1234, job_info.return_code_);
  ASSERT_EQ(JOB_STATUS_FAILED, job_info.job_status_);

  // max job id
  ObRsJobTableOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(&db_initer_.get_sql_proxy(), rs_addr));
  ::oceanbase::share::ObDMLSqlSplicer dml;
  ASSERT_EQ(OB_SUCCESS, trans.start(&db_initer_.get_sql_proxy()));
  ASSERT_EQ(OB_SUCCESS, op.create_job(JOB_TYPE_ALTER_TENANT_LOCALITY, dml, job_id1, trans));
  ASSERT_EQ(3, job_id1);
  ASSERT_EQ(OB_SUCCESS, trans.end(true));

  // no transaction 
  job_id1 = RS_JOB_CREATE(ALTER_TENANT_LOCALITY, trans, "tenant_id", 1010, "tenant_name", "tt1");
  ASSERT_EQ(-1, job_id1);
  job_id1 = 1;
  ret = RS_JOB_UPDATE_PROGRESS(job_id1, 50, trans);
  ASSERT_EQ(OB_TRANS_CTX_NOT_EXIST, ret);
  ret = RS_JOB_UPDATE(job_id1, trans, "extra_info", "slflskdjfoiw");
  ASSERT_EQ(OB_TRANS_CTX_NOT_EXIST, ret);
  ret = RS_JOB_FIND(ALTER_TENANT_LOCALITY, job_info, trans);
  ASSERT_EQ(OB_TRANS_CTX_NOT_EXIST, ret);
  ret = RS_JOB_COMPLETE(job_id1, 0, trans);
  ASSERT_EQ(OB_TRANS_CTX_NOT_EXIST, ret);
}


TEST_F(TestRsJobTableOperator, test_all_18_properties)
{
  oceanbase::common::ObMySQLTransaction trans;
  ASSERT_EQ(OB_SUCCESS, trans.start(&db_initer_.get_sql_proxy()));
  int64_t job_id1 = RS_JOB_CREATE(ALTER_TENANT_LOCALITY, trans, "tenant_id", 1010);
  int ret = OB_SUCCESS;
  ret = RS_JOB_UPDATE(job_id1, trans,
                      "job_type", "abcd1",
                      "job_status", "abcd2",
                      "return_code", 7699,
                      "progress", 7700,
                      "tenant_id", 7701,
                      "tenant_name", "abcd3",
                      "database_id", 7702,
                      "database_name", "abcd4",
                      "table_id", 7703,
                      "table_name", "abcd5",
                      "partition_id", 7704,
                      "svr_ip", "127.0.0.3",
                      "svr_port", 7705,
                      "rs_svr_ip", "127.0.0.4",
                      "rs_svr_port", 7706,
                      "unit_id", 7707,
                      "sql_text", "abcd6",
                      "extra_info", "abcd7");
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, trans.end(true));
  ObRsJobInfo job_info;
  ret = RS_JOB_GET(job_id1, job_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(job_id1, job_info.job_id_);
  ASSERT_EQ(7699, job_info.return_code_);
  ASSERT_EQ(7700, job_info.progress_);
  ASSERT_EQ(7701, job_info.tenant_id_);
  ASSERT_EQ(7702, job_info.database_id_);
  ASSERT_EQ(7703, job_info.table_id_);
  ASSERT_EQ(7704, job_info.partition_id_);
  ASSERT_EQ(7705, job_info.svr_addr_.get_port());
  ASSERT_EQ(7706, job_info.rs_addr_.get_port());
  ASSERT_EQ(7707, job_info.unit_id_);
  ASSERT_EQ(0, job_info.job_type_str_.compare("abcd1"));
  ASSERT_EQ(0, job_info.job_status_str_.compare("abcd2"));
  ASSERT_EQ(0, job_info.tenant_name_.compare("abcd3"));
  ASSERT_EQ(0, job_info.database_name_.compare("abcd4"));
  ASSERT_EQ(0, job_info.table_name_.compare("abcd5"));
  ASSERT_EQ(0, job_info.sql_text_.compare("abcd6"));
  ASSERT_EQ(0, job_info.extra_info_.compare("abcd7"));
}

TEST_F(TestRsJobTableOperator, test_delete_rows)
{
  int ret = OB_SUCCESS;
  oceanbase::common::ObMySQLTransaction trans;
  ASSERT_EQ(OB_SUCCESS, trans.start(&db_initer_.get_sql_proxy()));
  for (int i = 0; OB_SUCCESS == ret && i < 199120; ++i)
  {
    int64_t job_id1 = RS_JOB_CREATE(ALTER_TENANT_LOCALITY, trans, "tenant_id", 1010);
    ASSERT_TRUE(job_id1 > 0);
  } // end for
  ASSERT_EQ(OB_SUCCESS, trans.end(true));
  ASSERT_TRUE(THE_RS_JOB_TABLE.get_row_count() > 199120);
  ASSERT_EQ(OB_SUCCESS, trans.start(&db_initer_.get_sql_proxy()));
  for (int i = 0; OB_SUCCESS == ret && i < 900; ++i)
  {
    int64_t job_id1 = RS_JOB_CREATE(ALTER_TENANT_LOCALITY, trans, "tenant_id", 1010);
    ASSERT_TRUE(job_id1 > 0);
  } // end for
  ASSERT_EQ(OB_SUCCESS, trans.end(true));
  ASSERT_TRUE(THE_RS_JOB_TABLE.get_row_count() < 200020);
}
int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
    OB_LOGGER.set_mod_log_levels("ALL.*:INFO,LIB.MYSQLC:ERROR");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
