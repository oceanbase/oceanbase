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

#define USING_LOG_PREFIX RPC_TEST

#include <gtest/gtest.h>
#include "share/ob_define.h"
#include "observer/mysql/ob_mysql_request_manager.h"
#include "observer/mysql/ob_mysql_result_set.h"

using namespace oceanbase;
using namespace oceanbase::obmysql;
using namespace oceanbase::common;
using namespace observer;
using namespace oceanbase::obsys;
using namespace std;
using namespace sql;

namespace oceanbase
{
namespace tests
{

class TestObMysqlRequestManager : public ::testing::Test {
public:
  int init() {return OB_SUCCESS;}

};

TEST_F(TestObMysqlRequestManager, init_test)
{
  ObMySQLRequestManager request_manager;
  int ret = request_manager.init(2 * 1024 * 1024, 1000);
  OB_ASSERT(OB_SUCC(ret));
}

TEST_F(TestObMysqlRequestManager, basic_test)
{
  ObMySQLRequestManager request_manager;
  int ret = request_manager.init(2*1024*1024, 100000);
  EXPECT_EQ(ret, OB_SUCCESS);

  //add record
  ObSQLSessionInfo session;
  ObString uname = ObString::make_string("test");
  ObArenaAllocator arena(ObModIds::TEST);
  session.init(1, &arena);
  session.set_user(uname, OB_DEFAULT_HOST_NAME, 10);
  ret = request_manager.record_request(COM_LOGIN, &session);
  EXPECT_EQ(OB_SUCCESS, ret);
  //no COM_LOGOUT in 1.0
  ret = request_manager.record_request(oceanbase::obmysql::COM_END, &session);
  EXPECT_EQ(OB_SUCCESS, ret);
  ObISlidingWindowData *record = NULL;
  const int64_t* ref = NULL;

  ret = request_manager.get(request_manager.get_start_idx(), record, ref);
  EXPECT_EQ(OB_SUCCESS, ret);
  ObMySQLRequestRecord *mr = dynamic_cast<ObMySQLRequestRecord*>(record);
  EXPECT_EQ(1, mr != NULL);
  EXPECT_EQ(COM_LOGIN, mr->type_);
  EXPECT_EQ(uname, mr->username_);
  EXPECT_EQ(1, mr->id_);
  ret = request_manager.revert(ref);
  EXPECT_EQ(OB_SUCCESS, ret);

  ret = request_manager.get(request_manager.get_start_idx()+1, record, ref);
  EXPECT_EQ(OB_SUCCESS, ret);
  mr = dynamic_cast<ObMySQLRequestRecord*>(record);
  EXPECT_EQ(1, mr != NULL);
  EXPECT_EQ(oceanbase::obmysql::COM_END, mr->type_);
  EXPECT_EQ(2, mr->id_);
  EXPECT_EQ(uname, mr->username_);
  ret = request_manager.revert(ref);
  EXPECT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestObMysqlRequestManager, result_set_test)
{
  ObMySQLRequestManager request_manager;
  int ret = request_manager.init(2*1024*1024, 100000);
  EXPECT_EQ(ret, OB_SUCCESS);

  //add record
  ObMySQLResultSet result;
  ObString test_str;
  test_str.make_string("test sql str");
  result.set_errcode(0);
  result.set_affected_rows(100);
  result.set_session(NULL);
  int64_t receive_ts = ObTimeUtility::current_time();
  ret = request_manager.record_request(oceanbase::obmysql::COM_QUERY, receive_ts, &result, test_str);
  EXPECT_EQ(ret, OB_SUCCESS);
  const int32_t to_push_record_num = 10000;
  for(int32_t i = 0; i < to_push_record_num; ++i)
  {
    ret = request_manager.record_request(oceanbase::obmysql::COM_QUERY, receive_ts, &result, test_str);
    EXPECT_EQ(ret, OB_SUCCESS);
    if (i % 1000 == 0)
    {
      _OB_LOG(INFO, "add %d record", i);
    }
    EXPECT_EQ(OB_SUCCESS, ret);
  }
}

static void do_push(ObMySQLRequestManager *manager)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo session;
  ObString uname = ObString::make_string("xurui");
  ObString test_str = ObString::make_string("test sql str");
  ObArenaAllocator allocator(ObModIds::TEST);
  uint32_t version = 0;
  session.init(0, 0, 0, &allocator);
  session.set_user(uname, OB_DEFAULT_HOST_NAME, 10);

  /*
  ObMySQLRawPacket packet;
  char sql[100];
  memset(sql, 'a', 100);
  packet.set_cmd(oceanbase::obmysql::COM_QUERY);
  packet.set_content(sql, 100);
  //packet.set_header(100,1);
  //packet.set_receive_ts(10000);
  */
  ObMySQLResultSet result;
  result.set_errcode(0);
  result.set_affected_rows(100);
  result.set_session(&session);
  int64_t receive_ts = ObTimeUtility::current_time();
  const int32_t to_push_record_num = 10000;
  for(int32_t i = 0; i < to_push_record_num; ++i)
  {
    ret = manager->record_request(oceanbase::obmysql::COM_QUERY, receive_ts, &result, test_str);
    if (i % 1000 == 0 && 0 != i)
    {
      _OB_LOG(INFO, "add %d record", i);
      manager->release_old();
    }
    EXPECT_EQ(OB_SUCCESS, ret);
  }
}

class PushRunnable:public share::ObThreadPool
{
public:
  void run1()
  {
    ObMySQLRequestManager *manager = reinterpret_cast<ObMySQLRequestManager*>(arg);
    _OB_LOG(DEBUG, "start thread %p", thread);
    do_push(manager);
  }
};

TEST_F(TestObMysqlRequestManager, multithread_test)
{
  int ret = OB_SUCCESS;
  ObMySQLRequestManager manager;
  ret = manager.init(512*1024*1024, 2000000);
  EXPECT_EQ(OB_SUCCESS, ret);
  PushRunnable runner;
  const int thread_num = 100;
  CThread threads[thread_num];
  for (int i = 0; i < thread_num; ++i)
  {
    threads[i].start(&runner, &manager);
  }
  for (int i = 0; i < thread_num; ++i)
  {
    threads[i].join();
  }
}

} // namespace test
} // namespace oceanbase

int main(int argc, char *argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  //get_tsi_warning_buffer()->set_warn_log_on(true);
  return RUN_ALL_TESTS();
}
