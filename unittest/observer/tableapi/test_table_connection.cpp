/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#define private public
#include "observer/table/ob_table_connection_mgr.h"
#include "lib/time/ob_time_utility.h"
#include "rpc/ob_request.h"
#include "rpc/frame/ob_net_easy.cpp"
#include "share/ob_thread_pool.h"
#include "lib/container/ob_array.h"
using namespace oceanbase::table;
using namespace oceanbase::common;
using namespace oceanbase::rpc;
using namespace oceanbase::share;

namespace oceanbase {
namespace observer {
class TestTableConnection : public::testing::Test
{
public:
  TestTableConnection() {}
  virtual ~TestTableConnection() {}
  virtual void SetUp() {}
  virtual void TearDown() {}
};

#define CONS_FAKE_REQUEST(ob_req, ob_addr)\
  ObRequest ob_req(ObRequest::OB_RPC);\
  easy_request_t ez_req;\
  easy_message_session_t ms;\
  easy_connection_t c;\
  easy_addr_t ez_addr = __to_ez_addr(ob_addr);\
  c.addr = ez_addr;\
  ms.c = &c;\
  ez_req.ms = &ms;\
  ob_req.ez_req_ = &ez_req;

#define CONS_FAKE_EZ_CONN(ez_conn, ob_addr)\
  easy_connection_t ez_conn;\
  easy_addr_t ez_conn_addr = __to_ez_addr(ob_addr);\
  ez_conn.addr = ez_conn_addr;\

// mock a connection which used by multiply thread
class MockObTableRequestSender  : public ObThreadPool
{
public:
  MockObTableRequestSender() {}
  MockObTableRequestSender(const ObAddr &client_addr, int64_t thread_cnt, int64_t tenant_id,
                                    int64_t database_id, int64_t user_id, int64_t request_times)
  : client_addr_(client_addr),
    thread_cnt_(thread_cnt),
    tenant_id_(tenant_id),
    database_id_(database_id),
    user_id_(user_id),
    request_times_(request_times)
  {
    set_thread_count(thread_cnt_);
  }

  virtual ~MockObTableRequestSender() {};
  virtual void run1();
  TO_STRING_KV(K_(client_addr), K_(thread_cnt), K_(tenant_id), K_(database_id), K_(user_id), K_(request_times));
private:
  ObAddr client_addr_;
  int64_t thread_cnt_;
  int64_t tenant_id_;
  int64_t database_id_;
  int64_t user_id_;
  int64_t request_times_;
};

void MockObTableRequestSender::run1()
{
  CONS_FAKE_REQUEST(req, client_addr_);
  for(int64_t i = 0; i < request_times_; i++) {
    ASSERT_EQ(OB_SUCCESS, ObTableConnectionMgr::get_instance().update_table_connection(&req, tenant_id_, database_id_, user_id_));
  }
}

// mock the ObSrvRpcHandler::on_close
class MockObTableConnCloser  : public ObThreadPool
{
public:
  explicit MockObTableConnCloser(const ObAddr &client_addr)
  : client_addr_(client_addr)
  {
    set_thread_count(1);
  }
  virtual ~MockObTableConnCloser() {};
  virtual void run1();
  TO_STRING_KV(K_(client_addr));
private:
  const ObAddr client_addr_;
};

void MockObTableConnCloser::run1()
{
  CONS_FAKE_EZ_CONN(ez_conn, client_addr_)
  ObTableConnectionMgr::get_instance().on_conn_close(&ez_conn);
}

// test correctness of table connection init
TEST_F(TestTableConnection, test_table_connection)
{
  // fake connection
  ObTableConnection conn;
  ObAddr addr(2130706433, 6001);
  int64_t tenant_id = 1001;
  int64_t database_id = 99999;
  int64_t user_id = 99999;
  ASSERT_EQ(OB_SUCCESS, conn.init(addr, tenant_id, database_id, user_id));
  ASSERT_EQ(addr, conn.get_addr());
  ASSERT_EQ(tenant_id, conn.get_tenant_id());
  ASSERT_EQ(database_id, conn.get_database_id());
  ASSERT_EQ(user_id, conn.get_user_id());

  int64_t first_active_time = conn.get_first_active_time();
  ASSERT_EQ(first_active_time, conn.get_last_active_time());
  int64_t new_active_time = ObTimeUtility::current_time();
  conn.update_last_active_time(new_active_time);
  ASSERT_EQ(first_active_time, conn.get_first_active_time());
  ASSERT_EQ(new_active_time, conn.get_last_active_time());

  ObAddr invalid_addr;
  ASSERT_EQ(OB_INVALID_ARGUMENT, conn.init(invalid_addr, tenant_id, database_id, user_id));
}

// test basic interface correctness in connection mgr
TEST_F(TestTableConnection, test_connection_mgr)
{
  ObAddr addr(2130706433, 6001);
  int64_t tenant_id = 1001;
  int64_t database_id = 99999;
  int64_t user_id = 99999;
  CONS_FAKE_REQUEST(req, addr);
  ASSERT_EQ(OB_SUCCESS, ObTableConnectionMgr::get_instance().update_table_connection(&req, tenant_id, database_id, user_id));
  ASSERT_EQ(1, ObTableConnectionMgr::get_instance().connection_map_.size());
  CONS_FAKE_EZ_CONN(ez_conn, addr);
  ObTableConnectionMgr::get_instance().on_conn_close(&ez_conn);
  ASSERT_EQ(0, ObTableConnectionMgr::get_instance().connection_map_.size());
}

// send mutliply request using single connection concurrently
// the size of connection map should be equal to the size of req_sender
// after closer execute, the size of connection map should be equal to 0
TEST_F(TestTableConnection, test_multi_request)
{
  const int64_t start = ObTimeUtility::current_time();
  const int64_t thread_cnt = 100;
  int64_t tenant_id = 1001;
  int64_t database_id = 99999;
  int64_t user_id = 99999;
  int64_t request_times  = 100;
  const ObAddr addr(2130706433, 1);
  MockObTableRequestSender req_sender(addr, thread_cnt, tenant_id, database_id, user_id, request_times);
  ASSERT_EQ(OB_SUCCESS, req_sender.start());
  req_sender.wait();
  ASSERT_EQ(1, ObTableConnectionMgr::get_instance().connection_map_.size());
  MockObTableConnCloser closer(addr);
  ASSERT_EQ(OB_SUCCESS, closer.start());
  closer.wait();
  ASSERT_EQ(0, ObTableConnectionMgr::get_instance().connection_map_.size());
  const int64_t duration = ObTimeUtility::current_time() - start;
  printf("time elapsed: %ldms\n", duration/1000);
}

// send mutliply request using different connection concurrently
// the size of connection map should be equal to the size of req_sender
// after closer execute, the size of connection map should be equal to 0
TEST_F(TestTableConnection, test_multi_connection)
{
  const int64_t start = ObTimeUtility::current_time();
  ObArenaAllocator alloc;
  const int64_t thread_cnt = 8;
  int64_t tenant_id = 1001;
  int64_t database_id = 99999;
  int64_t user_id = 99999;
  int64_t request_times  = 100;
  int64_t conn_cnt = 200;
  const int32_t ip = 2130706433;
  ObArray<MockObTableRequestSender *> senders;
  ObArray<MockObTableConnCloser *> closers;
  for (int64_t i = 1; i <= conn_cnt; i++) {
    MockObTableRequestSender *sender = OB_NEWx(MockObTableRequestSender, (&alloc), ObAddr(ip, i), thread_cnt,
                                               tenant_id, database_id, user_id, request_times);
    ASSERT_NE(nullptr, sender);
    ASSERT_EQ(OB_SUCCESS, senders.push_back(sender));

    MockObTableConnCloser *closer = OB_NEWx(MockObTableConnCloser, (&alloc), ObAddr(ip, i));
    ASSERT_NE(nullptr, closer);
    ASSERT_EQ(OB_SUCCESS, closers.push_back(closer));
  }
  int64_t curr_mem = ObMallocAllocator::get_instance()->get_tenant_hold(OB_SERVER_TENANT_ID);
  printf("current server tenant mem before sending request: %ld\n", curr_mem);

  const int64_t req_start = ObTimeUtility::current_time();
  for (int i = 0; i < conn_cnt; i++) {
    ASSERT_EQ(OB_SUCCESS, senders.at(i)->start());
  }
  for (int i = 0; i < conn_cnt; i++) {
    senders.at(i)->wait();
  }
  ASSERT_EQ(conn_cnt, ObTableConnectionMgr::get_instance().connection_map_.size());

  curr_mem = ObMallocAllocator::get_instance()->get_tenant_hold(OB_SERVER_TENANT_ID);
  printf("current server tenant mem after sending request: %ld\n", curr_mem);

  for (int i = 0; i < conn_cnt; i++) {
    ASSERT_EQ(OB_SUCCESS, closers.at(i)->start());
  }
  for (int i = 0; i < conn_cnt; i++) {
    closers.at(i)->wait();
  }
  ASSERT_EQ(0, ObTableConnectionMgr::get_instance().connection_map_.size());

  const int64_t end = ObTimeUtility::current_time();
  printf("request time elapsed: %ldms\n", (end - req_start)/1000);
  printf("all time elapsed: %ldms\n", (end - start)/1000);
}

} // namespace observer
} // namespace oceanbase

int main(int argc, char** argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_table_connection.log", true);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
