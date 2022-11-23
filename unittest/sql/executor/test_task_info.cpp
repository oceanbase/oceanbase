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

#include <gtest/gtest.h>
#include "sql/executor/ob_task_info.h"
#include "sql/executor/ob_fifo_receive.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObTaskInfoTest : public ::testing::Test
{
public:
  ObTaskInfoTest();
  virtual ~ObTaskInfoTest();
  virtual void SetUp();
  virtual void TearDown();
  void MakeSliceID(ObSliceID &slice_id) {
    int64_t valid_id_a = 1012;
    int64_t valid_id_b = 13012;
    int64_t valid_id_c = 130120;
    int64_t valid_id_d = 1;
    ObAddr server(ObAddr::IPV4, "127.0.0.1", 8080);
    slice_id.set_execution_id(valid_id_a);
    slice_id.set_job_id(valid_id_b);
    slice_id.set_task_id(valid_id_c);
    slice_id.set_slice_id(valid_id_d);
    slice_id.set_server(server);
  }

  void MakeTaskID(ObTaskID &task_id, int64_t a, int64_t b=1001, int64_t c=1002, int64_t d=1) {
    task_id.set_execution_id(a);
    task_id.set_job_id(b);
    task_id.set_task_id(c);
    task_id.set_task_id(d);
  }


  void MakeServer(ObAddr &svr) {
    ObAddr server(ObAddr::IPV4, "127.0.0.1", 8080);
    svr = server;
  }

  void MakeTaskID(ObTaskID &task_id) {
    int64_t valid_id_a = 1012;
    int64_t valid_id_b = 13012;
    int64_t valid_id_c = 130120;
    int64_t valid_id_d = 1;
    task_id.set_execution_id(valid_id_a);
    task_id.set_job_id(valid_id_b);
    task_id.set_task_id(valid_id_c);
    task_id.set_task_id(valid_id_d);
  }

private:
  // disallow copy
  ObTaskInfoTest(const ObTaskInfoTest &other);
  ObTaskInfoTest& operator=(const ObTaskInfoTest &other);
private:
  // data members
};
ObTaskInfoTest::ObTaskInfoTest()
{
}

ObTaskInfoTest::~ObTaskInfoTest()
{
}

void ObTaskInfoTest::SetUp()
{
}

void ObTaskInfoTest::TearDown()
{
}

TEST_F(ObTaskInfoTest, all_test)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObTaskInfo ti(allocator);

  // state test
  ASSERT_TRUE(OB_TASK_STATE_NOT_INIT == ti.get_state());
  ti.set_state(OB_TASK_STATE_INITED);
  ASSERT_TRUE(OB_TASK_STATE_INITED == ti.get_state());

  ObArenaAllocator alloc;
  // root op test
  ObPhyOperator *op = new ObFifoReceive(alloc);
  ASSERT_TRUE(NULL == ti.get_root_op());
  ti.set_root_op(op);
  ASSERT_TRUE(op == ti.get_root_op());

  // range test
  // TODO
  //

  // location test
  ObTaskLocation loc;
  ObAddr server;
  ObTaskID task_id;
  ObTaskLocation loc_tmp;
  ObAddr server_tmp;
  ObTaskID task_id_tmp;
  this->MakeTaskID(task_id);
  this->MakeServer(server);
  loc.set_ob_task_id(task_id);
  loc.set_server(server);
  ti.set_task_location(loc);
  loc_tmp = ti.get_task_location();
  task_id_tmp = loc_tmp.get_ob_task_id();
  server_tmp = loc_tmp.get_server();
  ASSERT_TRUE(task_id_tmp.equal(task_id));
  ASSERT_TRUE(server.hash() == server_tmp.hash());

}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
