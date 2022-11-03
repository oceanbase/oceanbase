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
#include <thread>
#include <utility>
#include "lib/container/ob_se_array.h"
#include "lib/allocator/page_arena.h"
#include "common/row/ob_row.h"
#include "sql/dtl/ob_dtl_task.h"
#include "sql/dtl/ob_dtl_channel.h"
#include "sql/dtl/ob_dtl_channel_group.h"
#include "sql/dtl/ob_dtl.h"

using namespace std;
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::sql::dtl;

class Runnable {
public:
  virtual void run() = 0;
};

class Producer
    : public ObDtlTask, public Runnable
{
public:
  void run()
  {
    cout << "Producer" << endl;
    int ret = link_chans();
    EXPECT_EQ(OB_SUCCESS, ret);
    auto row = new ObNewRow();
    int cnt = 100;
    while (cnt--) {
      EXPECT_EQ(OB_SUCCESS, chans_[0]->push(row));
      cout << "push row, remain: " << cnt << endl;
    }
    unlink_chans();
  }
};

class Consumer
    : public ObDtlTask, public Runnable
{
public:
  void run()
  {
    cout << "Consumer" << endl;
    int ret = link_chans();
    EXPECT_EQ(OB_SUCCESS, ret);
    const ObNewRow *row = nullptr;
    int cnt = 0;
    while (OB_SUCC(chans_[0]->pop(row, 3000))) {
      cout << "pop row, got: " << cnt << endl;
      cnt++;
    }
    EXPECT_EQ(100, cnt);
  }
};

void th(void *arg)
{
  Runnable *task = reinterpret_cast<Runnable*>(arg);
  task->run();
}

class TestDtl
    : public ::testing::Test
{
public:
  virtual void SetUp()
  {
    cout << "set up" << endl;
  }

  virtual void TearDown()
  {
    cout << "tear down" << endl;
  }

protected:
  Consumer c;
  Producer p;
  thread *thes_[10];
};

TEST_F(TestDtl, TestName)
{
  ObDtlChannelGroup dtl_cg;

  const uint64_t tenant_id = 1;
  ASSERT_EQ(OB_SUCCESS, dtl_cg.make_channel(tenant_id, p, c));

  cout << "make chans done" << endl;

  thes_[0] = new thread(th, dynamic_cast<Runnable*>(&p));
  cout << thes_[0]->get_id() << endl;
  thes_[0]->join();
  thes_[1] = new thread(th, dynamic_cast<Runnable*>(&c));
  cout << thes_[1]->get_id() << endl;
  thes_[1]->join();
}

int main(int argc, char *argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level(3);
  DTL.init();
  return RUN_ALL_TESTS();
}
