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

#define USING_LOG_PREFIX SQL


#include <gtest/gtest.h>
#include <stdarg.h>
#include "lib/tbsys.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/px/ob_dfo_mgr.h"
#include "sql/engine/px/ob_px_coord.h"
#include "lib/utility/ob_tracepoint.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

class ObDFOMgrTest : public ::testing::Test
{
public:

  ObDFOMgrTest();
  virtual ~ObDFOMgrTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObDFOMgrTest);
private:
  // data members
};
ObDFOMgrTest::ObDFOMgrTest()
{
}

ObDFOMgrTest::~ObDFOMgrTest()
{
}

void ObDFOMgrTest::SetUp()
{
}

void ObDFOMgrTest::TearDown()
{
}

TEST_F(ObDFOMgrTest, left_deep_tree)
{
  ObDFOMgr dfo_mgr;
  ObDFO hash, prob, join, sort, qc;
  ASSERT_EQ(OB_SUCCESS, qc.append_child_dfo(&sort));
  ASSERT_EQ(OB_SUCCESS, sort.append_child_dfo(&join));
  ASSERT_EQ(OB_SUCCESS, join.append_child_dfo(&hash));
  ASSERT_EQ(OB_SUCCESS, join.append_child_dfo(&prob));

  hash.set_parent(&join);
  prob.set_parent(&join);
  join.set_parent(&sort);
  sort.set_parent(&qc);

/*

              qc
              /   <-- edge4
            sort
             /  <-- edge3
           join
 edge1-->  /   \ <-- edge2
         hash    prob

*/

  ASSERT_EQ(OB_SUCCESS, ObDFOSchedOrderGenerator::generate(dfo_mgr, &qc));

  ObArray<ObDFO *> dfos;
  ASSERT_EQ(OB_SUCCESS, dfo_mgr.get_ready_dfos(dfos));
  ASSERT_TRUE(dfos.count() == 2);
  ASSERT_TRUE(dfos.at(0) == &hash);
  ASSERT_TRUE(dfos.at(1) == &join);

  ASSERT_EQ(OB_SUCCESS, dfo_mgr.get_ready_dfos(dfos));
  ASSERT_TRUE(dfos.count() == 0);

  hash.set_state(ObDFOState::FINISH);

  ASSERT_EQ(OB_SUCCESS, dfo_mgr.get_ready_dfos(dfos));
  ASSERT_EQ(dfos.count(), 2);
  ASSERT_TRUE(dfos.at(0) == &prob);
  ASSERT_TRUE(dfos.at(1) == &join);

  ASSERT_EQ(OB_SUCCESS, dfo_mgr.get_ready_dfos(dfos));
  ASSERT_EQ(dfos.count(), 0);

  prob.set_state(ObDFOState::FINISH);

  ASSERT_EQ(OB_SUCCESS, dfo_mgr.get_ready_dfos(dfos));
  ASSERT_TRUE(dfos.count() == 2);
  ASSERT_TRUE(dfos.at(0) == &join);
  ASSERT_TRUE(dfos.at(1) == &sort);

  join.set_state(ObDFOState::FINISH);

  ASSERT_EQ(OB_SUCCESS, dfo_mgr.get_ready_dfos(dfos));
  ASSERT_TRUE(dfos.count() == 2);
  ASSERT_TRUE(dfos.at(0) == &sort);
  ASSERT_TRUE(dfos.at(1) == &qc);

  sort.set_state(ObDFOState::FINISH);

  ASSERT_EQ(OB_ITER_END, dfo_mgr.get_ready_dfos(dfos));
}

TEST_F(ObDFOMgrTest, normalizer)
{
  ObDFOMgr dfo_mgr;
  ObDFO h1, h2, prob, nlj, hj, sort, qc;

  ASSERT_EQ(OB_SUCCESS, qc.append_child_dfo(&hj));
  ASSERT_EQ(OB_SUCCESS, hj.append_child_dfo(&h2));
  ASSERT_EQ(OB_SUCCESS, hj.append_child_dfo(&nlj));
  ASSERT_EQ(OB_SUCCESS, nlj.append_child_dfo(&h1));
  ASSERT_EQ(OB_SUCCESS, nlj.append_child_dfo(&prob));
  h1.set_parent(&nlj);
  prob.set_parent(&nlj);
  h2.set_parent(&hj);
  nlj.set_parent(&hj);
  hj.set_parent(&qc);

  ASSERT_EQ(OB_SUCCESS, ObDFOSchedOrderGenerator::generate(dfo_mgr, &qc));

/*

              qc
               / (e5)
              hj
        (e4)/    \  (e3)
          h2     nlj
           (e1)  /   \ (e2)
               h1    prob

*/

  // 遍历 edge，然后逐个加入 dfo_mgr
  ObDFO *c1,*c2;
  ASSERT_EQ(OB_SUCCESS, nlj.get_child_dfo(0, c1));
  ASSERT_EQ(OB_SUCCESS, nlj.get_child_dfo(1, c2));
  ASSERT_EQ(&h1, c1);
  ASSERT_EQ(&prob, c2);

  ASSERT_EQ(OB_SUCCESS, hj.get_child_dfo(0, c1));
  ASSERT_EQ(OB_SUCCESS, hj.get_child_dfo(1, c2));
  ASSERT_EQ(&nlj, c1);
  ASSERT_EQ(&h2, c2);

  ASSERT_EQ(OB_SUCCESS, qc.get_child_dfo(0, c1));
  ASSERT_EQ(&hj, c1);


  ObArray<ObDFO *> dfos;
  ASSERT_EQ(OB_SUCCESS, dfo_mgr.get_ready_dfos(dfos));
  ASSERT_EQ(dfos.count(), 2);
  ASSERT_EQ(dfos.at(0), &h1);
  ASSERT_EQ(dfos.at(1), &nlj);

  ASSERT_EQ(OB_SUCCESS, dfo_mgr.get_ready_dfos(dfos));
  ASSERT_EQ(dfos.count(), 0);

  h1.set_state(ObDFOState::FINISH);

  ASSERT_EQ(OB_SUCCESS, dfo_mgr.get_ready_dfos(dfos));
  ASSERT_EQ(dfos.count(), 2);
  ASSERT_EQ(dfos.at(0), &prob);
  ASSERT_EQ(dfos.at(1), &nlj);

  prob.set_state(ObDFOState::FINISH);

  ASSERT_EQ(OB_SUCCESS, dfo_mgr.get_ready_dfos(dfos));
  ASSERT_EQ(dfos.count(), 2);
  ASSERT_EQ(dfos.at(0), &nlj);
  ASSERT_EQ(dfos.at(1), &hj);

  ASSERT_EQ(OB_SUCCESS, dfo_mgr.get_ready_dfos(dfos));
  ASSERT_EQ(dfos.count(), 2);
  ASSERT_EQ(dfos.at(0), &h2);
  ASSERT_EQ(dfos.at(1), &hj);

  h2.set_state(ObDFOState::FINISH);

  ASSERT_EQ(OB_SUCCESS, dfo_mgr.get_ready_dfos(dfos));
  ASSERT_EQ(dfos.count(), 0);

  nlj.set_state(ObDFOState::FINISH);

  ASSERT_EQ(OB_SUCCESS, dfo_mgr.get_ready_dfos(dfos));
  ASSERT_EQ(dfos.count(), 2);
  ASSERT_EQ(dfos.at(0), &hj);
  ASSERT_EQ(dfos.at(1), &qc);

  hj.set_state(ObDFOState::FINISH);

  ASSERT_EQ(OB_ITER_END, dfo_mgr.get_ready_dfos(dfos));
}



TEST_F(ObDFOMgrTest, px_coord)
{
  class MyRunnable : public share::ObThreadPool
  {
  public:
    void run1()
    {
      int ret = OB_SUCCESS;
      LOG_INFO("start thread", K(thread));
      ObPXCoord *coord = reinterpret_cast<ObPXCoord *>(arg);
      if (OB_FAIL(coord->open())) {
        LOG_WARN("fail open coord", K(ret));
      }
      LOG_INFO("end thread", K(thread));
    }
  };

  ObPXCoord coord;
  ObDFO h1, h2, prob, nlj, hj, qc;

  ASSERT_EQ(OB_SUCCESS, qc.append_child_dfo(&hj));
  ASSERT_EQ(OB_SUCCESS, hj.append_child_dfo(&h2));
  ASSERT_EQ(OB_SUCCESS, hj.append_child_dfo(&nlj));
  ASSERT_EQ(OB_SUCCESS, nlj.append_child_dfo(&h1));
  ASSERT_EQ(OB_SUCCESS, nlj.append_child_dfo(&prob));
  h1.set_parent(&nlj);
  prob.set_parent(&nlj);
  h2.set_parent(&hj);
  nlj.set_parent(&hj);
  hj.set_parent(&qc);
  h1.set_id(1);
  prob.set_id(2);
  h2.set_id(3);
  nlj.set_id(4);
  hj.set_id(5);
  qc.set_id(6);

/*

              qc
               / (e5)
              hj
        (e4)/    \  (e3)
          h2     nlj
           (e1)  /   \ (e2)
               h1    prob

*/


  coord.set_dfo_tree(qc);

  MyRunnable myrun;
  obsys::CThread px_thread;
  px_thread.start(&myrun, &coord);

  // 模拟消息到达
  h2.set_state(ObDFOState::FINISH);
  h1.set_state(ObDFOState::FINISH);
  usleep(1000 * 1000);
  prob.set_state(ObDFOState::FINISH);
  usleep(2000 * 1000);
  nlj.set_state(ObDFOState::FINISH);
  usleep(2000 * 1000);
  hj.set_state(ObDFOState::FINISH);
  usleep(2000 * 1000);

  px_thread.join();
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
