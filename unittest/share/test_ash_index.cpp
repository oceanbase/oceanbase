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

#define USING_LOG_PREFIX SHARE
#define private public
#define protected public
#include "share/ash/ob_active_sess_hist_list.h"
#undef private
#undef public
#include <gtest/gtest.h>

namespace oceanbase
{
namespace share
{
using namespace common;

class TestAshIndex : public ::testing::Test
{
public:
  virtual void SetUp();
  virtual void TearDown();
  void reset_ash_list()
  {
    ObActiveSessHistList::get_instance().write_pos_ = 0;
    ObActiveSessHistList::get_instance().list_.reset();
    ASSERT_EQ(OB_SUCCESS, ObActiveSessHistList::get_instance().init());
  }
  void reset_ash_list_with_write_pos(int64_t write_pos)
  {
    ObActiveSessHistList::get_instance().write_pos_ = 0;
    ObActiveSessHistList::get_instance().list_.reset();
    ASSERT_EQ(OB_SUCCESS, ObActiveSessHistList::get_instance().init());
    ObActiveSessHistList::get_instance().write_pos_ += write_pos;
  }
  void push_back_sample_time(int64_t sample_time)
  {
    int64_t write_pos = ObActiveSessHistList::get_instance().write_pos();
    ActiveSessionStat stat;
    stat.sample_time_ = sample_time;
    ObActiveSessHistList::get_instance().add(stat);
    ASSERT_EQ(ObActiveSessHistList::get_instance().write_pos(), write_pos + 1);
  }
  int test_body(int64_t write_pos);
  int check_iter_valid(const ObActiveSessHistList::Iterator &iter);
};

void TestAshIndex::SetUp()
{
  ASSERT_EQ(OB_SUCCESS, ObActiveSessHistList::get_instance().init());
}

void TestAshIndex::TearDown()
{
  reset_ash_list();
}

int TestAshIndex::check_iter_valid(const ObActiveSessHistList::Iterator &iter)
{
  int ret = OB_SUCCESS;
  if (!(iter.curr_ >= 0 && iter.end_ >= 0)) {
    if (iter.has_next()) {
      ret = OB_ERR_UNEXPECTED;
    }
  }
  return ret;
}

TEST_F(TestAshIndex, ash_index)
{
  int ret = OB_SUCCESS;

  // empty list
  auto iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(0, 0);
  ASSERT_EQ(false, iter.has_next());
  iter.init_with_sample_time_index(-1, -1);
  ASSERT_EQ(false, iter.has_next());
  iter.init_with_sample_time_index(999, 999);
  ASSERT_EQ(false, iter.has_next());

  // one node
  push_back_sample_time(10);
  iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(0, 0);
  ASSERT_EQ(false, iter.has_next());
  iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(10, 10);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(false, iter.has_next());
  iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(9, 10);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(false, iter.has_next());
  iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(10, 11);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(false, iter.has_next());
  iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(9, 12);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(false, iter.has_next());

  // several node
  reset_ash_list();
  push_back_sample_time(8);
  push_back_sample_time(9);
  push_back_sample_time(10);
  push_back_sample_time(10);
  push_back_sample_time(10);
  push_back_sample_time(11);
  push_back_sample_time(12);
  iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(0, 0);
  ASSERT_EQ(false, iter.has_next());
  iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(10, 10);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(false, iter.has_next());
  iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(9, 10);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(9, iter.next().sample_time_);
  ASSERT_EQ(false, iter.has_next());
  iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(10, 11);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(11, iter.next().sample_time_);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(false, iter.has_next());
  iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(9, 12);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(12, iter.next().sample_time_);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(11, iter.next().sample_time_);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(9, iter.next().sample_time_);
  ASSERT_EQ(false, iter.has_next());
  iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(7, 9);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(9, iter.next().sample_time_);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(8, iter.next().sample_time_);
  ASSERT_EQ(false, iter.has_next());
  iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(999, 996);
  ASSERT_EQ(false, iter.has_next());
  iter.init_with_sample_time_index(-1, -5);
  ASSERT_EQ(false, iter.has_next());
  iter.init_with_sample_time_index(-5, -1);
  ASSERT_EQ(false, iter.has_next());

  // timer reverse outcome is false, but should not core.
  reset_ash_list();
  push_back_sample_time(8);
  push_back_sample_time(9);
  push_back_sample_time(10);
  push_back_sample_time(10);
  push_back_sample_time(10);
  push_back_sample_time(9);
  push_back_sample_time(7);
  push_back_sample_time(10);
  iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(0, 0);
  ASSERT_EQ(OB_SUCCESS, check_iter_valid(iter));
  iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(10, 10);
  ASSERT_EQ(OB_SUCCESS, check_iter_valid(iter));
  iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(9, 10);
  ASSERT_EQ(OB_SUCCESS, check_iter_valid(iter));
  iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(10, 11);
  ASSERT_EQ(OB_SUCCESS, check_iter_valid(iter));
  iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(9, 12);
  ASSERT_EQ(OB_SUCCESS, check_iter_valid(iter));
}

TEST_F(TestAshIndex, ash_index_ring_buffer)
{
  int ret = OB_SUCCESS;
  reset_ash_list_with_write_pos(ObActiveSessHistList::get_instance().size());
  // empty list
  auto iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(0, 0);
  for (int i = 0; i < ObActiveSessHistList::get_instance().size(); i++) {
    ASSERT_EQ(true, iter.has_next());
    ASSERT_EQ(0, iter.next().sample_time_);
  }
  ASSERT_EQ(false, iter.has_next());
  iter.init_with_sample_time_index(-1, -1);
  ASSERT_EQ(false, iter.has_next());
  iter.init_with_sample_time_index(999, 999);
  ASSERT_EQ(false, iter.has_next());

  // one node
  reset_ash_list_with_write_pos(ObActiveSessHistList::get_instance().size());
  push_back_sample_time(10);
  iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(0, 0);
  for (int i = 0; i < ObActiveSessHistList::get_instance().size() - 1; i++) {
    ASSERT_EQ(true, iter.has_next());
    ASSERT_EQ(0, iter.next().sample_time_);
  }
  ASSERT_EQ(false, iter.has_next());
  iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(10, 10);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(false, iter.has_next());
  iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(9, 10);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(false, iter.has_next());
  iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(10, 11);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(false, iter.has_next());
  iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(9, 12);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(false, iter.has_next());

  // several node
  reset_ash_list_with_write_pos(ObActiveSessHistList::get_instance().size());
  push_back_sample_time(8);
  push_back_sample_time(9);
  push_back_sample_time(10);
  push_back_sample_time(10);
  push_back_sample_time(10);
  push_back_sample_time(11);
  push_back_sample_time(12);
  iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(0, 0);
  for (int i = 0; i < ObActiveSessHistList::get_instance().size() - 7; i++) {
    ASSERT_EQ(true, iter.has_next());
    ASSERT_EQ(0, iter.next().sample_time_);
  }
  ASSERT_EQ(false, iter.has_next());
  iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(10, 10);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(false, iter.has_next());
  iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(9, 10);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(9, iter.next().sample_time_);
  ASSERT_EQ(false, iter.has_next());
  iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(10, 11);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(11, iter.next().sample_time_);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(false, iter.has_next());
  iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(9, 12);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(12, iter.next().sample_time_);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(11, iter.next().sample_time_);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(9, iter.next().sample_time_);
  ASSERT_EQ(false, iter.has_next());
  iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(7, 9);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(9, iter.next().sample_time_);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(8, iter.next().sample_time_);
  ASSERT_EQ(false, iter.has_next());
  iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(999, 996);
  ASSERT_EQ(false, iter.has_next());
  iter.init_with_sample_time_index(-1, -5);
  ASSERT_EQ(false, iter.has_next());
  iter.init_with_sample_time_index(-5, -1);
  ASSERT_EQ(false, iter.has_next());

  // timer reverse outcome is false, but should not core.
  reset_ash_list_with_write_pos(ObActiveSessHistList::get_instance().size());
  push_back_sample_time(8);
  push_back_sample_time(9);
  push_back_sample_time(10);
  push_back_sample_time(10);
  push_back_sample_time(10);
  push_back_sample_time(9);
  push_back_sample_time(7);
  push_back_sample_time(10);
  iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(0, 0);
  ASSERT_EQ(OB_SUCCESS, check_iter_valid(iter));
  iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(10, 10);
  ASSERT_EQ(OB_SUCCESS, check_iter_valid(iter));
  iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(9, 10);
  ASSERT_EQ(OB_SUCCESS, check_iter_valid(iter));
  iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(10, 11);
  ASSERT_EQ(OB_SUCCESS, check_iter_valid(iter));
  iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(9, 12);
  ASSERT_EQ(OB_SUCCESS, check_iter_valid(iter));
}

TEST_F(TestAshIndex, ash_overwrite)
{
  int ret = OB_SUCCESS;

  // empty list
  auto iter = ObActiveSessHistList::get_instance().create_iterator();
  push_back_sample_time(0);
  iter.init_with_sample_time_index(0, 0);
  ASSERT_EQ(false, iter.has_next());
  iter.init_with_sample_time_index(-1, -1);
  ASSERT_EQ(false, iter.has_next());
  iter.init_with_sample_time_index(999, 999);
  ASSERT_EQ(false, iter.has_next());

  reset_ash_list();
  iter = ObActiveSessHistList::get_instance().create_iterator();
  push_back_sample_time(999);
  iter.init_with_sample_time_index(0, 0);
  ASSERT_EQ(false, iter.has_next());
  iter.init_with_sample_time_index(-1, -1);
  ASSERT_EQ(false, iter.has_next());
  iter.init_with_sample_time_index(999, 999);
  ASSERT_EQ(false, iter.has_next());

  reset_ash_list_with_write_pos(ObActiveSessHistList::get_instance().size());
  iter = ObActiveSessHistList::get_instance().create_iterator();
  push_back_sample_time(0);
  iter.init_with_sample_time_index(0, 0);
  for (int i = 0; i < ObActiveSessHistList::get_instance().size() - 1; i++) {
    ASSERT_EQ(true, iter.has_next()) << "current count:" << i;
    ASSERT_EQ(0, iter.next().sample_time_);
  }
  ASSERT_EQ(false, iter.has_next());
  iter = ObActiveSessHistList::get_instance().create_iterator();
  push_back_sample_time(0);
  iter.init_with_sample_time_index(0, 0);
  push_back_sample_time(0);
  for (int i = 0; i < ObActiveSessHistList::get_instance().size() - 2; i++) {
    ASSERT_EQ(true, iter.has_next()) << "current count:" << i;
    ASSERT_EQ(0, iter.next().sample_time_);
  }
  ASSERT_EQ(false, iter.has_next()) << "write pos:" << iter.list_->write_pos() << " curr:" << iter.curr_;
  iter.init_with_sample_time_index(-1, -1);
  ASSERT_EQ(false, iter.has_next());
  iter.init_with_sample_time_index(999, 999);
  ASSERT_EQ(false, iter.has_next());

  reset_ash_list_with_write_pos(ObActiveSessHistList::get_instance().size());
  iter = ObActiveSessHistList::get_instance().create_iterator();
  push_back_sample_time(999);
  iter.init_with_sample_time_index(0, 0);
  for (int i = 0; i < ObActiveSessHistList::get_instance().size() - 1; i++) {
    ASSERT_EQ(true, iter.has_next()) << "current count:" << i;
    ASSERT_EQ(0, iter.next().sample_time_);
  }
  ASSERT_EQ(false, iter.has_next());
  iter.init_with_sample_time_index(-1, -1);
  ASSERT_EQ(false, iter.has_next());

  iter.init_with_sample_time_index(999, 999);
  ASSERT_EQ(false, iter.has_next());
  iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(999, 999);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(999, iter.next().sample_time_);
  ASSERT_EQ(false, iter.has_next());

  // one node
  reset_ash_list_with_write_pos(ObActiveSessHistList::get_instance().size());
  iter = ObActiveSessHistList::get_instance().create_iterator();
  push_back_sample_time(10);
  iter.init_with_sample_time_index(0, 0);
  for (int i = 0; i < ObActiveSessHistList::get_instance().size() - 1; i++) {
    ASSERT_EQ(true, iter.has_next()) << "current count:" << i;
    ASSERT_EQ(0, iter.next().sample_time_);
  }
  ASSERT_EQ(false, iter.has_next());
  iter = ObActiveSessHistList::get_instance().create_iterator();
  push_back_sample_time(10);
  iter.init_with_sample_time_index(10, 10);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(false, iter.has_next());
  iter = ObActiveSessHistList::get_instance().create_iterator();
  push_back_sample_time(11);
  iter.init_with_sample_time_index(9, 11);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(false, iter.has_next());
  iter = ObActiveSessHistList::get_instance().create_iterator();
  push_back_sample_time(12);
  iter.init_with_sample_time_index(10, 11);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(11, iter.next().sample_time_);
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(false, iter.has_next());
  iter = ObActiveSessHistList::get_instance().create_iterator();
  push_back_sample_time(13);
  iter.init_with_sample_time_index(9, 12);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(12, iter.next().sample_time_);
  ASSERT_EQ(11, iter.next().sample_time_);
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(false, iter.has_next());

  // one node, add after binary search
  reset_ash_list_with_write_pos(ObActiveSessHistList::get_instance().size());
  iter = ObActiveSessHistList::get_instance().create_iterator();
  push_back_sample_time(10);
  iter.init_with_sample_time_index(0, 0);
  push_back_sample_time(10);
  for (int i = 0; i < ObActiveSessHistList::get_instance().size() - 2; i++) {
    ASSERT_EQ(true, iter.has_next());
    ASSERT_EQ(0, iter.next().sample_time_);
  }
  ASSERT_EQ(false, iter.has_next());
  iter = ObActiveSessHistList::get_instance().create_iterator();
  push_back_sample_time(10);
  iter.init_with_sample_time_index(10, 10);
  push_back_sample_time(10);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(false, iter.has_next());
  iter = ObActiveSessHistList::get_instance().create_iterator();
  push_back_sample_time(11);
  iter.init_with_sample_time_index(9, 11);
  push_back_sample_time(11);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(false, iter.has_next());
  iter = ObActiveSessHistList::get_instance().create_iterator();
  push_back_sample_time(12);
  iter.init_with_sample_time_index(10, 11);
  push_back_sample_time(12);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(11, iter.next().sample_time_);
  ASSERT_EQ(11, iter.next().sample_time_);
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(false, iter.has_next());
  iter = ObActiveSessHistList::get_instance().create_iterator();
  push_back_sample_time(13);
  iter.init_with_sample_time_index(9, 12);
  push_back_sample_time(13);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(12, iter.next().sample_time_);
  ASSERT_EQ(12, iter.next().sample_time_);
  ASSERT_EQ(11, iter.next().sample_time_);
  ASSERT_EQ(11, iter.next().sample_time_);
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(false, iter.has_next());

  // several node
  reset_ash_list_with_write_pos(ObActiveSessHistList::get_instance().size());
  push_back_sample_time(8);
  push_back_sample_time(9);
  push_back_sample_time(10);
  push_back_sample_time(10);
  push_back_sample_time(10);
  push_back_sample_time(11);
  push_back_sample_time(12);
  iter = ObActiveSessHistList::get_instance().create_iterator();
  push_back_sample_time(12);
  iter.init_with_sample_time_index(0, 0);
  push_back_sample_time(12);
  for (int i = 0; i < ObActiveSessHistList::get_instance().size() - 9; i++) {
    ASSERT_EQ(true, iter.has_next());
    ASSERT_EQ(0, iter.next().sample_time_);
  }
  ASSERT_EQ(false, iter.has_next());
  iter = ObActiveSessHistList::get_instance().create_iterator();
  push_back_sample_time(13);
  iter.init_with_sample_time_index(12, 13);
  push_back_sample_time(13);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(12, iter.next().sample_time_);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(12, iter.next().sample_time_);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(12, iter.next().sample_time_);
  ASSERT_EQ(false, iter.has_next());
  iter = ObActiveSessHistList::get_instance().create_iterator();
  push_back_sample_time(999);
  iter.init_with_sample_time_index(996, 999);
  ASSERT_EQ(false, iter.has_next());
  iter.init_with_sample_time_index(-1, -5);
  ASSERT_EQ(false, iter.has_next());
  iter.init_with_sample_time_index(-5, -1);
  ASSERT_EQ(false, iter.has_next());

  reset_ash_list_with_write_pos(ObActiveSessHistList::get_instance().size());
  push_back_sample_time(8);
  push_back_sample_time(9);
  push_back_sample_time(10);
  push_back_sample_time(10);
  push_back_sample_time(10);
  push_back_sample_time(11);
  push_back_sample_time(12);
  iter = ObActiveSessHistList::get_instance().create_iterator();
  ObActiveSessHistList::get_instance().write_pos_ += ObActiveSessHistList::get_instance().size();
  iter.init_with_sample_time_index(12, 13);
  ASSERT_EQ(false, iter.has_next());
  iter = ObActiveSessHistList::get_instance().create_iterator();
  ObActiveSessHistList::get_instance().write_pos_ += ObActiveSessHistList::get_instance().size() - 7;
  iter.init_with_sample_time_index(12, 13);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(12, iter.next().sample_time_);
  ASSERT_EQ(false, iter.has_next());

  reset_ash_list_with_write_pos(ObActiveSessHistList::get_instance().size());
  push_back_sample_time(8);
  push_back_sample_time(9);
  push_back_sample_time(10);
  push_back_sample_time(10);
  push_back_sample_time(10);
  push_back_sample_time(11);
  push_back_sample_time(12);
  iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(0, 8);
  for (int i = 0; i < ObActiveSessHistList::get_instance().size() - 8; i++) {
    push_back_sample_time(15);
  }
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(8, iter.next().sample_time_) << "write pos:" << iter.list_->write_pos() << " curr:" << iter.curr_;;
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(0, iter.next().sample_time_);
  ASSERT_EQ(false, iter.has_next());
}

TEST_F(TestAshIndex, ash_MIN_MAX)
{
  int ret = OB_SUCCESS;

  // empty list
  reset_ash_list();
  auto iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(0, INT64_MAX);
  ASSERT_EQ(false, iter.has_next());
  iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(INT64_MAX, 0);
  ASSERT_EQ(false, iter.has_next());

  // one node
  reset_ash_list();
  push_back_sample_time(10);
  iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(0, INT64_MAX);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(false, iter.has_next());
  iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(INT64_MAX, 0);
  ASSERT_EQ(false, iter.has_next());

  // several node
  reset_ash_list();
  push_back_sample_time(8);
  push_back_sample_time(9);
  push_back_sample_time(10);
  push_back_sample_time(10);
  push_back_sample_time(10);
  push_back_sample_time(11);
  push_back_sample_time(12);
  iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(0, INT64_MAX);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(12, iter.next().sample_time_);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(11, iter.next().sample_time_);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(9, iter.next().sample_time_);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(8, iter.next().sample_time_);
  ASSERT_EQ(false, iter.has_next());
  iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(INT64_MAX, 0);
  ASSERT_EQ(false, iter.has_next());

  // timer reverse outcome is false, but should not core.
  reset_ash_list();
  push_back_sample_time(8);
  push_back_sample_time(9);
  push_back_sample_time(10);
  push_back_sample_time(10);
  push_back_sample_time(10);
  push_back_sample_time(9);
  push_back_sample_time(7);
  push_back_sample_time(10);
  iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(0, INT_MAX);
  ASSERT_EQ(OB_SUCCESS, check_iter_valid(iter));
  iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(INT64_MAX, 0);
  ASSERT_EQ(false, iter.has_next());
  ASSERT_EQ(OB_SUCCESS, check_iter_valid(iter));

  // empty list
  reset_ash_list_with_write_pos(ObActiveSessHistList::get_instance().size());
  iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(0, INT64_MAX);
  for (int i = 0; i < ObActiveSessHistList::get_instance().size(); i++) {
    ASSERT_EQ(true, iter.has_next());
    ASSERT_EQ(0, iter.next().sample_time_);
  }
  ASSERT_EQ(false, iter.has_next());
  iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(INT64_MAX, 0);
  ASSERT_EQ(false, iter.has_next());

  // one node
  reset_ash_list_with_write_pos(ObActiveSessHistList::get_instance().size());
  push_back_sample_time(10);
  iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(0, INT64_MAX);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(10, iter.next().sample_time_);
  for (int i = 0; i < ObActiveSessHistList::get_instance().size() - 1; i++) {
    ASSERT_EQ(true, iter.has_next());
    ASSERT_EQ(0, iter.next().sample_time_);
  }
  ASSERT_EQ(false, iter.has_next());
  iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(INT64_MAX, 0);
  ASSERT_EQ(false, iter.has_next());

  // several node
  reset_ash_list_with_write_pos(ObActiveSessHistList::get_instance().size());
  push_back_sample_time(8);
  push_back_sample_time(9);
  push_back_sample_time(10);
  push_back_sample_time(10);
  push_back_sample_time(10);
  push_back_sample_time(11);
  push_back_sample_time(12);
  iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(0, INT64_MAX);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(12, iter.next().sample_time_);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(11, iter.next().sample_time_);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(10, iter.next().sample_time_);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(9, iter.next().sample_time_);
  ASSERT_EQ(true, iter.has_next());
  ASSERT_EQ(8, iter.next().sample_time_);
  for (int i = 0; i < ObActiveSessHistList::get_instance().size() - 7; i++) {
    ASSERT_EQ(true, iter.has_next());
    ASSERT_EQ(0, iter.next().sample_time_);
  }
  ASSERT_EQ(false, iter.has_next());
  iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(INT64_MAX, 0);
  ASSERT_EQ(false, iter.has_next());

  // timer reverse outcome is false, but should not core.
  reset_ash_list_with_write_pos(ObActiveSessHistList::get_instance().size());
  push_back_sample_time(8);
  push_back_sample_time(9);
  push_back_sample_time(10);
  push_back_sample_time(10);
  push_back_sample_time(10);
  push_back_sample_time(9);
  push_back_sample_time(7);
  push_back_sample_time(10);
  iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(0, INT_MAX);
  ASSERT_EQ(OB_SUCCESS, check_iter_valid(iter));
  iter = ObActiveSessHistList::get_instance().create_iterator();
  iter.init_with_sample_time_index(INT64_MAX, 0);
  ASSERT_EQ(false, iter.has_next());
  ASSERT_EQ(OB_SUCCESS, check_iter_valid(iter));
}

} // end namespace share
} // end namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
