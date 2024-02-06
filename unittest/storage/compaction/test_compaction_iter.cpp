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

#define USING_LOG_PREFIX STORAGE
#define protected public
#define private public

#include "share/rc/ob_tenant_base.h"
#include "storage/ls/ob_ls.h"
#include "storage/compaction/ob_compaction_schedule_iterator.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "mtlenv/mock_tenant_module_env.h"

namespace oceanbase
{
using namespace share;
using namespace common;
namespace unittest
{

class MockObCompactionScheduleIterator : public compaction::ObCompactionScheduleIterator
{
public:
  MockObCompactionScheduleIterator(const int64_t batch_tablet_cnt)
    : ObCompactionScheduleIterator(
      true/*is_major, no meaning*/,
      ObLSGetMod::STORAGE_MOD),
      mock_tablet_id_cnt_(0),
      error_tablet_idx_(-1),
      errno_(OB_SUCCESS)
  {
    max_batch_tablet_cnt_ = batch_tablet_cnt;
  }
  virtual int get_cur_ls_handle(ObLSHandle &ls_handle) override
  {
    return OB_SUCCESS;
  }
  virtual int get_tablet_ids() override
  {
    int ret = OB_SUCCESS;
    for (int64_t i = 0; OB_SUCC(ret) && i < mock_tablet_id_cnt_; ++i) {
      ret = tablet_ids_.push_back(ObTabletID(i));
    }
    return ret;
  }
  virtual int get_tablet_handle(const ObTabletID &tablet_id, ObTabletHandle &tablet_handle) override
  {
    int ret = OB_SUCCESS;
    if (tablet_idx_ == error_tablet_idx_) {
      ret = errno_;
    }
    return ret;
  }
  void prepare_ls_id_array(const int64_t ls_cnt)
  {
    for (int64_t i = 0; i < ls_cnt; ++i) {
      ASSERT_EQ(OB_SUCCESS, ls_ids_.push_back(ObLSID(i)));
    }
  }
  int64_t mock_tablet_id_cnt_;
  int64_t error_tablet_idx_;
  int errno_;
};

class TestCompactionIter : public ::testing::Test
{
public:
  void test_iter(
    const int64_t ls_cnt,
    const int64_t max_batch_tablet_cnt,
    const int64_t tablet_cnt_per_ls,
    const int64_t error_tablet_idx = -1,
    const int input_errno = OB_SUCCESS);
};

void TestCompactionIter::test_iter(
  const int64_t ls_cnt,
  const int64_t max_batch_tablet_cnt,
  const int64_t tablet_cnt_per_ls,
  const int64_t error_tablet_idx,
  const int input_errno)
{
  LOG_INFO("test_iter", K(ls_cnt), K(max_batch_tablet_cnt), K(tablet_cnt_per_ls), K(error_tablet_idx), K(input_errno));
  MockObCompactionScheduleIterator iter(max_batch_tablet_cnt);
  iter.max_batch_tablet_cnt_ = max_batch_tablet_cnt;
  iter.prepare_ls_id_array(ls_cnt);
  iter.mock_tablet_id_cnt_ = tablet_cnt_per_ls;
  iter.error_tablet_idx_ = error_tablet_idx;
  iter.errno_ = input_errno;

  int ret = OB_SUCCESS;
  int iter_cnt = 0;
  int loop_cnt = 0;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  while (OB_SUCC(ret)) {
    if (iter_cnt > 0 && iter_cnt % max_batch_tablet_cnt == 0) {
      STORAGE_LOG(INFO, "iter batch finish", K(ret), K(iter), K(iter_cnt));
      ASSERT_EQ(iter.schedule_tablet_cnt_ >= max_batch_tablet_cnt, true);
      iter.start_cur_batch();
      loop_cnt++;
    }
    ret = iter.get_next_ls(ls_handle);
    if (OB_ITER_END == ret) {
      if (iter.ls_idx_ == iter.ls_ids_.count()) {
        if (iter.schedule_tablet_cnt_ > 0) {
          loop_cnt++;
        }
      } else {
        STORAGE_LOG(WARN, "unexpected error", K(ret), K(iter), K(iter_cnt));
      }
    }
    while (OB_SUCC(ret)) {
      if (OB_SUCC(iter.get_next_tablet(tablet_handle))) {
        iter_cnt++;
      } else {
        if (OB_ITER_END != ret) {
          iter.skip_cur_ls();
        }
        ret = OB_SUCCESS;
        break;
      }
    } // end of while
  }
  if (input_errno == OB_SUCCESS) {
    ASSERT_EQ(iter_cnt, ls_cnt * tablet_cnt_per_ls);
    ASSERT_EQ(loop_cnt, iter_cnt / max_batch_tablet_cnt + (iter_cnt % max_batch_tablet_cnt != 0));
  } else if (error_tablet_idx < 0 || error_tablet_idx >= tablet_cnt_per_ls) {
    // no errno
  } else if (OB_TABLET_NOT_EXIST == input_errno) {
    // for this errno, just skip this tablet
    ASSERT_EQ(iter_cnt, ls_cnt * (tablet_cnt_per_ls - 1));
    ASSERT_EQ(loop_cnt, iter_cnt / max_batch_tablet_cnt + (iter_cnt % max_batch_tablet_cnt != 0));
  } else {
    ASSERT_EQ(iter_cnt, ls_cnt * error_tablet_idx);
    ASSERT_EQ(loop_cnt, iter_cnt / max_batch_tablet_cnt + (iter_cnt % max_batch_tablet_cnt != 0));
  }
}

TEST_F(TestCompactionIter, test_normal_loop)
{
  test_iter(
    3,/*ls_cnt*/
    10000,/*max_batch_tablet_cnt*/
    10000/*tablet_cnt_per_ls*/
  );
  test_iter(
    5,/*ls_cnt*/
    1000,/*max_batch_tablet_cnt*/
    10000/*tablet_cnt_per_ls*/
  );
  test_iter(
    5,/*ls_cnt*/
    1000,/*max_batch_tablet_cnt*/
    100/*tablet_cnt_per_ls*/
  );
}

TEST_F(TestCompactionIter, test_single_ls)
{
  test_iter(
    1,/*ls_cnt*/
    1000,/*max_batch_tablet_cnt*/
    100/*tablet_cnt_per_ls*/
  );
  test_iter(
    1,/*ls_cnt*/
    1000,/*max_batch_tablet_cnt*/
    1000/*tablet_cnt_per_ls*/
  );
  test_iter(
    1,/*ls_cnt*/
    1000,/*max_batch_tablet_cnt*/
    10000/*tablet_cnt_per_ls*/
  );
}

TEST_F(TestCompactionIter, test_loop_with_not_exist_tablet)
{
  test_iter(
    2,/*ls_cnt*/
    1000,/*max_batch_tablet_cnt*/
    10000,/*tablet_cnt_per_ls*/
    50,/*error_tablet_idx*/
    OB_TABLET_NOT_EXIST/*errno*/
  );
  test_iter(
    2,/*ls_cnt*/
    1000,/*max_batch_tablet_cnt*/
    10000,/*tablet_cnt_per_ls*/
    50,/*error_tablet_idx*/
    OB_TABLET_NOT_EXIST/*errno*/
  );
  test_iter(
    2,/*ls_cnt*/
    1000,/*max_batch_tablet_cnt*/
    10000,/*tablet_cnt_per_ls*/
    50,/*error_tablet_idx*/
    OB_ERR_UNEXPECTED/*errno*/
  );
  test_iter(
    2,/*ls_cnt*/
    1000,/*max_batch_tablet_cnt*/
    1000,/*tablet_cnt_per_ls*/
    999,/*error_tablet_idx*/
    OB_ERR_UNEXPECTED/*errno*/
  );
}

} // namespace unittest
} //namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_compaction_iter.log*");
  OB_LOGGER.set_file_name("test_compaction_iter.log");
  oceanbase::common::ObLogger::get_logger().set_log_level("TRACE");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
