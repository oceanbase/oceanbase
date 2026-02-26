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

#define USING_LOG_PREFIX STORAGE
#include <gtest/gtest.h>
#define protected public
#define private public

#include "src/storage/ob_i_store.h"
#include "mtlenv/mock_tenant_module_env.h"
namespace oceanbase
{
using namespace share;
using namespace common;

namespace storage
{
bool ObLSHandle::is_valid() const
{
  return true;
}

ObLSHandle& ObLSHandle::operator=(const ObLSHandle &other) {
  return *this;
}

}

namespace unittest
{

class MockObCompactionScheduleIterator : public compaction::ObCompactionScheduleIterator
{
public:
  MockObCompactionScheduleIterator(const int64_t batch_tablet_cnt)
    : ObCompactionScheduleIterator(true/*is_major*/),
      mock_tablet_id_cnt_(0),
      tablet_cnt_in_ls_array_(NULL),
      error_tablet_id_(),
      errno_(OB_SUCCESS)
  {
    max_batch_tablet_cnt_ = batch_tablet_cnt;
  }
  int init(
    const int64_t ls_cnt,
    const int64_t max_batch_tablet_cnt,
    const int64_t tablet_cnt_per_ls,
    const int64_t error_tablet_idx,
    const int input_errno);
  int init(
    const ObIArray<int64_t> &tablet_cnt_in_ls_array,
    const int64_t max_batch_tablet_cnt,
    const int64_t error_tablet_idx,
    const int input_errno);
  int init_map();
  virtual int get_cur_ls_handle(ObLSHandle &ls_handle) override
  {
    return OB_SUCCESS;
  }
  virtual int get_tablet_ids() override
  {
    int ret = OB_SUCCESS;
    int64_t touch_cnt = 0;
    const ObLSID &ls_id = ls_ids_.at(ls_idx_);
    int64_t tablet_cnt = 0;
    if (OB_NOT_NULL(tablet_cnt_in_ls_array_)) {
      tablet_cnt = tablet_cnt_in_ls_array_->at(ls_idx_);
    } else {
      tablet_cnt = mock_tablet_id_cnt_;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_cnt; ++i) {
      const ObTabletID &cur_tablet_id = ObTabletID(i + 1);
      ObTabletLSPair pair(cur_tablet_id, ls_id);
      if (OB_SUCC(tablet_ids_.array_.push_back(cur_tablet_id))) {
        if (OB_SUCCESS != tablet_map_.get_refactored(pair, touch_cnt)) {
          touch_cnt = 0;
          if (OB_FAIL(tablet_map_.set_refactored(pair, touch_cnt))) {
            LOG_WARN("failed to set refactor", KR(ret), K(cur_tablet_id), K(touch_cnt));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("success to get tablet ids", KR(ret), K(tablet_cnt));
    }
    return ret;
  }
  virtual int get_tablet_handle(const ObTabletID &tablet_id, ObTabletHandle &tablet_handle) override
  {
    int ret = OB_SUCCESS;
    int64_t touch_cnt = 0;
    ObTabletLSPair pair(tablet_id, ls_ids_.at(ls_idx_));
    if (tablet_id == error_tablet_id_) {
      ret = errno_;
    } else if (OB_FAIL(tablet_map_.get_refactored(pair, touch_cnt))) {
      LOG_WARN("failed to get refactor", KR(ret), K(tablet_id), K(touch_cnt));
    } else if (FALSE_IT(++touch_cnt)) {
    } else if (OB_FAIL(tablet_map_.set_refactored(pair, touch_cnt, 1/*overwrite*/))) {
      LOG_WARN("failed to set refactor", KR(ret), K(tablet_id), K(touch_cnt));
    } else {
      LOG_INFO("success to set refactor", KR(ret), K(tablet_id), K(pair), K(touch_cnt));
    }
    return ret;
  }
  void prepare_ls_id_array(const int64_t ls_cnt)
  {
    for (int64_t i = 0; i < ls_cnt; ++i) {
      ASSERT_EQ(OB_SUCCESS, ls_ids_.push_back(ObLSID(i + 1)));
    }
  }
  int check_valid(const int64_t expect_touch_cnt);
  static const int64_t DEFAULT_BUCKET_NUM = 1024;
  typedef hash::ObHashMap<ObTabletLSPair, int64_t> TabletID2CntMap;
  TabletID2CntMap tablet_map_;
  int64_t mock_tablet_id_cnt_;
  const ObIArray<int64_t> *tablet_cnt_in_ls_array_;
  ObTabletID error_tablet_id_;
  int errno_;
};

int MockObCompactionScheduleIterator::init_map()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(tablet_map_.created())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet map is created", KR(ret));
  } else if (OB_FAIL(tablet_map_.create(DEFAULT_BUCKET_NUM, "MockMap", "MockMap"))) {
    LOG_WARN("failed to create tablet map", KR(ret));
  }
  return ret;
}

int MockObCompactionScheduleIterator::init(
    const int64_t ls_cnt,
    const int64_t max_batch_tablet_cnt,
    const int64_t tablet_cnt_per_ls,
    const int64_t error_tablet_idx,
    const int input_errno)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(init_map())) {
    LOG_WARN("failed to create tablet map", KR(ret));
  } else {
    prepare_ls_id_array(ls_cnt);
    max_batch_tablet_cnt_ = max_batch_tablet_cnt;
    mock_tablet_id_cnt_ = tablet_cnt_per_ls;
    error_tablet_id_ = ObTabletID(error_tablet_idx + 1);
    errno_ = input_errno;
  }
  return ret;
}

int MockObCompactionScheduleIterator::init(
  const ObIArray<int64_t> &tablet_cnt_in_ls_array,
  const int64_t max_batch_tablet_cnt,
  const int64_t error_tablet_idx,
  const int input_errno)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(tablet_cnt_in_ls_array.empty() || max_batch_tablet_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_cnt_in_ls_array), K(max_batch_tablet_cnt));
  } else if (OB_FAIL(init_map())) {
    LOG_WARN("failed to create tablet map", KR(ret));
  } else {
    prepare_ls_id_array(tablet_cnt_in_ls_array.count());
    max_batch_tablet_cnt_ = max_batch_tablet_cnt;
    tablet_cnt_in_ls_array_ = &tablet_cnt_in_ls_array;
    error_tablet_id_ = ObTabletID(error_tablet_idx + 1);
    errno_ = input_errno;
  }
  return ret;
}

int MockObCompactionScheduleIterator::check_valid(const int64_t expect_touch_cnt)
{
  int ret = OB_SUCCESS;
  int64_t touch_cnt = 0;
  for (int64_t ls_idx = 0; ls_idx < ls_ids_.count(); ++ls_idx) {
    const ObLSID &ls_id = ls_ids_.at(ls_idx);
    for (int64_t i = 0; OB_SUCC(ret) && i < mock_tablet_id_cnt_; ++i) {
      const ObTabletID &cur_tablet_id = ObTabletID(i + 1);
      ObTabletLSPair pair(cur_tablet_id, ls_id);
      if (!error_tablet_id_.is_valid()
        || (cur_tablet_id < error_tablet_id_ || (OB_TABLET_NOT_EXIST == errno_ && cur_tablet_id > error_tablet_id_))) {
        if (OB_FAIL(tablet_map_.get_refactored(pair, touch_cnt))) {
          LOG_WARN("failed to get refactor", KR(ret), K(cur_tablet_id), K(pair), K(touch_cnt));
        } else if (touch_cnt != expect_touch_cnt) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tablet touch cnt is unexpected", KR(ret), K(pair), K(touch_cnt), K(expect_touch_cnt), K(error_tablet_id_));
        }
      }
    } // for
  } // for
  return ret;
}

class TestCompactionIter : public ::testing::Test
{
public:
  void test_iter(
    const int64_t ls_cnt,
    const int64_t max_batch_tablet_cnt,
    const int64_t tablet_cnt_per_ls,
    const ObIArray<int64_t> *tablet_cnt_in_ls_array = NULL,
    const int64_t error_tablet_idx = -1,
    const int input_errno = OB_SUCCESS);
};

void TestCompactionIter::test_iter(
  const int64_t ls_cnt,
  const int64_t max_batch_tablet_cnt,
  const int64_t tablet_cnt_per_ls,
  const ObIArray<int64_t> *tablet_cnt_in_ls_array,
  const int64_t error_tablet_idx,
  const int input_errno)
{
  LOG_INFO("test_iter", K(ls_cnt), K(max_batch_tablet_cnt), K(tablet_cnt_per_ls), K(error_tablet_idx), K(input_errno));
  MockObCompactionScheduleIterator iter(max_batch_tablet_cnt);
  if (OB_ISNULL(tablet_cnt_in_ls_array)) {
    ASSERT_TRUE(ls_cnt > 0 && tablet_cnt_per_ls > 0);
    ASSERT_EQ(OB_SUCCESS, iter.init(ls_cnt, max_batch_tablet_cnt, tablet_cnt_per_ls, error_tablet_idx, input_errno));
  } else {
    ASSERT_TRUE(ls_cnt == 0 && tablet_cnt_per_ls == 0);
    ASSERT_EQ(OB_SUCCESS, iter.init(*tablet_cnt_in_ls_array, max_batch_tablet_cnt, error_tablet_idx, input_errno));
  }

  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  int64_t iter_batch_cnt = 0;
  int iter_cnt = 0;
  while (OB_SUCC(ret)) {
    while (OB_SUCC(iter.get_next_ls(ls_handle))) {
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
      } // iter tablet
    }   // iter ls
    ASSERT_EQ(OB_ITER_END, ret);
    ret = OB_SUCCESS;
    ++iter_batch_cnt;
    if (iter.is_valid()) {
      ASSERT_EQ(iter.schedule_tablet_cnt_ >= max_batch_tablet_cnt, true);
      iter.start_cur_batch();
    } else {
      break;
    }
  } // while
  ASSERT_EQ(OB_SUCCESS, iter.check_valid(1));
  if (OB_ISNULL(tablet_cnt_in_ls_array)) {
    if (input_errno == OB_SUCCESS) {
      ASSERT_EQ(iter_cnt, ls_cnt * tablet_cnt_per_ls);
    } else if (OB_TABLET_NOT_EXIST == input_errno) {
      // for this errno, just skip this tablet
      ASSERT_EQ(iter_cnt, ls_cnt * (tablet_cnt_per_ls - 1));
    } else {
      ASSERT_EQ(iter_cnt, ls_cnt * error_tablet_idx);
    }
  } else {
    int64_t expect_iter_cnt = 0;
    for (int64_t idx = 0; idx < tablet_cnt_in_ls_array->count(); ++idx) {
      if (input_errno == OB_SUCCESS) {
        expect_iter_cnt += tablet_cnt_in_ls_array->at(idx);
      } else if (OB_TABLET_NOT_EXIST == input_errno) {
        // for this errno, just skip this tablet
        expect_iter_cnt += (tablet_cnt_in_ls_array->at(idx) - (tablet_cnt_in_ls_array->at(idx) > error_tablet_idx ? 1 : 0));
      } else {
        expect_iter_cnt += MIN(tablet_cnt_in_ls_array->at(idx), error_tablet_idx);
      }
    } // for
    ASSERT_EQ(iter_cnt, expect_iter_cnt);
  }
  ASSERT_EQ(iter_batch_cnt, MAX(1, iter_cnt / max_batch_tablet_cnt + (iter_cnt % max_batch_tablet_cnt != 0)));
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
  test_iter(
    5,/*ls_cnt*/
    100,/*max_batch_tablet_cnt*/
    100/*tablet_cnt_per_ls*/
  );
  test_iter(
    5,/*ls_cnt*/
    200,/*max_batch_tablet_cnt*/
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
    NULL,
    50,/*error_tablet_idx*/
    OB_TABLET_NOT_EXIST/*errno*/
  );
  test_iter(
    2,/*ls_cnt*/
    1000,/*max_batch_tablet_cnt*/
    10000,/*tablet_cnt_per_ls*/
    NULL,
    50,/*error_tablet_idx*/
    OB_TABLET_NOT_EXIST/*errno*/
  );
  test_iter(
    2,/*ls_cnt*/
    1000,/*max_batch_tablet_cnt*/
    1001,/*tablet_cnt_per_ls*/
    NULL,
    999,/*error_tablet_idx*/
    OB_TABLET_NOT_EXIST/*errno*/
  );
}

TEST_F(TestCompactionIter, test_loop_with_errno)
{
  test_iter(
    2,/*ls_cnt*/
    1000,/*max_batch_tablet_cnt*/
    10000,/*tablet_cnt_per_ls*/
    NULL,
    50,/*error_tablet_idx*/
    OB_ERR_UNEXPECTED/*errno*/
  );
  test_iter(
    2,/*ls_cnt*/
    1000,/*max_batch_tablet_cnt*/
    1000,/*tablet_cnt_per_ls*/
    NULL,
    999,/*error_tablet_idx*/
    OB_ERR_UNEXPECTED/*errno*/
  );
  test_iter(
    3,/*ls_cnt*/
    1000,/*max_batch_tablet_cnt*/
    999,/*tablet_cnt_per_ls*/
    NULL,
    999,/*error_tablet_idx*/
    OB_ERR_UNEXPECTED/*errno*/
  );
  test_iter(
    3,/*ls_cnt*/
    1000,/*max_batch_tablet_cnt*/
    999,/*tablet_cnt_per_ls*/
    NULL,
    0,/*error_tablet_idx*/
    OB_ERR_UNEXPECTED/*errno*/
  );
  test_iter(
    2,/*ls_cnt*/
    1000,/*max_batch_tablet_cnt*/
    50,/*tablet_cnt_per_ls*/
    NULL,
    1,/*error_tablet_idx*/
    OB_ERR_UNEXPECTED/*errno*/
  );
}

TEST_F(TestCompactionIter, test_iter_with_tablet_cnt_list)
{
  ObSEArray<int64_t, 5> tablet_cnt_in_ls;
  tablet_cnt_in_ls.push_back(100);
  tablet_cnt_in_ls.push_back(0);
  tablet_cnt_in_ls.push_back(2);
  tablet_cnt_in_ls.push_back(0);
  tablet_cnt_in_ls.push_back(300);

  test_iter(
    0,/*ls_cnt*/
    1000,/*max_batch_tablet_cnt*/
    0,/*tablet_cnt_per_ls*/
    &tablet_cnt_in_ls
  );
  test_iter(
    0,/*ls_cnt*/
    100,/*max_batch_tablet_cnt*/
    0,/*tablet_cnt_per_ls*/
    &tablet_cnt_in_ls,
    50,/*error_tablet_idx*/
    OB_TABLET_NOT_EXIST/*errno*/
  );
  test_iter(
    0,/*ls_cnt*/
    200,/*max_batch_tablet_cnt*/
    0,/*tablet_cnt_per_ls*/
    &tablet_cnt_in_ls,
    50,/*error_tablet_idx*/
    OB_TABLET_NOT_EXIST/*errno*/
  );
  test_iter(
    0,/*ls_cnt*/
    250,/*max_batch_tablet_cnt*/
    0,/*tablet_cnt_per_ls*/
    &tablet_cnt_in_ls,
    50,/*error_tablet_idx*/
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
