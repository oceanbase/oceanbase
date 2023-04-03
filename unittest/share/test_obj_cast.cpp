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

#define USING_LOG_PREFIX SHARE
#include "share/object/ob_obj_cast.h"
#include <gtest/gtest.h>
#include "lib/worker.h"


namespace oceanbase
{
namespace common
{
using namespace number;

class TestObjCast : public ::testing::Test
{
public:
  virtual void SetUp();
  virtual void TearDown() {}

  ObArenaAllocator allocator_;
};

void TestObjCast::SetUp()
{
  const lib::ObMemAttr attr(common::OB_SYS_TENANT_ID, ObModIds::OB_NUMBER);
  int ret = ObNumberConstValue::init(allocator_, attr);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestObjCast, test_number_range_check_mysql_old)
{
  int ret = OB_SUCCESS;
  lib::CompatModeGuard tmp_mode(lib::Worker::CompatMode::MYSQL);
  ObNumber zero_number;
  zero_number.set_zero();
  ObObj obj1;
  obj1.set_number(zero_number);
  const ObObj *res_obj = &obj1;
  ObObjCastParams params;
  params.allocator_v2_ = &allocator_;
  params.cast_mode_ |= CM_WARN_ON_FAIL;
  int64_t get_range_beg = ObTimeUtility::current_time();
  for (int16_t precision = OB_MIN_DECIMAL_PRECISION; OB_SUCC(ret) && precision <= ObNumber::MAX_PRECISION; ++precision) {
    for (int16_t scale = 0; OB_SUCC(ret) && precision >= scale && scale <= ObNumber::MAX_SCALE; ++scale) {
      ObAccuracy accuracy(precision, scale);
      ret = number_range_check(params, accuracy, obj1, obj1, res_obj, params.cast_mode_);
      ASSERT_EQ(OB_SUCCESS, ret);
    }
  }
  int64_t get_range_cost = ObTimeUtility::current_time() - get_range_beg;
  _OB_LOG(INFO, "test_number_range_check_mysql_old(%d) cost time: %f", (ObNumber::MAX_PRECISION + 1) * (ObNumber::MAX_SCALE + 1) / 2, (double)get_range_cost / (double)1000);
}

TEST_F(TestObjCast, test_number_range_check_mysql_new)
{
  int ret = OB_SUCCESS;
  lib::CompatModeGuard tmp_mode(lib::Worker::CompatMode::MYSQL);
  ObNumber zero_number;
  zero_number.set_zero();
  ObObj obj1;
  obj1.set_number(zero_number);
  const ObObj *res_obj = &obj1;
  ObObjCastParams params;
  params.allocator_v2_ = &allocator_;
  params.cast_mode_ |= CM_WARN_ON_FAIL;
  int64_t get_range_beg = ObTimeUtility::current_time();
  for (int16_t precision = OB_MIN_DECIMAL_PRECISION; OB_SUCC(ret) && precision <= ObNumber::MAX_PRECISION; ++precision) {
    for (int16_t scale = 0; OB_SUCC(ret) && precision >= scale && scale <= ObNumber::MAX_SCALE; ++scale) {
      ObAccuracy accuracy(precision, scale);
      ret = number_range_check_v2(params, accuracy, obj1, obj1, res_obj, params.cast_mode_);
      ASSERT_EQ(OB_SUCCESS, ret);
    }
  }
  int64_t get_range_cost = ObTimeUtility::current_time() - get_range_beg;
  _OB_LOG(INFO, "test_number_range_check_mysql_new(%d) cost time: %f", (ObNumber::MAX_PRECISION + 1) * (ObNumber::MAX_SCALE + 1) / 2, (double)get_range_cost / (double)1000);
}

TEST_F(TestObjCast, test_number_range_check_oracle_old)
{
  int ret = OB_SUCCESS;
  lib::CompatModeGuard tmp_mode(lib::Worker::CompatMode::ORACLE);
  ObNumber zero_number;
  zero_number.set_zero();
  ObObj obj1;
  obj1.set_number(zero_number);
  const ObObj *res_obj = &obj1;
  ObObjCastParams params;
  params.allocator_v2_ = &allocator_;
  params.cast_mode_ |= CM_WARN_ON_FAIL;
  int64_t get_range_beg = ObTimeUtility::current_time();
  for (int16_t precision = OB_MIN_NUMBER_PRECISION; OB_SUCC(ret) && precision <= OB_MAX_NUMBER_PRECISION; ++precision) {
    for (int16_t scale = ObNumber::MIN_SCALE; OB_SUCC(ret) && scale <= ObNumber::MAX_SCALE; ++scale) {
      ObAccuracy accuracy(precision, scale);
      ret = number_range_check_for_oracle(params, accuracy, obj1, obj1, res_obj, params.cast_mode_);
      ASSERT_EQ(OB_SUCCESS, ret);
    }
  }
  int64_t get_range_cost = ObTimeUtility::current_time() - get_range_beg;
  _OB_LOG(INFO, "test_number_range_check_oracle_old(%ld) cost time: %f", (OB_MAX_NUMBER_PRECISION) * (ObNumberConstValue::MAX_ORACLE_SCALE_SIZE + 1), (double)get_range_cost / (double)1000);
}

TEST_F(TestObjCast, test_number_range_check_oracle_new)
{
  int ret = OB_SUCCESS;
  lib::CompatModeGuard tmp_mode(lib::Worker::CompatMode::ORACLE);
  ObNumber zero_number;
  zero_number.set_zero();
  ObObj obj1;
  obj1.set_number(zero_number);
  const ObObj *res_obj = &obj1;
  ObObjCastParams params;
  params.allocator_v2_ = &allocator_;
  params.cast_mode_ |= CM_WARN_ON_FAIL;
  int64_t get_range_beg = ObTimeUtility::current_time();
  for (int16_t precision = OB_MIN_NUMBER_PRECISION; OB_SUCC(ret) && precision <= OB_MAX_NUMBER_PRECISION; ++precision) {
    for (int16_t scale = ObNumber::MIN_SCALE; OB_SUCC(ret) && scale <= ObNumber::MAX_SCALE; ++scale) {
      ObAccuracy accuracy(precision, scale);
      ret = number_range_check_v2(params, accuracy, obj1, obj1, res_obj, params.cast_mode_);
      ASSERT_EQ(OB_SUCCESS, ret);
    }
  }
  int64_t get_range_cost = ObTimeUtility::current_time() - get_range_beg;
  _OB_LOG(INFO, "test_number_range_check_oracle_new(%ld) cost time: %f", (OB_MAX_NUMBER_PRECISION) * (ObNumberConstValue::MAX_ORACLE_SCALE_SIZE + 1), (double)get_range_cost / (double)1000);
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
