/**
 * Copyright (c) 2022 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE
#include <gmock/gmock.h>

#define private public
#define protected public

#define private public
#define OK(ass) ASSERT_EQ(OB_SUCCESS, (ass))
#include "common/ob_target_specific.h"
#include "storage/ob_storage_struct.h"


namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace blocksstable;
using namespace share;

namespace unittest
{

class TestCGBitmap: public ::testing::Test
{
public:
  TestCGBitmap();
  ~TestCGBitmap() = default;
public:
  virtual void SetUp() {};
  virtual void TearDown() {};
  static void SetUpTestCase()
  {
    LOG_INFO("Supported cpu instructions",
             K(common::is_arch_supported(ObTargetArch::Default)),
             K(common::is_arch_supported(ObTargetArch::SSE42)),
             K(common::is_arch_supported(ObTargetArch::AVX)),
             K(common::is_arch_supported(ObTargetArch::AVX2)),
             K(common::is_arch_supported(ObTargetArch::AVX512)));
  }
  static void TearDownTestCase() {};
public:
  ModulePageAllocator allocator_;
};

TestCGBitmap::TestCGBitmap()
  : allocator_()
{
}

TEST_F(TestCGBitmap, test_constant)
{
  ObCGBitmap bitmap(allocator_);
  bitmap.init(1000, false);
  ASSERT_EQ(false, bitmap.get_filter_constant_type().is_constant());

  bitmap.reuse(0);
  ASSERT_EQ(false, bitmap.is_all_true());

  sql::ObBoolMask bmt;
  bmt.set_always_false();
  bitmap.set_constant_filter_info(bmt, 0);
  ASSERT_EQ(true, bitmap.is_all_false());

  bmt.set_always_true();
  bitmap.set_constant_filter_info(bmt, 0);
  ASSERT_EQ(true, bitmap.is_all_true());

  bitmap.bit_not();
  ASSERT_EQ(false, bitmap.is_all_true());
}

TEST_F(TestCGBitmap, set_batch)
{
  ObCGBitmap bitmap(allocator_);
  bitmap.init(1000, false);
  ASSERT_EQ(false, bitmap.get_filter_constant_type().is_constant());

  bitmap.reuse(0, true);
  ASSERT_EQ(true, bitmap.is_all_true());

  ObCSRange cs_range(0, 200);
  int64_t count;
  bitmap.set_bitmap_batch(cs_range.start_row_id_, cs_range.end_row_id_, 0, count);
  ASSERT_EQ(false, bitmap.is_all_true());
  ASSERT_EQ(true, bitmap.get_filter_constant_type().is_uncertain());
  ASSERT_EQ(true, bitmap.is_all_false(cs_range));
}

TEST_F(TestCGBitmap, copy_from)
{
  ObCGBitmap bitmap1(allocator_);
  bitmap1.init(1000, false);
  ASSERT_EQ(false, bitmap1.get_filter_constant_type().is_constant());

  ModulePageAllocator allocator2_;
  ObCGBitmap bitmap2(allocator2_);
  bitmap2.init(500, false);
  ASSERT_EQ(false, bitmap2.get_filter_constant_type().is_constant());

  bitmap1.reuse(0, true);
  ASSERT_EQ(true, bitmap1.is_all_true());

  bitmap2.reuse(0);
  ASSERT_EQ(true, bitmap2.is_all_false());

  bitmap1.copy_from(bitmap2);
  ASSERT_EQ(true, bitmap1.is_all_false());
  ASSERT_EQ(500, bitmap1.size());

  sql::ObBoolMask bmt;
  bmt.set_always_true();
  bitmap2.set_constant_filter_info(bmt, 0);
  ASSERT_EQ(true, bitmap2.is_all_true());

  bitmap1.copy_from(bitmap2);
  ASSERT_EQ(true, bitmap1.is_all_true());
  ASSERT_EQ(500, bitmap1.size());

  bitmap2.set_filter_uncertain();
  bitmap1.copy_from(bitmap2);
  ASSERT_EQ(false, bitmap1.get_filter_constant_type().is_constant());
}

} //namespace unittest
} //namespace oceanbase


int main(int argc, char **argv)
{
  system("rm -rf test_cg_bitmap.log");
  OB_LOGGER.set_file_name("test_cg_bitmap.log", true);
  OB_LOGGER.set_log_level("INFO");
  CLOG_LOG(INFO, "begin unittest: test_cg_bitmap");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
