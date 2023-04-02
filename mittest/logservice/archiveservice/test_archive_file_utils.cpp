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

#include "lib/container/ob_se_array.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "logservice/palf/lsn.h"
#include <cstdint>
#define private public
#include "logservice/archiveservice/ob_archive_file_utils.h"
#undef private
#include <gtest/gtest.h>
namespace oceanbase
{
namespace unittest
{
typedef ObSEArray<int64_t, 16>  IntArray;
class FakeArchiveFilUtils : public archive::ObArchiveFileUtils
{
public:
  static int locate_file_by_scn(IntArray &array, const share::SCN &ref_scn, int64_t &file_id)
  {
    int ret = OB_SUCCESS;
    auto get_value = [&](const int64_t index, share::SCN &scn, palf::LSN &lsn) -> int
    {
      int ret = 0;
      if (index >= array.count()) {
        ret = OB_SIZE_OVERFLOW;
      } else {
        lsn = palf::LSN(index);
        scn.convert_for_tx(array[index]);
      }
      return ret;
    };
    ret = locate_(0, array.count() - 1, ref_scn, file_id, get_value);
    return ret;
  }
};

TEST(TestLocateFunc, test_archive_locate_func)
{
  IntArray array1;
  array1.push_back(1);
  array1.push_back(23);
  array1.push_back(32);
  array1.push_back(33);
  array1.push_back(35);
  array1.push_back(65);
  array1.push_back(77);
  array1.push_back(88);
  array1.push_back(89);
  array1.push_back(99);

  share::SCN scn = share::SCN::min_scn();
  int64_t file_id = -1;

  EXPECT_EQ(OB_SUCCESS, FakeArchiveFilUtils::locate_file_by_scn(array1, scn, file_id));
  EXPECT_EQ(file_id, 0);

  EXPECT_EQ(OB_SUCCESS, FakeArchiveFilUtils::locate_file_by_scn(array1, scn, file_id));
  EXPECT_EQ(file_id, 0);

  scn.convert_for_tx(1);
  EXPECT_EQ(OB_SUCCESS, FakeArchiveFilUtils::locate_file_by_scn(array1, scn, file_id));
  EXPECT_EQ(file_id, 0);

  scn.convert_for_tx(2);
  EXPECT_EQ(OB_SUCCESS, FakeArchiveFilUtils::locate_file_by_scn(array1, scn, file_id));
  EXPECT_EQ(file_id, 0);

  scn.convert_for_tx(2);
  EXPECT_EQ(OB_SUCCESS, FakeArchiveFilUtils::locate_file_by_scn(array1, scn, file_id));
  EXPECT_EQ(file_id, 0);

  scn.convert_for_tx(33);
  EXPECT_EQ(OB_SUCCESS, FakeArchiveFilUtils::locate_file_by_scn(array1, scn, file_id));
  EXPECT_EQ(file_id, 3);

  scn.convert_for_tx(33);
  EXPECT_EQ(OB_SUCCESS, FakeArchiveFilUtils::locate_file_by_scn(array1, scn, file_id));
  EXPECT_EQ(file_id, 3);

  scn.convert_for_tx(34);
  EXPECT_EQ(OB_SUCCESS, FakeArchiveFilUtils::locate_file_by_scn(array1, scn, file_id));
  EXPECT_EQ(file_id, 3);

  scn.convert_for_tx(99);
  EXPECT_EQ(OB_SUCCESS, FakeArchiveFilUtils::locate_file_by_scn(array1, scn, file_id));
  EXPECT_EQ(file_id, 9);

  scn.convert_for_tx(101);
  EXPECT_EQ(OB_SUCCESS, FakeArchiveFilUtils::locate_file_by_scn(array1, scn, file_id));
  EXPECT_EQ(file_id, 9);
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_archive_locate_func.log", true);
  OB_LOGGER.set_log_level("INFO");
  CLOG_LOG(INFO, "begin unittest::test_archive_locate_func");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
