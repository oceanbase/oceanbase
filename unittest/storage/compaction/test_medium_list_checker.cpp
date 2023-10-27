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
#define private public
#define protected public
#include <string.h>
#include "storage/compaction/ob_medium_list_checker.h"
#include "storage/compaction/ob_medium_compaction_info.h"
#include "storage/compaction/ob_extra_medium_info.h"

namespace oceanbase
{
using namespace common;
using namespace compaction;
using namespace storage;

namespace unittest
{

class TestMediumListChecker : public ::testing::Test
{
public:
  virtual void SetUp()
  {
    GCONF.cluster_id = INIT_CLUSTER_ID;
  }
  virtual void TearDown()
  {
    allocator_.reset();
  }
  void set_basic_info(ObMediumCompactionInfo &medium_info)
  {
    medium_info.compaction_type_ = ObMediumCompactionInfo::MEDIUM_COMPACTION;
    medium_info.tenant_id_ = MTL_ID();
    medium_info.data_version_ = 100;
    medium_info.cluster_id_ = INIT_CLUSTER_ID;
    medium_info.medium_compat_version_ = ObMediumCompactionInfo::MEDIUM_COMPAT_VERSION_V2;
  }
  int construct_array(
      const char *snapshot_list,
      ObMediumListChecker::MediumInfoArray &array,
      const int64_t last_medium_scn_of_first_medium_info = 1,
      const int64_t cluster_id = INIT_CLUSTER_ID);
  int construct_array(
      const char *snapshot_list,
      ObIArray<int64_t> &array);
  static const int64_t INIT_CLUSTER_ID = 1;
  static const int64_t OTHER_CLUSTER_ID = 2;
  static const int64_t ARRAY_SIZE = 10;
private:
  ObMediumCompactionInfo medium_info_array_[ARRAY_SIZE];
  ObSEArray<int64_t, ARRAY_SIZE> array_;
  ObArenaAllocator allocator_;
};

int TestMediumListChecker::construct_array(
    const char *snapshot_list,
    ObIArray<int64_t> &array)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(snapshot_list)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(snapshot_list));
  } else {
    array.reset();
    std::string copy(snapshot_list);
    char *org = const_cast<char *>(copy.c_str());
    static const char *delim = " ";
    char *s = std::strtok(org, delim);
    if (NULL != s) {
      array.push_back(atoi(s));
      while (NULL != (s= strtok(NULL, delim))) {
        array.push_back(atoi(s));
      }
    }
  }
  return ret;
}

int TestMediumListChecker::construct_array(
    const char *snapshot_list,
    ObMediumListChecker::MediumInfoArray &input_array,
    const int64_t last_medium_scn_of_first_medium_info,
    const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  construct_array(snapshot_list, array_);
  OB_ASSERT(array_.count() <= ARRAY_SIZE);
  for (int i = 0; OB_SUCC(ret) && i < array_.count(); ++i) {
    set_basic_info(medium_info_array_[i]);
    medium_info_array_[i].medium_snapshot_ = array_.at(i);
    medium_info_array_[i].last_medium_snapshot_ = (i > 0 ? array_.at(i - 1) : last_medium_scn_of_first_medium_info);
    ret = input_array.push_back(&medium_info_array_[i]);
  }
  return ret;
}


TEST_F(TestMediumListChecker, test_validate_medium_info_list)
{
  int ret = OB_SUCCESS;
  ObExtraMediumInfo extra_info;
  extra_info.last_medium_scn_ = 100;
  ObSEArray<compaction::ObMediumCompactionInfo*, 10> array;

  ASSERT_EQ(OB_SUCCESS, construct_array("300, 400, 500", array, 10/*last_medium_scn_of_first_medium_info*/));
  ret = ObMediumListChecker::validate_medium_info_list(extra_info, &array, 100/*last_major_snapshot*/);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);

  array.reset();
  ASSERT_EQ(OB_SUCCESS, construct_array("200, 400, 500", array, 100/*last_medium_scn_of_first_medium_info*/));
  ret = ObMediumListChecker::validate_medium_info_list(extra_info, &array, 50/*last_major_snapshot*/);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);

  ret = ObMediumListChecker::validate_medium_info_list(extra_info, &array, 100/*last_major_snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);

  extra_info.last_medium_scn_ = 1000;
  ret = ObMediumListChecker::validate_medium_info_list(extra_info, &array, 1000/*last_major_snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);

  // push item without clear array
  ASSERT_EQ(OB_SUCCESS, construct_array("900", array, 700/*last_medium_scn_of_first_medium_info*/));
  ret = ObMediumListChecker::check_continue(array);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);
}

TEST_F(TestMediumListChecker, test_check_extra_info)
{
  int ret = OB_SUCCESS;
  ObExtraMediumInfo extra_info;

  extra_info.last_medium_scn_ = 0;
  ret = ObMediumListChecker::check_extra_info(extra_info, 50/*last_major_snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);

  extra_info.last_medium_scn_ = 100;
  ret = ObMediumListChecker::check_extra_info(extra_info, 0/*last_major_snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);

  extra_info.last_medium_scn_ = 100;
  ret = ObMediumListChecker::check_extra_info(extra_info, 50/*last_major_snapshot*/);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);

  extra_info.last_medium_scn_ = 100;
  ret = ObMediumListChecker::check_extra_info(extra_info, 100/*last_major_snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestMediumListChecker, test_check_next_schedule_medium)
{
  int ret = OB_SUCCESS;
  ObMediumCompactionInfo medium_info;
  set_basic_info(medium_info);
  medium_info.medium_snapshot_ = 130;
  medium_info.last_medium_snapshot_ = 100;

  ret = ObMediumListChecker::check_next_schedule_medium(&medium_info, 100/*last_major_snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = ObMediumListChecker::check_next_schedule_medium(&medium_info, 120/*last_major_snapshot*/);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);

  // medium from different cluster
  medium_info.cluster_id_ = OTHER_CLUSTER_ID;
  ret = ObMediumListChecker::check_next_schedule_medium(&medium_info, 50/*last_major_snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestMediumListChecker, test_filter_finish_medium_info)
{
  int ret = OB_SUCCESS;
  ObSEArray<compaction::ObMediumCompactionInfo*, 10> array;
  ASSERT_EQ(OB_SUCCESS, construct_array("300, 400, 500", array));
  int64_t next_medium_info_idx = 0;
  ret = ObMediumListChecker::filter_finish_medium_info(array, 100, next_medium_info_idx);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(next_medium_info_idx, 0);

  ret = ObMediumListChecker::filter_finish_medium_info(array, 500, next_medium_info_idx);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(next_medium_info_idx, 3);

  ret = ObMediumListChecker::filter_finish_medium_info(array, 400, next_medium_info_idx);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(next_medium_info_idx, 2);
}

}//end namespace unittest
}//end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_medium_list_checker.log*");
  OB_LOGGER.set_file_name("test_medium_list_checker.log");
  OB_LOGGER.set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
