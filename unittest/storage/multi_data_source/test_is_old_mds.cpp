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

#include "share/ob_errno.h"
#include <gtest/gtest.h>
#include "storage/schema_utils.h"
#include "storage/test_tablet_helper.h"
#include "storage/tablet/ob_mds_schema_helper.h"
namespace oceanbase {
namespace storage {
namespace mds {
void *DefaultAllocator::alloc(const int64_t size) {
  void *ptr = std::malloc(size);// ob_malloc(size, "MDS");
  ATOMIC_INC(&alloc_times_);
  MDS_LOG(DEBUG, "alloc obj", KP(ptr), K(size), K(lbt()));
  return ptr;
}
void DefaultAllocator::free(void *ptr) {
  ATOMIC_INC(&free_times_);
  MDS_LOG(DEBUG, "free obj", KP(ptr), K(lbt()));
  std::free(ptr);// ob_free(ptr);
}
void *MdsAllocator::alloc(const int64_t size) {
  void *ptr = std::malloc(size);// ob_malloc(size, "MDS");
  ATOMIC_INC(&alloc_times_);
  MDS_LOG(DEBUG, "alloc obj", KP(ptr), K(size), K(lbt()));
  return ptr;
}
void MdsAllocator::free(void *ptr) {
  ATOMIC_INC(&free_times_);
  MDS_LOG(DEBUG, "free obj", KP(ptr), K(lbt()));
  std::free(ptr);// ob_free(ptr);
}
}}}

#define USING_LOG_PREFIX STORAGE

#define UNITTEST

namespace oceanbase
{
namespace storage
{
namespace checkpoint
{
class TestIsOldMds : public ::testing::Test
{
public:
  TestIsOldMds() {}
  virtual ~TestIsOldMds() = default;

  static void SetUpTestCase()
  {
    ObMdsSchemaHelper::get_instance().init();
  }

  static void TearDownTestCase()
  {
  }
};

TEST_F(TestIsOldMds, test_is_old_mds_for_unbind) {
  int ret = OB_SUCCESS;
  ObBatchUnbindTabletArg arg;
  for (int64_t i =0; OB_SUCC(ret) && i < 5; i++) {
    arg.orig_tablet_ids_.push_back(ObTabletID(1));
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  for (int64_t i =0; OB_SUCC(ret) && i < 5; i++) {
    arg.hidden_tablet_ids_.push_back(ObTabletID(1));
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  int64_t len = 5000;
  char buf[5000] = {0};
  int64_t pos = 0;
  bool is_old_mds;

  arg.is_old_mds_ = false;
  ret = arg.serialize(buf, len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = arg.is_old_mds(buf, len, is_old_mds);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(false, is_old_mds);

  pos = 0;
  arg.is_old_mds_ = true;
  memset(buf, 0, 5000);
  ret = arg.serialize(buf, len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = arg.is_old_mds(buf, len, is_old_mds);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, is_old_mds);
}

TEST_F(TestIsOldMds, test_is_old_mds_for_delete) {
  int ret = OB_SUCCESS;
  ObBatchRemoveTabletArg arg;
  for (int64_t i =0; OB_SUCC(ret) && i < 5; i++) {
    arg.tablet_ids_.push_back(ObTabletID(1));
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  int64_t len = 5000;
  char buf[5000] = {0};
  int64_t pos = 0;
  bool is_old_mds;

  arg.is_old_mds_ = false;
  ret = arg.serialize(buf, len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = arg.is_old_mds(buf, len, is_old_mds);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(false, is_old_mds);

  pos = 0;
  arg.is_old_mds_ = true;
  memset(buf, 0, 5000);
  ret = arg.serialize(buf, len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = arg.is_old_mds(buf, len, is_old_mds);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, is_old_mds);
}

TEST_F(TestIsOldMds, test_is_old_mds_for_create) {
  int ret = OB_SUCCESS;
  ObBatchCreateTabletArg arg;
  ObTableSchema table_schema;
  TestSchemaUtils::prepare_data_schema(table_schema);
  ret = arg.table_schemas_.push_back(table_schema);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = arg.table_schemas_.push_back(table_schema);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i =0; OB_SUCC(ret) && i < 5; i++) {
    obrpc::ObCreateTabletInfo create_tablet_info;
    create_tablet_info.compat_mode_ = lib::Worker::CompatMode::MYSQL;
    create_tablet_info.data_tablet_id_ = ObTabletID(1);
    ASSERT_EQ(OB_SUCCESS, ret);
    for (int64_t j = 0; OB_SUCC(ret) && j < 5; ++j) {
      create_tablet_info.tablet_ids_.push_back(ObTabletID(1));
      ASSERT_EQ(OB_SUCCESS, ret);
      create_tablet_info.table_schema_index_.push_back(0);
      ASSERT_EQ(OB_SUCCESS, ret);
    }
    arg.tablets_.push_back(create_tablet_info);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  int64_t len = 5000;
  char buf[5000] = {0};
  int64_t pos = 0;
  bool is_old_mds;

  arg.is_old_mds_ = false;
  ret = arg.serialize(buf, len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = arg.is_old_mds(buf, len, is_old_mds);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(false, is_old_mds);

  pos = 0;
  arg.is_old_mds_ = true;
  memset(buf, 0, 5000);
  ret = arg.serialize(buf, len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = arg.is_old_mds(buf, len, is_old_mds);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, is_old_mds);
}

}
} // end namespace storage
} // end namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f test_is_old_mds.log*");
  OB_LOGGER.set_file_name("test_is_old_mds.log", true);
  OB_LOGGER.set_log_level("INFO");
  signal(49, SIG_IGN);
  testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  return ret;
}
