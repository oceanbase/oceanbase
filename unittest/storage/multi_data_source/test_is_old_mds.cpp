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
#include "lib/utility/ob_serialization_helper.h"
#include "share/ob_rpc_struct.h"
#include "deps/oblib/src/lib/utility/ob_unify_serialize.h"

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
  }

  static void TearDownTestCase()
  {
  }
};

struct TestBatchCreateTabletArg
{
  const static int64_t UNIS_VERSION = 1;
  int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;
  int serialize_(char* buf, const int64_t buf_len, int64_t& pos) const;
  int64_t get_serialize_size() const;
  int64_t get_serialize_size_() const;
  ObBatchCreateTabletArg arg;
};

OB_DEF_SERIALIZE(TestBatchCreateTabletArg)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, arg.id_, arg.major_frozen_scn_, arg.tablets_, arg.table_schemas_, arg.need_check_tablet_cnt_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(TestBatchCreateTabletArg)
{
  int len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, arg.id_, arg.major_frozen_scn_, arg.tablets_, arg.table_schemas_, arg.need_check_tablet_cnt_);
  return len;
}

struct TestBatchUnbindTabletArg
{
  const static int64_t UNIS_VERSION = 1;
  int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;
  int serialize_(char* buf, const int64_t buf_len, int64_t& pos) const;
  int64_t get_serialize_size() const;
  int64_t get_serialize_size_() const;
  ObBatchUnbindTabletArg arg;
};

OB_DEF_SERIALIZE(TestBatchUnbindTabletArg)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, arg.tenant_id_, arg.ls_id_, arg.schema_version_, arg.orig_tablet_ids_, arg.hidden_tablet_ids_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(TestBatchUnbindTabletArg)
{
  int len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, arg.tenant_id_, arg.ls_id_, arg.schema_version_, arg.orig_tablet_ids_, arg.hidden_tablet_ids_);
  return len;
}

struct TestBatchRemoveTabletArg
{
  const static int64_t UNIS_VERSION = 1;
  int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;
  int serialize_(char* buf, const int64_t buf_len, int64_t& pos) const;
  int64_t get_serialize_size() const;
  int64_t get_serialize_size_() const;
  ObBatchRemoveTabletArg arg;
};

OB_DEF_SERIALIZE(TestBatchRemoveTabletArg)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, arg.tablet_ids_, arg.id_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(TestBatchRemoveTabletArg)
{
  int len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, arg.tablet_ids_, arg.id_);
  return len;
}

TEST_F(TestIsOldMds, test_is_old_mds_for_unbind) {
  int ret = OB_SUCCESS;
  TestBatchUnbindTabletArg test_arg;
  for (int64_t i =0; OB_SUCC(ret) && i < 5; i++) {
    test_arg.arg.orig_tablet_ids_.push_back(ObTabletID(1));
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  for (int64_t i =0; OB_SUCC(ret) && i < 5; i++) {
    test_arg.arg.hidden_tablet_ids_.push_back(ObTabletID(1));
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  int64_t len = 5000;
  char buf[5000] = {0};
  int64_t pos = 0;
  bool is_old_mds;

  pos = 0;
  test_arg.arg.is_old_mds_ = true;
  memset(buf, 0, 5000);
  ret = test_arg.arg.serialize(buf, len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = test_arg.arg.is_old_mds(buf, len, is_old_mds);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, is_old_mds);

  pos = 0;
  test_arg.arg.is_old_mds_ = false;
  memset(buf, 0, 5000);
  ret = test_arg.arg.serialize(buf, len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = test_arg.arg.is_old_mds(buf, len, is_old_mds);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(false, is_old_mds);

  pos = 0;
  memset(buf, 0, 5000);
  ret = test_arg.serialize(buf, len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  len = pos;
  ret = test_arg.arg.is_old_mds(buf, len, is_old_mds);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, is_old_mds);
}

TEST_F(TestIsOldMds, test_is_old_mds_for_delete) {
  int ret = OB_SUCCESS;
  TestBatchRemoveTabletArg test_arg;
  for (int64_t i =0; OB_SUCC(ret) && i < 5; i++) {
    test_arg.arg.tablet_ids_.push_back(ObTabletID(1));
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  int64_t len = 5000;
  char buf[5000] = {0};
  int64_t pos = 0;
  bool is_old_mds;

  pos = 0;
  test_arg.arg.is_old_mds_ = true;
  memset(buf, 0, 5000);
  ret = test_arg.arg.serialize(buf, len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = test_arg.arg.is_old_mds(buf, len, is_old_mds);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, is_old_mds);

  pos = 0;
  test_arg.arg.is_old_mds_ = false;
  ret = test_arg.arg.serialize(buf, len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = test_arg.arg.is_old_mds(buf, len, is_old_mds);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(false, is_old_mds);

  pos = 0;
  memset(buf, 0, 5000);
  ret = test_arg.serialize(buf, len, pos);
  len = pos;
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = test_arg.arg.is_old_mds(buf, len, is_old_mds);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, is_old_mds);
}

TEST_F(TestIsOldMds, test_is_old_mds_for_create) {
  int ret = OB_SUCCESS;
  TestBatchCreateTabletArg test_arg;
  ObTableSchema table_schema;
  TestSchemaUtils::prepare_data_schema(table_schema);
  ret = test_arg.arg.table_schemas_.push_back(table_schema);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = test_arg.arg.table_schemas_.push_back(table_schema);
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
    test_arg.arg.tablets_.push_back(create_tablet_info);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  int64_t len = 5000;
  char buf[5000] = {0};
  int64_t pos = 0;
  bool is_old_mds;

  pos = 0;
  test_arg.arg.is_old_mds_ = true;
  memset(buf, 0, 5000);
  ret = test_arg.arg.serialize(buf, len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = test_arg.arg.is_old_mds(buf, len, is_old_mds);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, is_old_mds);

  pos = 0;
  test_arg.arg.is_old_mds_ = false;
  ret = test_arg.arg.serialize(buf, len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = test_arg.arg.is_old_mds(buf, len, is_old_mds);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(false, is_old_mds);

  pos = 0;
  memset(buf, 0, 5000);
  ret = test_arg.serialize(buf, len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = test_arg.arg.is_old_mds(buf, pos, is_old_mds);
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
